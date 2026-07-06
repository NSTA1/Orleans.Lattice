using System.Collections.Concurrent;

namespace Orleans.Lattice.Backup;

/// <summary>
/// A small, thread-safe, in-memory registry of backup inventory and per-scope
/// run status that the capture, restore, scheduler, and retention paths update as
/// they run. It exists so the observable-gauge callbacks on
/// <see cref="LatticeBackupMetrics"/> (and the admin status surface) can read
/// cheap cached state - current backup count, chain depth, oldest / newest ages,
/// cumulative catalog bytes, per-scope last-run status, and aggregate
/// failure / reclaimed counters - without scanning the live catalog on every
/// metric collection.
/// </summary>
/// <remarks>
/// The registry is a process-wide singleton (<see cref="Instance"/>) so the
/// static meter's gauge callbacks and the dependency-injected consumers share one
/// instance. It reflects activity observed for the lifetime of the process; the
/// authoritative, restart-durable inventory is always the catalog itself, which
/// the admin surface consults for absolute counts while reading the
/// process-lifetime failure / reclaimed tallies from here.
/// </remarks>
internal sealed class BackupInventoryRegistry
{
    /// <summary>The process-wide singleton shared by the static meter and the DI container.</summary>
    public static BackupInventoryRegistry Instance { get; } = new();

    private readonly ConcurrentDictionary<string, BackupRecord> _backups = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ScopeRuntimeRecord> _scopes = new(StringComparer.Ordinal);

    private long _captureFailures;
    private long _restoreFailures;
    private long _bytesReclaimed;

    /// <summary>A single tracked backup's inventory-relevant fields.</summary>
    private readonly record struct BackupRecord(
        string ScopeKey,
        BackupKind Kind,
        DateTimeOffset CreatedAtUtc,
        string? BaseBackupId,
        long ByteLength);

    /// <summary>The mutable per-scope run status backing the per-scope gauges and the status surface.</summary>
    private sealed class ScopeRuntimeRecord
    {
        public BackupScopeRunOutcome LastRunOutcome;
        public DateTimeOffset? LastRunUtc;
        public DateTimeOffset? LastSuccessUtc;
    }

    /// <summary>Records a freshly captured backup for the inventory gauges and marks its scope's run a success.</summary>
    /// <param name="manifest">The committed manifest. Must not be <c>null</c>.</param>
    public void RecordCaptureSuccess(BackupManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);

        long byteLength = 0;
        foreach (var descriptor in manifest.ContentDescriptors)
        {
            byteLength += descriptor.ByteLength;
        }

        var scopeKey = BackupScopeKey.For(manifest.Scope);
        _backups[manifest.Id] = new BackupRecord(
            scopeKey, manifest.Kind, manifest.CreatedAtUtc, manifest.BaseBackupId, byteLength);

        RecordScopeOutcome(scopeKey, BackupScopeRunOutcome.Success, manifest.CreatedAtUtc);
    }

    /// <summary>Removes a pruned backup from the inventory and adds its bytes to the reclaimed tally.</summary>
    /// <param name="manifest">The pruned manifest. Must not be <c>null</c>.</param>
    public void RecordPruned(BackupManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);

        if (_backups.TryRemove(manifest.Id, out var record))
        {
            Interlocked.Add(ref _bytesReclaimed, record.ByteLength);
        }
        else
        {
            long byteLength = 0;
            foreach (var descriptor in manifest.ContentDescriptors)
            {
                byteLength += descriptor.ByteLength;
            }

            Interlocked.Add(ref _bytesReclaimed, byteLength);
        }
    }

    /// <summary>Records the outcome of a scope's most recent capture cycle.</summary>
    /// <param name="scopeKey">The scope key. Must not be <c>null</c> or empty.</param>
    /// <param name="outcome">The terminal outcome.</param>
    /// <param name="whenUtc">The wall-clock time the cycle finished.</param>
    public void RecordScopeOutcome(string scopeKey, BackupScopeRunOutcome outcome, DateTimeOffset whenUtc)
    {
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);

        var record = _scopes.GetOrAdd(scopeKey, static _ => new ScopeRuntimeRecord());
        lock (record)
        {
            record.LastRunOutcome = outcome;
            record.LastRunUtc = whenUtc;
            if (outcome == BackupScopeRunOutcome.Success)
            {
                record.LastSuccessUtc = whenUtc;
            }
        }
    }

    /// <summary>Increments the aggregate capture-failure tally.</summary>
    public void IncrementCaptureFailures() => Interlocked.Increment(ref _captureFailures);

    /// <summary>Increments the aggregate restore-failure tally.</summary>
    public void IncrementRestoreFailures() => Interlocked.Increment(ref _restoreFailures);

    /// <summary>The aggregate number of capture failures observed this process lifetime.</summary>
    public long CaptureFailureCount => Interlocked.Read(ref _captureFailures);

    /// <summary>The aggregate number of restore failures observed this process lifetime.</summary>
    public long RestoreFailureCount => Interlocked.Read(ref _restoreFailures);

    /// <summary>The cumulative bytes reclaimed by retention / deletion this process lifetime.</summary>
    public long BytesReclaimed => Interlocked.Read(ref _bytesReclaimed);

    /// <summary>Computes a point-in-time inventory snapshot from the tracked backups.</summary>
    /// <returns>The current inventory aggregates.</returns>
    public BackupInventorySnapshot Snapshot()
    {
        long count = 0;
        long totalBytes = 0;
        var maxChainDepth = 0;
        DateTimeOffset? oldest = null;
        DateTimeOffset? newest = null;

        foreach (var record in _backups.Values)
        {
            count++;
            totalBytes += record.ByteLength;
            if (oldest is null || record.CreatedAtUtc < oldest)
            {
                oldest = record.CreatedAtUtc;
            }

            if (newest is null || record.CreatedAtUtc > newest)
            {
                newest = record.CreatedAtUtc;
            }
        }

        // Chain depth: the longest base-backup chain fully contained in the
        // registry. A missing base terminates the walk (a pruned base is not
        // counted).
        foreach (var id in _backups.Keys)
        {
            var depth = 1;
            var currentId = id;
            var guard = 0;
            while (_backups.TryGetValue(currentId, out var current)
                && current.BaseBackupId is { } baseId
                && _backups.ContainsKey(baseId)
                && guard++ < _backups.Count)
            {
                depth++;
                currentId = baseId;
            }

            if (depth > maxChainDepth)
            {
                maxChainDepth = depth;
            }
        }

        return new BackupInventorySnapshot(count, totalBytes, maxChainDepth, oldest, newest);
    }

    /// <summary>Reads a scope's cached run status, or <c>null</c> when the scope has no recorded run.</summary>
    /// <param name="scopeKey">The scope key. Must not be <c>null</c> or empty.</param>
    /// <returns>The scope's run status, or <c>null</c> when none is recorded.</returns>
    public BackupScopeRuntime? TryGetScope(string scopeKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);

        if (!_scopes.TryGetValue(scopeKey, out var record))
        {
            return null;
        }

        lock (record)
        {
            return new BackupScopeRuntime(record.LastRunOutcome, record.LastRunUtc, record.LastSuccessUtc);
        }
    }

    /// <summary>Enumerates a stable snapshot of every scope's cached run status, keyed by scope key.</summary>
    /// <returns>One entry per scope that has a recorded run.</returns>
    public IReadOnlyList<KeyValuePair<string, BackupScopeRuntime>> EnumerateScopes()
    {
        var list = new List<KeyValuePair<string, BackupScopeRuntime>>(_scopes.Count);
        foreach (var pair in _scopes)
        {
            BackupScopeRuntime runtime;
            lock (pair.Value)
            {
                runtime = new BackupScopeRuntime(
                    pair.Value.LastRunOutcome, pair.Value.LastRunUtc, pair.Value.LastSuccessUtc);
            }

            list.Add(new KeyValuePair<string, BackupScopeRuntime>(pair.Key, runtime));
        }

        return list;
    }

    /// <summary>Clears all tracked state. Intended for test isolation only.</summary>
    public void Reset()
    {
        _backups.Clear();
        _scopes.Clear();
        Interlocked.Exchange(ref _captureFailures, 0);
        Interlocked.Exchange(ref _restoreFailures, 0);
        Interlocked.Exchange(ref _bytesReclaimed, 0);
    }
}

/// <summary>A point-in-time inventory aggregate computed from the tracked backups.</summary>
/// <param name="Count">The number of tracked backups.</param>
/// <param name="TotalBytes">The cumulative artifact bytes across tracked backups.</param>
/// <param name="MaxChainDepth">The deepest fully-contained base-backup chain.</param>
/// <param name="OldestCreatedAtUtc">The oldest tracked backup's capture time, or <c>null</c> when empty.</param>
/// <param name="NewestCreatedAtUtc">The newest tracked backup's capture time, or <c>null</c> when empty.</param>
internal readonly record struct BackupInventorySnapshot(
    long Count,
    long TotalBytes,
    int MaxChainDepth,
    DateTimeOffset? OldestCreatedAtUtc,
    DateTimeOffset? NewestCreatedAtUtc);

/// <summary>A scope's cached run status.</summary>
/// <param name="LastRunOutcome">The outcome of the most recent capture cycle.</param>
/// <param name="LastRunUtc">The wall-clock time of the most recent cycle, or <c>null</c> when none.</param>
/// <param name="LastSuccessUtc">The wall-clock time of the most recent successful cycle, or <c>null</c> when none.</param>
internal readonly record struct BackupScopeRuntime(
    BackupScopeRunOutcome LastRunOutcome,
    DateTimeOffset? LastRunUtc,
    DateTimeOffset? LastSuccessUtc);
