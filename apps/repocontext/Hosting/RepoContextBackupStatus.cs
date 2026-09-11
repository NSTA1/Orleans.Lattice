using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The live, positive statement of what this container has actually captured of
/// the durable agent-memory tree. It is the health signal for the backup wiring
/// added by issue #2602.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a positive statement and not a success flag.</b> A backup job that runs
/// over an empty or wrongly-scoped selection completes successfully and reports
/// success, so "the last run succeeded" cannot distinguish a deployment that is
/// protecting agent memory from one that is diligently capturing nothing. Every
/// field here is therefore an assertion about <em>what was captured</em>: the tree
/// id read off the manifest's own scope, and the number of key descriptors the
/// manifest carries. A reader can tell "captured 412 entries of
/// <c>repo-context-memory</c>" from "captured 0 entries" and from "never
/// captured", which a boolean cannot.
/// </para>
/// <para>
/// <b>Why the fallback count is surfaced.</b>
/// <c>LatticeBackupCaptureService</c> silently degrades an incremental capture
/// into a full one when the base chain is owned by a different capturing cluster
/// or when WAL retention has trimmed past the base resume point. The capture
/// succeeds and the data is safe, but a deployment where every incremental has
/// quietly fallen back is not doing what its configuration says, and the cost
/// (a full capture every hour) is real. Counting the fallbacks makes that
/// visible rather than leaving it to be inferred from sink size.
/// </para>
/// <para>
/// Instances are shared across the host's hosted services and its health
/// endpoint, so every member is thread-safe. Updates take a private lock; reads
/// return an immutable snapshot.
/// </para>
/// </remarks>
public sealed class RepoContextBackupStatus
{
    private readonly object _gate = new();
    private string? _capturedTreeId;
    private string? _lastFullBackupId;
    private DateTimeOffset? _lastFullAtUtc;
    private int _lastFullEntryCount;
    private string? _lastIncrementalBackupId;
    private DateTimeOffset? _lastIncrementalAtUtc;
    private int _lastIncrementalEntryCount;
    private int _captureCount;
    private int _incrementalFallbackCount;
    private string? _lastFailure;
    private int _sinkBackupCount = -1;
    private string? _sinkNewestBackupId;
    private DateTimeOffset? _sinkNewestAtUtc;
    private string? _nonDurableSinkType;

    /// <summary>
    /// Initializes the status for a host in which backup is either wired or not.
    /// </summary>
    /// <param name="enabled">
    /// <see langword="true"/> when an external backup sink is configured and the
    /// capture cadence is running; <see langword="false"/> when this container is
    /// deliberately running without backup.
    /// </param>
    /// <param name="scopedTreeId">
    /// The tree this host is configured to capture. Recorded separately from
    /// <see cref="CapturedTreeId"/> so a mismatch between what was configured and
    /// what a manifest actually says is observable rather than assumed away.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="scopedTreeId"/> is null.</exception>
    public RepoContextBackupStatus(bool enabled, string scopedTreeId)
    {
        ArgumentNullException.ThrowIfNull(scopedTreeId);
        Enabled = enabled;
        ScopedTreeId = scopedTreeId;
    }

    /// <summary>Whether backup capture is wired and running in this container.</summary>
    public bool Enabled { get; }

    /// <summary>The tree id this host is configured to capture.</summary>
    public string ScopedTreeId { get; }

    /// <summary>
    /// The tree id read off the most recent capture's own manifest scope, or
    /// <see langword="null"/> when nothing has been captured yet. This is the
    /// evidence, as opposed to <see cref="ScopedTreeId"/>, which is the intent.
    /// </summary>
    public string? CapturedTreeId
    {
        get { lock (_gate) { return _capturedTreeId; } }
    }

    /// <summary>The backup id of the most recent full capture, or null.</summary>
    public string? LastFullBackupId
    {
        get { lock (_gate) { return _lastFullBackupId; } }
    }

    /// <summary>The completion time of the most recent full capture, or null.</summary>
    public DateTimeOffset? LastFullAtUtc
    {
        get { lock (_gate) { return _lastFullAtUtc; } }
    }

    /// <summary>
    /// The number of keys the most recent full capture's manifest describes. Zero
    /// after a successful full capture means an empty selection was captured, which
    /// is a distinct and reportable condition, not a healthy backup.
    /// </summary>
    public int LastFullEntryCount
    {
        get { lock (_gate) { return _lastFullEntryCount; } }
    }

    /// <summary>The backup id of the most recent incremental capture, or null.</summary>
    public string? LastIncrementalBackupId
    {
        get { lock (_gate) { return _lastIncrementalBackupId; } }
    }

    /// <summary>The completion time of the most recent incremental capture, or null.</summary>
    public DateTimeOffset? LastIncrementalAtUtc
    {
        get { lock (_gate) { return _lastIncrementalAtUtc; } }
    }

    /// <summary>The number of keys the most recent incremental capture's manifest describes.</summary>
    public int LastIncrementalEntryCount
    {
        get { lock (_gate) { return _lastIncrementalEntryCount; } }
    }

    /// <summary>The total number of captures this host has completed since it started.</summary>
    public int CaptureCount
    {
        get { lock (_gate) { return _captureCount; } }
    }

    /// <summary>
    /// How many times an incremental capture was silently promoted to a full one by
    /// the capture service. A count equal to the number of incrementals attempted
    /// means the configured incremental cadence is not being honoured.
    /// </summary>
    public int IncrementalFallbackCount
    {
        get { lock (_gate) { return _incrementalFallbackCount; } }
    }

    /// <summary>The message of the most recent capture failure, or null when none has failed.</summary>
    public string? LastFailure
    {
        get { lock (_gate) { return _lastFailure; } }
    }

    /// <summary>
    /// The one-value summary of everything above, for the surfaces an operator or an
    /// alert actually reads.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why the derivation lives here.</b> The health endpoint and the metric
    /// series must never disagree about the same container, and they would drift
    /// apart the moment each decided for itself what counts as protected. Both read
    /// this property, so there is exactly one place where the question is answered
    /// and exactly one place a future condition has to be added.
    /// </para>
    /// <para>
    /// The whole snapshot is read under a single lock, so the returned state is a
    /// consistent view rather than a composition of separately-observed fields that
    /// a concurrent capture could have moved underneath it.
    /// </para>
    /// </remarks>
    public RepoContextBackupState State
    {
        get
        {
            if (!Enabled)
            {
                return RepoContextBackupState.Disabled;
            }

            lock (_gate)
            {
                if (_lastFailure is not null)
                {
                    // Nothing captured in this process means nothing it produced is
                    // recoverable, which is a materially worse position than a broken
                    // cadence over an existing capture. They are reported apart.
                    return _captureCount == 0
                        ? RepoContextBackupState.FailingUnprotected
                        : RepoContextBackupState.FailingAfterCapture;
                }

                if (_captureCount == 0)
                {
                    return RepoContextBackupState.NeverCaptured;
                }

                // A successful full capture describing zero entries is the
                // "diligently capturing nothing" case this type's remarks open with.
                // It reports success everywhere else, so it is separated here.
                return _lastFullBackupId is not null && _lastFullEntryCount == 0
                    ? RepoContextBackupState.CapturedNothing
                    : RepoContextBackupState.Protected;
            }
        }
    }

    /// <summary>
    /// How many backups of the configured tree the external sink already held when
    /// this container enumerated it at startup, or <c>-1</c> when the sink has not
    /// been enumerated (yet, or at all).
    /// </summary>
    /// <remarks>
    /// This is deliberately distinct from <see cref="CaptureCount"/>, which counts
    /// only what <em>this</em> process captured. After the store is destroyed and
    /// the container restarts, this is the only number that says whether anything
    /// is recoverable at all - the catalog cannot say, because the catalog was
    /// stored in the destroyed tree. Zero and <c>-1</c> are different and important
    /// facts: zero means the sink was readable and empty, <c>-1</c> means it was
    /// never successfully read.
    /// </remarks>
    public int SinkBackupCount
    {
        get { lock (_gate) { return _sinkBackupCount; } }
    }

    /// <summary>The id of the newest backup of the configured tree in the sink, or null.</summary>
    public string? SinkNewestBackupId
    {
        get { lock (_gate) { return _sinkNewestBackupId; } }
    }

    /// <summary>The creation time of the newest backup of the configured tree in the sink, or null.</summary>
    public DateTimeOffset? SinkNewestAtUtc
    {
        get { lock (_gate) { return _sinkNewestAtUtc; } }
    }

    /// <summary>
    /// The type name of the resolved backup sink when that sink reported itself
    /// NOT durable, or null when the sink is durable or has not been probed.
    /// </summary>
    /// <remarks>
    /// A non-durable sink stores backups inside the very cluster they protect, so
    /// captures against one succeed, report success, and are destroyed by the same
    /// gesture that destroys the source tree. That is the sharpest form of the
    /// false-protection failure this wiring exists to remove, so it is surfaced as
    /// its own field rather than folded into the generic failure message.
    /// </remarks>
    public string? NonDurableSinkType
    {
        get { lock (_gate) { return _nonDurableSinkType; } }
    }

    /// <summary>
    /// Records that the resolved sink reported itself not durable.
    /// </summary>
    /// <param name="sinkTypeName">The resolved sink's type name. Must not be null or whitespace.</param>
    /// <exception cref="ArgumentException"><paramref name="sinkTypeName"/> is null or whitespace.</exception>
    public void RecordNonDurableSink(string sinkTypeName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sinkTypeName);

        lock (_gate)
        {
            _nonDurableSinkType = sinkTypeName;
        }
    }

    /// <summary>
    /// Records what the external sink already holds for the configured tree, as
    /// read at startup, so the health signal can state what is recoverable before
    /// this container has captured anything itself.
    /// </summary>
    /// <param name="backupCount">The number of backups of the configured tree found in the sink.</param>
    /// <param name="newestBackupId">The newest such backup's id, or null when there are none.</param>
    /// <param name="newestAtUtc">The newest such backup's creation time, or null when there are none.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="backupCount"/> is negative.</exception>
    public void RecordSinkInventory(int backupCount, string? newestBackupId, DateTimeOffset? newestAtUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(backupCount);

        lock (_gate)
        {
            _sinkBackupCount = backupCount;
            _sinkNewestBackupId = newestBackupId;
            _sinkNewestAtUtc = newestAtUtc;
        }
    }

    /// <summary>
    /// Records a completed capture from the manifest the capture service returned.
    /// </summary>
    /// <param name="backupId">The captured backup's id.</param>
    /// <param name="capturedTreeId">The tree id read off the manifest's scope.</param>
    /// <param name="entryCount">The number of key descriptors the manifest carries.</param>
    /// <param name="isFull">Whether the resulting manifest is a full capture.</param>
    /// <param name="requestedIncremental">Whether an incremental capture was what the host asked for.</param>
    /// <param name="capturedAtUtc">The manifest's creation time.</param>
    /// <exception cref="ArgumentNullException"><paramref name="backupId"/> or <paramref name="capturedTreeId"/> is null.</exception>
    public void RecordCapture(
        string backupId,
        string capturedTreeId,
        int entryCount,
        bool isFull,
        bool requestedIncremental,
        DateTimeOffset capturedAtUtc)
    {
        ArgumentNullException.ThrowIfNull(backupId);
        ArgumentNullException.ThrowIfNull(capturedTreeId);

        lock (_gate)
        {
            _capturedTreeId = capturedTreeId;
            _captureCount++;
            _lastFailure = null;

            if (isFull)
            {
                _lastFullBackupId = backupId;
                _lastFullAtUtc = capturedAtUtc;
                _lastFullEntryCount = entryCount;

                // An incremental that came back as a full manifest is the silent
                // promotion described on this type. Count it where it happened.
                if (requestedIncremental)
                {
                    _incrementalFallbackCount++;
                }
            }
            else
            {
                _lastIncrementalBackupId = backupId;
                _lastIncrementalAtUtc = capturedAtUtc;
                _lastIncrementalEntryCount = entryCount;
            }
        }
    }

    /// <summary>Records a capture failure so it is visible alongside the successes.</summary>
    /// <param name="message">The failure message.</param>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
    public void RecordFailure(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        lock (_gate) { _lastFailure = message; }
    }

    /// <summary>
    /// Renders the positive statement as one line, suitable for a startup log, a
    /// health-endpoint payload, or an operator asking "is my agent memory actually
    /// backed up?".
    /// </summary>
    /// <returns>A single-line human-readable summary. Never null.</returns>
    public string Describe()
    {
        if (!Enabled)
        {
            return "RepoContext memory backup is DISABLED: no external backup sink is configured, "
                + $"so the '{ScopedTreeId}' tree is NOT being captured anywhere. "
                + $"Set {RepoContextBackup.BlobConnectionStringKey} to enable it.";
        }

        lock (_gate)
        {
            var durabilityNote = _nonDurableSinkType is null
                ? string.Empty
                : $" WARNING: the resolved sink '{_nonDurableSinkType}' is NOT durable, so backups are "
                    + "stored inside the very cluster they protect and will be destroyed with it.";

            var sinkNote = _sinkBackupCount switch
            {
                < 0 => string.Empty,
                0 => " The sink currently holds NO backup of this tree, so nothing is recoverable from it yet.",
                _ => string.Create(
                    CultureInfo.InvariantCulture,
                    $" The sink holds {_sinkBackupCount} backup(s) of this tree, newest "
                    + $"'{_sinkNewestBackupId}' from "
                    + $"{_sinkNewestAtUtc?.ToString("O", CultureInfo.InvariantCulture) ?? "(unknown)"}."),
            };

            if (_captureCount == 0)
            {
                return ($"RepoContext memory backup is ENABLED for tree '{ScopedTreeId}' but has "
                    + "captured NOTHING yet."
                    + durabilityNote
                    + sinkNote
                    + (_lastFailure is null ? string.Empty : $" Last failure: {_lastFailure}")).Trim();
            }

            var scopeNote = string.Equals(_capturedTreeId, ScopedTreeId, StringComparison.Ordinal)
                ? string.Empty
                : $" WARNING: the captured scope '{_capturedTreeId}' is NOT the configured scope '{ScopedTreeId}'.";

            var emptyNote = _lastFullEntryCount == 0 && _lastFullBackupId is not null
                ? " WARNING: the last full capture described ZERO entries, so it protects nothing."
                : string.Empty;

            var fallbackNote = _incrementalFallbackCount == 0
                ? string.Empty
                : $" {_incrementalFallbackCount.ToString(CultureInfo.InvariantCulture)} incremental "
                    + "capture(s) were silently promoted to full captures by the capture service.";

            return string.Create(
                CultureInfo.InvariantCulture,
                $"RepoContext memory backup captured tree '{_capturedTreeId}': "
                + $"last full '{_lastFullBackupId ?? "(none)"}' at "
                + $"{_lastFullAtUtc?.ToString("O", CultureInfo.InvariantCulture) ?? "(never)"} "
                + $"describing {_lastFullEntryCount} entries; "
                + $"last incremental '{_lastIncrementalBackupId ?? "(none)"}' at "
                + $"{_lastIncrementalAtUtc?.ToString("O", CultureInfo.InvariantCulture) ?? "(never)"} "
                + $"describing {_lastIncrementalEntryCount} entries; "
                + $"{_captureCount} capture(s) total.{durabilityNote}{scopeNote}{emptyNote}{fallbackNote}")
                .Trim();
        }
    }
}
