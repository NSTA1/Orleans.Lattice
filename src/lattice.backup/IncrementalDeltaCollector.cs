using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Wal;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Drives the forward write-ahead-log drain for an incremental capture and streams
/// the delta to the sink. Resumes from the base backup's per-partition offsets,
/// tails every partition through the shared commit-log subscription seam (all
/// origins, maintenance-filtered, fall-off aware), scope-filters the surfaced
/// entries, and emits each drained page as a serialized
/// <see cref="LwwEntry"/> array chunk - byte-identical in shape to a full
/// capture, so the restore chain replay decodes a base and its increments through
/// one uniform path. A point set surfaces as a live entry and a point delete as a
/// tombstone entry, each carrying its full last-writer-wins envelope (value,
/// hybrid-logical-clock, expiry, origin, and version-vector).
/// <para>
/// The content digest is seeded with the base backup id so two empty increments
/// off different bases never collide on a content address, keeping the chained
/// backup id unique while an identical retry off the same base stays idempotent.
/// The delta is never buffered whole - only one drained page is held at a time.
/// </para>
/// <para>
/// A range delete has no faithful point-keyed representation in the uniform
/// entry-array artifact (its effect depends on which keys are live at replay), so
/// when one surfaces in the delta window the collector raises
/// <see cref="RequiresFullFallback"/> and the caller captures a fresh full backup
/// instead of emitting a delta that a chain restore could not fold correctly.
/// </para>
/// </summary>
internal sealed class IncrementalDeltaCollector : IWalSubscriptionHandler
{
    private readonly Serializer _serializer;
    private readonly IWalSubscriber _subscriber;
    private readonly string _treeId;
    private readonly string _consumerId;
    private readonly int _partitions;
    private readonly IReadOnlyDictionary<int, long> _baseOffsets;
    private readonly string? _startInclusive;
    private readonly string? _endExclusive;
    private readonly BackupKeyMergeMode _mergeMode;
    private readonly int _batchSize;

    private readonly IncrementalHash _hasher;
    private readonly string _baseBackupId;
    private readonly List<BackupKeyDescriptor> _keyDescriptors = new();
    private readonly Dictionary<string, long> _perOriginHighWater = new(StringComparer.Ordinal);
    private readonly Dictionary<int, long> _maxAdvanced = new();
    private readonly List<LwwEntry> _pending = new();

    private long _byteLength;
    private int _chunkCount;
    private string? _contentHash;
    private string? _backupId;
    private HybridLogicalClock _highestHlc = HybridLogicalClock.Zero;

    /// <summary>Initializes a new <see cref="IncrementalDeltaCollector"/>.</summary>
    public IncrementalDeltaCollector(
        Serializer serializer,
        IWalSubscriber subscriber,
        string treeId,
        string consumerId,
        int partitions,
        IReadOnlyDictionary<int, long> baseOffsets,
        string? startInclusive,
        string? endExclusive,
        BackupKeyMergeMode mergeMode,
        string baseBackupId,
        int batchSize)
    {
        _serializer = serializer;
        _subscriber = subscriber;
        _treeId = treeId;
        _consumerId = consumerId;
        _partitions = partitions;
        _baseOffsets = baseOffsets;
        _startInclusive = startInclusive;
        _endExclusive = endExclusive;
        _mergeMode = mergeMode;
        _batchSize = batchSize;
        _baseBackupId = baseBackupId;

        // The content digest hashes only the streamed artifact bytes, so the restore
        // integrity gate (which re-hashes the artifact alone) reproduces it exactly.
        // Base-relative uniqueness of the chained backup id is folded in separately
        // (see BackupId), keeping the content address a pure hash of the payload.
        _hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    }

    /// <summary>The per-key descriptors captured, in drain order.</summary>
    public IReadOnlyList<BackupKeyDescriptor> KeyDescriptors => _keyDescriptors;

    /// <summary>The per-origin causal high-water of the captured delta.</summary>
    public IReadOnlyDictionary<string, long> PerOriginHighWater => _perOriginHighWater;

    /// <summary>The total serialized byte length streamed to the sink.</summary>
    public long ByteLength => _byteLength;

    /// <summary>The number of chunks (drained pages) streamed to the sink.</summary>
    public int ChunkCount => _chunkCount;

    /// <summary>The highest hybrid-logical-clock read across the drain (surfaced or not).</summary>
    public HybridLogicalClock HighestHlc => _highestHlc;

    /// <summary>
    /// <c>true</c> when the WAL trimmed past the base resume point mid-drain, so the
    /// caller must fall back to a fresh full backup rather than emit a torn increment.
    /// </summary>
    public bool FellOffLog { get; private set; }

    /// <summary>
    /// <c>true</c> when a range delete surfaced in the delta window. A range delete
    /// cannot be faithfully replayed from the uniform point-keyed entry artifact, so
    /// the caller falls back to a fresh full backup instead of a delta.
    /// </summary>
    public bool RequiresFullFallback { get; private set; }

    /// <summary>
    /// The lowercase hexadecimal SHA-256 content address of the streamed delta bytes
    /// alone. This is what the restore integrity gate re-hashes, so it is recorded on
    /// the content descriptor. Available only after <see cref="StreamAsync"/> has been
    /// fully enumerated.
    /// </summary>
    public string ContentHash =>
        _contentHash ?? throw new InvalidOperationException(
            "The content hash is not available until the delta stream has been fully drained.");

    /// <summary>
    /// The lowercase hexadecimal SHA-256 chained backup id: the content hash folded
    /// with the base backup id so two empty increments off different bases never
    /// collide on a manifest id, while an identical retry off the same base stays
    /// idempotent. Available only after <see cref="StreamAsync"/> has been fully
    /// enumerated.
    /// </summary>
    public string BackupId =>
        _backupId ?? throw new InvalidOperationException(
            "The backup id is not available until the delta stream has been fully drained.");

    /// <inheritdoc />
    public void OnEntry(in WalSubscriptionEntry entry)
    {
        var mutation = entry.Mutation;

        // Keep only restorable user data. Saga terminal marks (TxCommit / TxAbort)
        // and tombstone-reap compaction marks carry no key-value to restore; the
        // subscriber already skips maintenance-category entries.
        if (mutation.Kind is MutationKind.TxCommit or MutationKind.TxAbort or MutationKind.Tombstone)
        {
            return;
        }

        if (mutation.Kind == MutationKind.DeleteRange)
        {
            // A range delete that overlaps the scope forces a full-backup fallback:
            // the uniform point-keyed entry artifact cannot encode a range whose
            // replay effect depends on which keys are live at fold time.
            if (RangeIntersectsScope(mutation))
            {
                RequiresFullFallback = true;
            }

            return;
        }

        if (!KeyInScope(mutation.Key))
        {
            return;
        }

        var origin = string.IsNullOrEmpty(mutation.OriginClusterId) ? null : mutation.OriginClusterId;

        _pending.Add(new LwwEntry
        {
            Key = mutation.Key,
            Value = mutation.Kind == MutationKind.Delete ? null : mutation.Value,
            Timestamp = mutation.Timestamp,
            IsTombstone = mutation.Kind == MutationKind.Delete,
            ExpiresAtTicks = mutation.ExpiresAtTicks,
            OriginClusterId = origin,
            VectorClock = mutation.VectorClock,
        });

        if (!string.IsNullOrEmpty(mutation.Key))
        {
            _keyDescriptors.Add(new BackupKeyDescriptor(mutation.Key, _mergeMode, origin));
        }

        if (origin is { } originId)
        {
            var ticks = mutation.Timestamp.WallClockTicks;
            if (ticks < 0)
            {
                ticks = 0;
            }
            if (!_perOriginHighWater.TryGetValue(originId, out var current) || ticks > current)
            {
                _perOriginHighWater[originId] = ticks;
            }
        }
    }

    /// <summary>
    /// The new per-partition next-offset (head) frontier reached by the drain,
    /// keyed by partition index. Partitions that surfaced no new entries carry the
    /// base offset forward so the next increment resumes cleanly. Available after
    /// <see cref="StreamAsync"/> has been fully enumerated.
    /// </summary>
    public IReadOnlyDictionary<int, long> NewPartitionOffsets()
    {
        var offsets = new Dictionary<int, long>(_partitions);
        for (var partition = 0; partition < _partitions; partition++)
        {
            offsets[partition] = _maxAdvanced.TryGetValue(partition, out var advanced)
                ? advanced + 1
                : _baseOffsets.GetValueOrDefault(partition, 0L);
        }

        return offsets;
    }

    /// <summary>
    /// Drains the WAL forward from the base offsets, yielding each drained page's
    /// serialized delta bytes. Finalizes <see cref="ContentHash"/> when the WAL is
    /// fully caught up (or on fall-off / range-delete fallback).
    /// </summary>
    /// <param name="cancellationToken">Cancels the drain.</param>
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> StreamAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Checkpoint semantics: a checkpoint is the last applied offset, so the
        // base head H resumes at checkpoint H-1 (H=0 resumes at -1, i.e. the
        // start of the WAL).
        var checkpoints = new Dictionary<int, long>(_partitions);
        for (var partition = 0; partition < _partitions; partition++)
        {
            checkpoints[partition] = _baseOffsets.GetValueOrDefault(partition, 0L) - 1;
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _pending.Clear();

            var context = new WalSubscriptionContext(_treeId, _consumerId, _partitions, checkpoints)
            {
                BatchSize = _batchSize,
                MaintenancePolicy = WalMaintenancePolicy.Skip,
                // The caller owns the WAL pin (a fixed floor at the base frontier
                // during the drain, advanced to the increment frontier after), so
                // the per-pass forward-advancing pin is disabled here to avoid
                // self-inflicting a fall-off on a lagging partition.
                PinWal = false,
            };

            var result = await _subscriber.DrainAsync(context, this, cancellationToken).ConfigureAwait(false);

            if (result.FellOffLog)
            {
                FellOffLog = true;
                break;
            }

            if (result.HighestTimestamp > _highestHlc)
            {
                _highestHlc = result.HighestTimestamp;
            }

            foreach (var (partition, offset) in result.AdvancedOffsets)
            {
                checkpoints[partition] = offset;
                if (!_maxAdvanced.TryGetValue(partition, out var current) || offset > current)
                {
                    _maxAdvanced[partition] = offset;
                }
            }

            if (_pending.Count > 0)
            {
                var page = _pending.ToArray();
                var bytes = _serializer.SerializeToArray(page);
                _hasher.AppendData(bytes);
                _byteLength += bytes.Length;
                _chunkCount++;
                yield return bytes;
            }

            // A range delete in the window means the whole capture is abandoned for
            // a full backup; stop draining and let the caller fall back.
            if (RequiresFullFallback)
            {
                break;
            }

            if (result.EntriesRead == 0)
            {
                break;
            }
        }

        _contentHash = Convert.ToHexStringLower(_hasher.GetHashAndReset());
        _hasher.Dispose();

        // Fold the base backup id into the chained backup id so an empty delta off a
        // different base yields a distinct manifest id, while the content descriptor
        // keeps the pure payload hash for the restore integrity gate.
        var idBytes = Encoding.UTF8.GetBytes(
            $"{_baseBackupId}{(char)BackupConstants.KeySeparator}{_contentHash}");
        _backupId = Convert.ToHexStringLower(SHA256.HashData(idBytes));
    }

    private bool RangeIntersectsScope(in LatticeMutation mutation)
    {
        // A range delete intersects the scope when its half-open range
        // [Key, EndExclusiveKey) overlaps the scope range [start, end).
        var rangeStart = mutation.Key;
        var rangeEnd = mutation.EndExclusiveKey;
        var startsBeforeScopeEnd = _endExclusive is null
            || string.CompareOrdinal(rangeStart, _endExclusive) < 0;
        var endsAfterScopeStart = _startInclusive is null
            || rangeEnd is null
            || string.CompareOrdinal(rangeEnd, _startInclusive) > 0;
        return startsBeforeScopeEnd && endsAfterScopeStart;
    }

    private bool KeyInScope(string key)
    {
        if (_startInclusive is not null && string.CompareOrdinal(key, _startInclusive) < 0)
        {
            return false;
        }
        if (_endExclusive is not null && string.CompareOrdinal(key, _endExclusive) >= 0)
        {
            return false;
        }
        return true;
    }
}
