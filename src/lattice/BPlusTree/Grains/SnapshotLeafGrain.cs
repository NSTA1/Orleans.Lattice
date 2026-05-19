using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Implementation of <see cref="ISnapshotLeafGrain"/>. Transient
/// (in-memory only) per-shard snapshot leaf used by zero-observable-
/// writes snapshot cursors: rebuilds a read-only view of one shard's
/// projection by replaying the per-shard write-ahead log up to the
/// captured offset, then serves range-scan queries off that view.
/// <para>
/// Idle-evicts after
/// <see cref="LatticeOptions.SnapshotLeafIdleTtl"/>; a subsequent
/// access transparently rebuilds via
/// <c>ILeafReplayCoordinatorGrain</c>. The underlying WAL prefix is
/// kept alive by the snapshot's <c>IWalCursorRegistry</c> pin held
/// by the owning <see cref="LatticeCursorGrain"/>.
/// </para>
/// </summary>
internal sealed class SnapshotLeafGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<SnapshotLeafGrain> logger) : Grain, ISnapshotLeafGrain
{
    /// <summary>Tree this snapshot leaf belongs to (set on first <see cref="OpenAsync"/>).</summary>
    private string _treeId = string.Empty;

    /// <summary>Virtual shard index this snapshot leaf materialises.</summary>
    private int _shardIndex = -1;

    /// <summary>Upper-bound (exclusive) WAL offset the snapshot replays to.</summary>
    private long _capturedOffset = -1;

    /// <summary>
    /// True once the WAL replay has completed and the snapshot
    /// projection is stable.
    /// </summary>
    private bool _opened;

    /// <inheritdoc />
    public Task OpenAsync(string treeId, int shardIndex, long capturedOffset, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (shardIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(shardIndex), "Shard index must be non-negative.");
        if (capturedOffset < 0)
            throw new ArgumentOutOfRangeException(nameof(capturedOffset), "Captured offset must be non-negative.");
        cancellationToken.ThrowIfCancellationRequested();

        if (_opened)
        {
            // Idempotent re-open with the same coordinate is a no-op;
            // a different coordinate would target a different grain
            // key, so a mismatch here indicates a programming error
            // upstream and must surface loudly.
            if (_treeId != treeId || _shardIndex != shardIndex || _capturedOffset != capturedOffset)
            {
                throw new InvalidOperationException(
                    $"SnapshotLeafGrain for '{this.GetPrimaryKeyString()}' was already opened against ({_treeId}, {_shardIndex}, {_capturedOffset}); refusing to re-open against ({treeId}, {shardIndex}, {capturedOffset}).");
            }
            return Task.CompletedTask;
        }

        _treeId = treeId;
        _shardIndex = shardIndex;
        _capturedOffset = capturedOffset;

        // Replay materialisation lands in commit 3/5. For the skeleton
        // commit, the snapshot leaf opens to an empty projection so
        // the wiring layers above can be exercised end-to-end without
        // dragging the WAL-replay loop's failure modes into the
        // skeleton's scope.
        _opened = true;

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug(
                "SnapshotLeafGrain opened: tree={TreeId}, shard={ShardIndex}, capturedOffset={CapturedOffset}.",
                treeId, shardIndex, capturedOffset);
        }

        // Reference unused-during-skeleton dependencies so the analyser
        // does not flag them; commit 3 wires them into the replay loop.
        _ = grainFactory;
        _ = optionsMonitor;
        _ = context;

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null)
    {
        EnsureOpened();
        // Skeleton: empty projection - real replay-backed scan lands
        // in commit 3.
        return Task.FromResult(new List<string>());
    }

    /// <inheritdoc />
    public Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null)
    {
        EnsureOpened();
        return Task.FromResult(new List<KeyValuePair<string, byte[]>>());
    }

    /// <summary>
    /// Validates that <see cref="OpenAsync"/> has been called before
    /// any read; throws <see cref="InvalidOperationException"/>
    /// otherwise to surface a wiring bug rather than silently
    /// returning empty pages.
    /// </summary>
    private void EnsureOpened()
    {
        if (!_opened)
        {
            throw new InvalidOperationException(
                $"SnapshotLeafGrain for '{this.GetPrimaryKeyString()}' has not been opened. Call OpenAsync before reading.");
        }
    }
}
