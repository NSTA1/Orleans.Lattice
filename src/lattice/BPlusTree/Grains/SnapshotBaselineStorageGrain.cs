using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ISnapshotBaselineStorageGrain"/> implementation. Holds a
/// single persisted <see cref="SnapshotShardBaseline"/> per (snapshot cursor,
/// physical shard) via the lattice storage provider configured by
/// <see cref="LatticeOptions.StorageProviderName"/>.
/// <para>
/// Intentionally minimal: one read, one write, one clear. The capture fold
/// (leaf-chain walk plus per-leaf tail replay) is owned by
/// <see cref="ShardRootGrain"/>; this grain only persists the materialised
/// result.
/// </para>
/// <para>
/// A baseline only ever reaches durable storage lazily, when a snapshot scan
/// must survive past its first page (issue #916). The normal lifecycle deletes
/// each row on cursor close or idle-TTL expiry, but a client that abandons a
/// multi-page continuation without closing the cursor would otherwise orphan
/// the row forever. To bound that leak this grain derives from
/// <see cref="TtlGrain{TSelf}"/>: every <see cref="SaveAsync"/> arms a sliding
/// self-clear reminder sized to <see cref="LatticeOptions.SnapshotBaselineTtl"/>,
/// a still-active scan slides it forward via <see cref="TouchAsync"/> (throttled
/// by <see cref="SlideDebounce"/> so the reminder table is not rewritten on
/// every page), and an abandoned baseline is reclaimed automatically once the
/// window elapses with no activity.
/// </para>
/// </summary>
internal sealed class SnapshotBaselineStorageGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<SnapshotBaselineStorageGrain> logger,
    [PersistentState("snapshot-baseline", LatticeOptions.StorageProviderName)]
    IPersistentState<SnapshotShardBaseline> state)
    : TtlGrain<SnapshotBaselineStorageGrain>(context, reminderRegistry, logger), ISnapshotBaselineStorageGrain
{
    private const string RetentionReminderName = "snapshot-baseline-retention";

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => optionsMonitor.Get(ResolveTreeId()).SnapshotBaselineTtl;

    /// <summary>
    /// Throttles sliding-TTL reminder-table writes to at most one per half-TTL
    /// window (floored at one minute). A long active scan slides the retention
    /// window forward, but never rewrites the reminder more than a couple of
    /// times per TTL window. The serving leaf already throttles its
    /// <see cref="TouchAsync"/> calls to the same cadence; this is defence in
    /// depth against any future caller that touches more eagerly.
    /// </summary>
    protected override TimeSpan SlideDebounce
    {
        get
        {
            var ttl = ResolveTtl();
            if (ttl == Timeout.InfiniteTimeSpan)
                return Timeout.InfiniteTimeSpan;
            var half = TimeSpan.FromTicks(ttl.Ticks / 2);
            return half < TimeSpan.FromMinutes(1) ? TimeSpan.FromMinutes(1) : half;
        }
    }

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        if (!state.State.Captured)
            return;

        Logger.LogInformation(
            "Snapshot baseline {Key}: leak-guard retention window expired with the cursor still open; clearing the orphaned frozen baseline.",
            GrainContext.GrainId.Key);
        await state.ClearStateAsync().ConfigureAwait(true);
        state.State = new SnapshotShardBaseline();
    }

    /// <inheritdoc />
    public async Task SaveAsync(SnapshotShardBaseline baseline, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(baseline);
        cancellationToken.ThrowIfCancellationRequested();

        baseline.Captured = true;
        state.State = baseline;
        await state.WriteStateAsync().ConfigureAwait(true);

        // Arm (or refresh) the leak-guard reminder. SlideTtlAsync swallows
        // reminder-registry faults, so a host without reminders configured still
        // persists the baseline - it just forgoes the automatic backstop.
        await SlideTtlAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task<SnapshotShardBaseline?> LoadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // The default-allocated state row carries Captured == false; a real
        // baseline is stamped Captured == true on save. This is the only way
        // to tell "no baseline was ever written" apart from "a freshly
        // defaulted state row was returned by the provider".
        if (!state.State.Captured)
        {
            return null;
        }

        // A reload implies a cursor is paging this baseline again (failover or
        // post-eviction rebuild), so keep it alive. Debounced, so an eager
        // sequence of reloads does not hammer the reminder table.
        await SlideTtlAsync().ConfigureAwait(true);
        return state.State;
    }

    /// <inheritdoc />
    public async Task TouchAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Nothing persisted yet: a touch against an unwritten row would arm a
        // reminder on an empty baseline. Skip so the leak guard only tracks rows
        // that actually exist.
        if (!state.State.Captured)
            return;

        await SlideTtlAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Always unregister the leak-guard reminder, even when there is no row
        // to clear: a previous Save may have armed it before a transient clear
        // reset the in-memory state shape.
        await UnregisterTtlAsync().ConfigureAwait(true);

        if (!state.State.Captured)
        {
            // Nothing to clear; ClearStateAsync still touches the provider, so
            // short-circuit to keep idempotent calls I/O-free.
            return;
        }

        await state.ClearStateAsync().ConfigureAwait(true);

        // After ClearStateAsync the provider resets the in-memory state;
        // defensively re-seed the sentinel so LoadAsync's null contract holds
        // without relying on the provider's post-clear state shape.
        state.State = new SnapshotShardBaseline();
    }

    /// <summary>
    /// Recovers the owning tree id from this grain's compound key
    /// (<c>{treeId}/{shardIndex}/{baselineToken:N}</c>) so the leak-guard TTL is
    /// resolved against the owning tree's options. The token and shard index are
    /// the final two slash-delimited segments; everything before them is the
    /// tree id (which may itself contain slashes). Falls back to the empty
    /// (root) options name if the key is not in the expected shape.
    /// </summary>
    private string ResolveTreeId()
    {
        var key = GrainContext.GrainId.Key.ToString();
        if (string.IsNullOrEmpty(key))
            return string.Empty;
        var lastSlash = key.LastIndexOf('/');
        if (lastSlash <= 0)
            return string.Empty;
        var secondLastSlash = key.LastIndexOf('/', lastSlash - 1);
        return secondLastSlash <= 0 ? string.Empty : key[..secondLastSlash];
    }
}
