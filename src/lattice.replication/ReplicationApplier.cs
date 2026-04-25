using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationApplier"/> implementation. Resolves
/// the per-origin high-water-mark for the entry's
/// <c>(treeId, originClusterId)</c> pair, filters re-delivery, and
/// routes the entry through the core library's
/// <see cref="IReplicationApplyGrain"/> seam so the persisted
/// <c>LwwValue&lt;byte[]&gt;</c> carries the remote cluster's HLC and
/// origin id verbatim. The HWM is advanced only after the apply
/// returns successfully.
/// </summary>
internal sealed class ReplicationApplier(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options) : IReplicationApplier
{
    /// <inheritdoc />
    public async Task<ApplyResult> ApplyAsync(ReplogEntry entry, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrEmpty(entry.TreeId))
        {
            throw new ArgumentException("ReplogEntry.TreeId must be non-empty.", nameof(entry));
        }

        if (string.IsNullOrEmpty(entry.OriginClusterId))
        {
            throw new ArgumentException(
                "ReplogEntry.OriginClusterId must be non-empty for replication apply.",
                nameof(entry));
        }

        var resolved = options.Get(entry.TreeId);
        if (string.Equals(entry.OriginClusterId, resolved.ClusterId, StringComparison.Ordinal))
        {
            // Defence-in-depth: a local-origin entry must never be applied
            // back onto its authoring cluster. The outbound ship loop's
            // origin filter already prevents this in the steady state, but
            // hand-built apply pipelines and tests can still hand us such
            // an entry — surface it as an explicit no-op rather than
            // silently merging into the same cluster's state. The HWM is
            // not consulted here so we report Zero — saves a needless
            // grain call against a row that should never carry state.
            return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
        }

        // Range deletes carry HybridLogicalClock.Zero by design (the walk
        // produces many per-leaf HLCs that cannot be faithfully collapsed),
        // so per-origin HWM dedupe does not apply to them. Range applies
        // are naturally idempotent at the leaf layer — re-running a range
        // delete on already-tombstoned keys is a no-op. The HWM is not
        // consulted, so we return Zero rather than incurring a grain call
        // for an informational value that the caller cannot use.
        if (entry.Op == ReplogOp.DeleteRange)
        {
            await ApplyRangeAsync(entry, cancellationToken);
            return new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero };
        }

        var hwmGrain = GetHwmGrain(entry.TreeId, entry.OriginClusterId);
        var hwm = await hwmGrain.GetAsync(cancellationToken);
        if (entry.Timestamp <= hwm)
        {
            return new ApplyResult { Applied = false, HighWaterMark = hwm };
        }

        await ApplyPointAsync(entry);

        // Advance the HWM only after the apply commits. TryAdvanceAsync is
        // monotonic; under steady single-threaded grain semantics this call
        // returns true and the new HWM equals entry.Timestamp. A concurrent
        // applier that raced ahead would leave us with the higher HWM and
        // TryAdvanceAsync returns false — fall back to a fetch only in
        // that rare case so the steady-state apply costs one fewer grain
        // call than a naive read-after-write.
        var advanced = await hwmGrain.TryAdvanceAsync(entry.Timestamp, cancellationToken);
        var newHwm = advanced
            ? entry.Timestamp
            : await hwmGrain.GetAsync(cancellationToken);
        return new ApplyResult { Applied = true, HighWaterMark = newHwm };
    }

    private Task ApplyPointAsync(ReplogEntry entry)
    {
        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);
        return entry.Op switch
        {
            ReplogOp.Set when entry.Value is null
                => throw new ArgumentException(
                    "ReplogEntry.Value must be non-null for ReplogOp.Set.",
                    nameof(entry)),
            ReplogOp.Set => apply.ApplySetAsync(
                entry.Key,
                entry.Value!,
                entry.Timestamp,
                entry.OriginClusterId!,
                entry.ExpiresAtTicks),
            ReplogOp.Delete => apply.ApplyDeleteAsync(
                entry.Key,
                entry.Timestamp,
                entry.OriginClusterId!),
            _ => throw new InvalidOperationException(
                $"Unsupported point-apply op {entry.Op} for entry on tree '{entry.TreeId}'."),
        };
    }

    private Task ApplyRangeAsync(ReplogEntry entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (entry.EndExclusiveKey is null)
        {
            throw new ArgumentException(
                "ReplogEntry.EndExclusiveKey must be non-null for ReplogOp.DeleteRange.",
                nameof(entry));
        }

        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);
        return apply.ApplyDeleteRangeAsync(entry.Key, entry.EndExclusiveKey, entry.OriginClusterId!);
    }

    private IReplicationHighWaterMarkGrain GetHwmGrain(string treeId, string originClusterId) =>
        grainFactory.GetGrain<IReplicationHighWaterMarkGrain>($"{treeId}/{originClusterId}");
}
