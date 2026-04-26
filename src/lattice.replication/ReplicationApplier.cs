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
            ReplogOp.Set => entry.Mode switch
            {
                ReplicationMode.LwwRegister => apply.ApplySetAsync(
                    entry.Key,
                    entry.Value!,
                    entry.Timestamp,
                    entry.OriginClusterId!,
                    entry.ExpiresAtTicks),
                ReplicationMode.OrSet => ApplyStateMergeAsync<OrSet>(
                    entry,
                    static (existing, incoming) => existing.MergeFrom(incoming),
                    static () => new OrSet()),
                ReplicationMode.PnCounter => ApplyStateMergeAsync<PnCounter>(
                    entry,
                    static (existing, incoming) => existing.MergeFrom(incoming),
                    static () => new PnCounter()),
                ReplicationMode.VersionVector => ApplyStateMergeAsync<VersionVector>(
                    entry,
                    static (existing, incoming) => existing.MergeFrom(incoming),
                    static () => new VersionVector()),
                _ => throw new InvalidOperationException(
                    $"ReplogEntry on tree '{entry.TreeId}' carries unrecognised replication mode '{entry.Mode}' "
                    + "(value="
                    + ((int)entry.Mode).ToString(System.Globalization.CultureInfo.InvariantCulture)
                    + "). The receiver has no apply rule registered for this mode; in a future release such "
                    + "entries will be routed to a dead-letter queue."),
            },
            ReplogOp.Delete => apply.ApplyDeleteAsync(
                entry.Key,
                entry.Timestamp,
                entry.OriginClusterId!),
            _ => throw new InvalidOperationException(
                $"Unsupported point-apply op {entry.Op} for entry on tree '{entry.TreeId}'."),
        };
    }

    /// <summary>
    /// CAS retry budget for the read-merge-write loop used by typed CRDT
    /// state-merge applies (<see cref="ReplicationMode.OrSet"/>,
    /// <see cref="ReplicationMode.PnCounter"/>, <see cref="ReplicationMode.VersionVector"/>).
    /// Mirrors the budget the typed accessors (<see cref="OrSetAccessor.DefaultMaxAttempts"/>,
    /// <see cref="PnCounterAccessor.DefaultMaxAttempts"/>,
    /// <see cref="VersionVectorAccessor.DefaultMaxAttempts"/>) use for the
    /// authoring side, so a typical fan-in matches.
    /// </summary>
    private const int StateMergeMaxAttempts = 16;

    private async Task ApplyStateMergeAsync<TState>(
        ReplogEntry entry,
        Action<TState, TState> merge,
        Func<TState> emptyFactory)
        where TState : class
    {
        if (entry.Value is null)
        {
            throw new ArgumentException(
                $"ReplogEntry.Value must be non-null for {entry.Mode} state-merge apply.",
                nameof(entry));
        }

        var lattice = grainFactory.GetGrain<ILattice>(entry.TreeId);
        var serializer = JsonLatticeSerializer<TState>.Default;
        var incoming = serializer.Deserialize(entry.Value);

        // Stamp the remote origin onto the receiver-side mutation so the
        // outbound change-feed observer publishes the foreign origin and
        // the producer's outbound ship loop filters the resulting entry
        // back out (the durable, async-boundary-safe successor to the
        // legacy thread-local replay flag).
        using var scope = LatticeOriginContext.With(entry.OriginClusterId);

        for (var attempt = 0; attempt < StateMergeMaxAttempts; attempt++)
        {
            var versioned = await lattice.GetWithVersionAsync(entry.Key);
            var existing = versioned.Value is null
                ? emptyFactory()
                : serializer.Deserialize(versioned.Value);
            merge(existing, incoming);
            var bytes = serializer.Serialize(existing);
            var ok = await lattice.SetIfVersionAsync(entry.Key, bytes, versioned.Version);
            if (ok)
            {
                return;
            }
        }

        throw new InvalidOperationException(
            $"Replication state-merge CAS budget exhausted after {StateMergeMaxAttempts} attempts on tree "
            + $"'{entry.TreeId}', key '{entry.Key}', mode '{entry.Mode}'. The receiver could not install the "
            + "merged state under optimistic concurrency; reduce contention on this key or increase the "
            + "budget in a future configuration knob.");
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
