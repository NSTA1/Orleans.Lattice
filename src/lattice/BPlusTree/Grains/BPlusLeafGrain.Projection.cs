using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// <see cref="ILeafProjection"/> implementation for <see cref="BPlusLeafGrain"/>.
/// Replays a single durably-committed mutation against the leaf's
/// in-memory state using LWW semantics; persists the projection
/// checkpoint offset alongside the leaf's existing storage row.
/// <para>
/// Ships dormant: today's foreground commit path is unchanged and no
/// caller drives <see cref="ILeafProjection.Apply"/>. The seam is
/// exercised exclusively by unit tests until the WAL-as-sole-commit-point
/// promotion lands.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    void ILeafProjection.Apply(in LatticeMutation mutation)
    {
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                ApplySet(mutation);
                break;
            case MutationKind.Delete:
                ApplyDelete(mutation);
                break;
            case MutationKind.DeleteRange:
                ApplyDeleteRange(mutation);
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(mutation),
                    mutation.Kind,
                    $"Unknown {nameof(MutationKind)} '{mutation.Kind}'.");
        }
    }

    Task<long> ILeafProjection.GetCheckpointOffsetAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(state.State.ProjectionCheckpointOffset);
    }

    async Task ILeafProjection.SetCheckpointOffsetAsync(long offset, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (offset < state.State.ProjectionCheckpointOffset)
        {
            throw new ArgumentOutOfRangeException(
                nameof(offset),
                offset,
                $"Projection checkpoint must be monotonically non-decreasing; current offset is {state.State.ProjectionCheckpointOffset}.");
        }

        if (offset == state.State.ProjectionCheckpointOffset)
        {
            // No-op idempotent advance: still flush so any in-memory
            // projection mutations issued via Apply since the previous
            // checkpoint are committed durably.
            await PersistAsync();
            return;
        }

        state.State.ProjectionCheckpointOffset = offset;
        await PersistAsync();
    }

    private void ApplySet(in LatticeMutation mutation)
    {
        var incoming = new LwwValue<byte[]>
        {
            Value = mutation.IsTombstone ? null : mutation.Value,
            Timestamp = mutation.Timestamp,
            IsTombstone = mutation.IsTombstone,
            ExpiresAtTicks = mutation.ExpiresAtTicks,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };
        MergeIntoProjection(mutation.Key, incoming);
        AdvanceProjectionClock(mutation.Timestamp);
    }

    private void ApplyDelete(in LatticeMutation mutation)
    {
        var tombstone = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = mutation.Timestamp,
            IsTombstone = true,
            ExpiresAtTicks = 0,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };
        MergeIntoProjection(mutation.Key, tombstone);
        AdvanceProjectionClock(mutation.Timestamp);
    }

    private void ApplyDeleteRange(in LatticeMutation mutation)
    {
        var endExclusive = mutation.EndExclusiveKey;
        if (endExclusive is null)
            return;

        var startInclusive = mutation.Key;
        if (string.CompareOrdinal(startInclusive, endExclusive) >= 0)
            return;

        // Tombstone every existing entry inside the range. The mutation
        // carries one HLC for the whole batch; replays converge under LWW
        // because the tombstone's timestamp dominates any earlier write
        // and is dominated by any later write.
        List<string>? toRewrite = null;
        foreach (var (key, _) in state.State.Entries)
        {
            if (string.CompareOrdinal(key, startInclusive) < 0)
                continue;
            if (string.CompareOrdinal(key, endExclusive) >= 0)
                break;
            (toRewrite ??= []).Add(key);
        }

        if (toRewrite is null)
            return;

        var tombstone = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = mutation.Timestamp,
            IsTombstone = true,
            ExpiresAtTicks = 0,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };

        foreach (var key in toRewrite)
        {
            MergeIntoProjection(key, tombstone);
        }

        AdvanceProjectionClock(mutation.Timestamp);
    }

    private void MergeIntoProjection(string key, LwwValue<byte[]> incoming)
    {
        if (state.State.Entries.TryGetValue(key, out var existing))
        {
            state.State.Entries[key] = LwwValue<byte[]>.Merge(existing, incoming);
        }
        else
        {
            state.State.Entries[key] = incoming;
        }
    }

    private void AdvanceProjectionClock(HybridLogicalClock incoming)
    {
        if (incoming > state.State.Clock)
            state.State.Clock = incoming;
    }
}
