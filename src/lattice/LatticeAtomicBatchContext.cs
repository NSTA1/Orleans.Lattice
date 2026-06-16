using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient atomic-batch context used to stamp
/// <see cref="LatticeMutation.AtomicBatchSize"/> /
/// <see cref="LatticeMutation.AtomicBatchIndex"/> onto the per-key
/// mutations emitted by an in-flight atomic transaction (a
/// <c>SetManyAtomicAsync</c> saga).
/// </summary>
/// <remarks>
/// <para>
/// Atomic-batch metadata flows on the inbound write path via an Orleans
/// <see cref="RequestContext"/> entry keyed <c>"ol.batch"</c>. The
/// <c>AtomicWriteGrain</c> coordinator captures the batch size once on
/// the first <c>Prepare</c>, persists it on its grain state, and
/// re-stamps a <c>(Size, Index)</c> pair onto this ambient at the head
/// of every per-key call it issues (including compensation rolls) so
/// the leaf grain mutation publish helpers can read the pair and
/// stamp the corresponding wire slots. Single-key writes outside a
/// saga leave the context unset; the publish helpers default both
/// slots to <c>0</c>.
/// </para>
/// <para>
/// The library itself does not interpret the pair beyond carrying it
/// through to <see cref="LatticeMutation"/>; replication consumers
/// read the slots to reconstruct atomic-batch sibling membership for
/// receiver-side staging.
/// </para>
/// </remarks>
public static class LatticeAtomicBatchContext
{
    /// <summary>
    /// Gets or sets the atomic-batch <c>(Size, Index)</c> pair on the
    /// ambient <see cref="RequestContext"/>. Setting <see langword="null"/>
    /// removes the key rather than storing a null value, matching the
    /// "not in a saga" default.
    /// </summary>
    public static (int Size, int Index)? Current
    {
        get => RequestContext.Get(LatticeEventConstants.AtomicBatchRequestContextKey)
            is ValueTuple<int, int> pair
            ? (pair.Item1, pair.Item2)
            : null;
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.AtomicBatchRequestContextKey);
            }
            else
            {
                RequestContext.Set(
                    LatticeEventConstants.AtomicBatchRequestContextKey,
                    (value.Value.Size, value.Value.Index));
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="batch"/> for the
    /// lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    /// <param name="batch">
    /// The atomic-batch <c>(Size, Index)</c> pair to stamp onto
    /// mutations authored inside the scope, or <see langword="null"/>
    /// to explicitly clear the ambient context.
    /// </param>
    public static IDisposable With((int Size, int Index)? batch)
    {
        var previous = Current;
        Current = batch;
        return new Scope(previous, CurrentIndexMap, restoreMap: false, CurrentDeltaMap, restoreDeltaMap: false);
    }

    /// <summary>
    /// Gets or sets the optional <c>key -> globalIndex</c> map on the
    /// ambient <see cref="RequestContext"/>. The map exists to
    /// preserve per-entry <see cref="LatticeMutation.AtomicBatchIndex"/>
    /// stamping through the <c>LatticeGrain.SetManyAsync</c>
    /// shard-bucketing fan-out: when a saga issues a single batched
    /// <c>SetManyAsync</c> covering keys that route to multiple
    /// shards, the leaf-side batched commit path looks each entry's
    /// key up in this map to recover its saga-global index regardless
    /// of bucket-local ordering. Setting <see langword="null"/>
    /// removes the key, matching the "no map" default that triggers
    /// the publish helpers' <c>BaseIndex + bucketLocal</c> fallback.
    /// </summary>
    public static IReadOnlyDictionary<string, int>? CurrentIndexMap
    {
        get => RequestContext.Get(LatticeEventConstants.AtomicBatchIndexMapRequestContextKey)
            as IReadOnlyDictionary<string, int>;
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.AtomicBatchIndexMapRequestContextKey);
            }
            else
            {
                RequestContext.Set(
                    LatticeEventConstants.AtomicBatchIndexMapRequestContextKey,
                    value);
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> AND <see cref="CurrentIndexMap"/>
    /// for the lifetime of the returned scope. Restores both prior
    /// values on <see cref="IDisposable.Dispose"/>. Used by the saga
    /// coordinator to stamp <c>(Size, BaseIndex)</c> alongside the
    /// per-key <c>key -> globalIndex</c> lookup table so the leaf
    /// commit path can recover the saga-global index after
    /// <c>LatticeGrain.SetManyAsync</c> shard-bucketing.
    /// </summary>
    public static IDisposable With(
        (int Size, int Index)? batch,
        IReadOnlyDictionary<string, int>? indexMap)
    {
        var previousBatch = Current;
        var previousMap = CurrentIndexMap;
        Current = batch;
        CurrentIndexMap = indexMap;
        return new Scope(previousBatch, previousMap, restoreMap: true, CurrentDeltaMap, restoreDeltaMap: false);
    }

    /// <summary>
    /// Gets or sets the optional <c>key -> deltaBytes</c> map on the
    /// ambient <see cref="RequestContext"/>. The map lets a single atomic
    /// saga stamp a <em>different</em> author-delta onto each per-entry
    /// emit, overriding the saga-wide <see cref="LatticeDeltaContext"/>
    /// carry; the leaf publish helpers look each committed entry's key up
    /// in this map and prefer its delta when present. Setting
    /// <see langword="null"/> removes the key, matching the "no per-entry
    /// delta" default under which the publish helpers read the saga-wide
    /// <see cref="LatticeDeltaContext.Current"/> carry alone.
    /// </summary>
    public static IReadOnlyDictionary<string, byte[]>? CurrentDeltaMap
    {
        get => RequestContext.Get(LatticeEventConstants.AtomicBatchDeltaMapRequestContextKey)
            as IReadOnlyDictionary<string, byte[]>;
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.AtomicBatchDeltaMapRequestContextKey);
            }
            else
            {
                RequestContext.Set(
                    LatticeEventConstants.AtomicBatchDeltaMapRequestContextKey,
                    value);
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/>, <see cref="CurrentIndexMap"/>, AND
    /// <see cref="CurrentDeltaMap"/> for the lifetime of the returned
    /// scope. Restores all three prior values on
    /// <see cref="IDisposable.Dispose"/>. Used by the saga coordinator to
    /// stamp the per-entry author-delta lookup table alongside the
    /// <c>(Size, BaseIndex)</c> pair and the <c>key -> globalIndex</c> map
    /// so the leaf commit path can stamp each entry's typed CRDT delta -
    /// the carry flag-CRDT membership rows need to converge under a single
    /// cross-tree atomic write.
    /// </summary>
    public static IDisposable With(
        (int Size, int Index)? batch,
        IReadOnlyDictionary<string, int>? indexMap,
        IReadOnlyDictionary<string, byte[]>? deltaMap)
    {
        var previousBatch = Current;
        var previousMap = CurrentIndexMap;
        var previousDeltaMap = CurrentDeltaMap;
        Current = batch;
        CurrentIndexMap = indexMap;
        CurrentDeltaMap = deltaMap;
        return new Scope(previousBatch, previousMap, restoreMap: true, previousDeltaMap, restoreDeltaMap: true);
    }

    private sealed class Scope(
        (int Size, int Index)? previousBatch,
        IReadOnlyDictionary<string, int>? previousMap,
        bool restoreMap,
        IReadOnlyDictionary<string, byte[]>? previousDeltaMap,
        bool restoreDeltaMap) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Current = previousBatch;
            if (restoreMap)
            {
                CurrentIndexMap = previousMap;
            }
            if (restoreDeltaMap)
            {
                CurrentDeltaMap = previousDeltaMap;
            }
        }
    }
}