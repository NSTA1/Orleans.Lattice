namespace Orleans.Lattice;

using System.Runtime.InteropServices;

/// <summary>
/// A version vector that tracks causal history per replica (grain).
/// Each entry maps a <see cref="GrainId"/> to the highest <see cref="HybridLogicalClock"/>
/// value observed from that grain.
///
/// Merge is pointwise-max: for each replica ID, keep the higher clock.
/// This forms a join-semilattice (commutative, associative, idempotent).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.VersionVector)]
public sealed class VersionVector : ICrdt<VersionVector>
{
    [Id(0)]
    public Dictionary<string, HybridLogicalClock> Entries { get; set; } = [];

    /// <inheritdoc />
    /// <remarks>
    /// A <see cref="VersionVector"/> is bottom when no replica has
    /// ticked - <c>Entries</c> is empty. A vector with entries
    /// at <see cref="HybridLogicalClock.Zero"/> is not bottom because
    /// the entries themselves carry replica identity.
    /// </remarks>
    public bool IsBottom => Entries.Count == 0;

    /// <summary>
    /// Advances the clock for the given <paramref name="replicaId"/> and records
    /// it in this vector. Returns the new clock value.
    /// </summary>
    public HybridLogicalClock Tick(string replicaId)
    {
        // Single probe: GetValueRefOrAddDefault hashes replicaId once and
        // returns a ref to the entry (added zero-initialised when absent).
        // HybridLogicalClock.Zero is default(HybridLogicalClock), so the
        // add-default slot equals the Zero that GetClock returns for a
        // missing replica - the value fed into Tick is identical to the
        // previous TryGetValue-then-indexer-set form, with one fewer hash
        // and bucket walk per tick.
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(Entries, replicaId, out _);
        var next = HybridLogicalClock.Tick(slot);
        slot = next;
        return next;
    }

    /// <summary>
    /// Returns the clock value for the given <paramref name="replicaId"/>,
    /// or <see cref="HybridLogicalClock.Zero"/> if not present.
    /// </summary>
    public HybridLogicalClock GetClock(string replicaId) =>
        Entries.TryGetValue(replicaId, out var clock) ? clock : HybridLogicalClock.Zero;

    /// <summary>
    /// Lattice merge: pointwise-max of all replica entries across both vectors.
    /// Commutative, associative, idempotent.
    /// </summary>
    public static VersionVector Merge(VersionVector left, VersionVector right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = new VersionVector();

        // Seed the result from the left vector via the dictionary copy
        // constructor: it presizes the backing store to left.Entries.Count
        // exactly and bulk-copies, avoiding the 2-3 incremental Resize() grows
        // the previous entry-by-entry fill paid. The right-hand fold below then
        // only grows the dictionary for replica ids unique to the right.
        var merged = new Dictionary<string, HybridLogicalClock>(left.Entries);

        // Single-probe fold: GetValueRefOrAddDefault hashes each id once and
        // returns a ref to the slot (added zero-initialised when absent),
        // replacing the previous TryGetValue-then-indexer pattern that hashed
        // and bucket-walked twice for every replaced or inserted id. The
        // pointwise-max result is identical: a missing slot is Zero (the
        // add-default value), so writing clock is the same as the old insert
        // branch, and an existing slot is bumped only when the incoming clock
        // is strictly greater (ties keep the incumbent, matching the old
        // existing >= clock choice).
        foreach (var (id, clock) in right.Entries)
        {
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(merged, id, out var existed);
            if (!existed || clock > slot) slot = clock;
        }

        result.Entries = merged;
        return result;
    }

    /// <summary>
    /// In-place lattice merge: applies the pointwise-max of <paramref name="other"/>
    /// into this vector without allocating a new instance. Equivalent to
    /// <see cref="Merge(VersionVector, VersionVector)"/> followed by replacing
    /// the receiver, but avoids the intermediate clone.
    /// </summary>
    public void MergeFrom(VersionVector other)
    {
        ArgumentNullException.ThrowIfNull(other);
        foreach (var (id, clock) in other.Entries)
        {
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(Entries, id, out var existed);
            if (!existed || clock > slot) slot = clock;
        }
    }

    /// <summary>
    /// Returns <c>true</c> if every entry in <paramref name="other"/> is ≤ the
    /// corresponding entry in this vector. This means <paramref name="other"/>
    /// contains no information this vector hasn't already seen.
    /// </summary>
    public bool DominatesOrEquals(VersionVector other)
    {
        foreach (var (id, clock) in other.Entries)
        {
            if (GetClock(id) < clock)
                return false;
        }
        return true;
    }

    /// <summary>
    /// Returns <c>true</c> if this vector has at least one entry strictly greater
    /// than the corresponding entry in <paramref name="other"/>.
    /// </summary>
    public bool IsNewerThan(VersionVector other)
    {
        foreach (var (id, clock) in Entries)
        {
            if (clock > other.GetClock(id))
                return true;
        }
        return false;
    }

    /// <summary>Creates a deep copy of this version vector.</summary>
    public VersionVector Clone()
    {
        // The dictionary copy constructor presizes to Entries.Count exactly and
        // bulk-copies the entries, eliminating the incremental Resize() grows
        // the previous entry-by-entry fill paid. HybridLogicalClock is an
        // immutable value type, so a shallow per-entry copy is a deep copy.
        return new VersionVector
        {
            Entries = new Dictionary<string, HybridLogicalClock>(Entries),
        };
    }

    /// <summary>
    /// Folds a <see cref="VersionVectorDelta"/> into this vector. For
    /// every <c>(replicaId, clock)</c> pair in
    /// <see cref="VersionVectorDelta.Entries"/>, the local entry becomes
    /// <c>max(local, clock)</c>. Commutative, associative, and
    /// idempotent against arrival order and duplicate delivery.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. A null
    /// <see cref="VersionVectorDelta.Entries"/> is treated as empty.
    /// </param>
    public void MergeDelta(VersionVectorDelta delta)
    {
        var entries = delta.Entries;
        if (entries is null || entries.Count == 0) return;
        foreach (var (id, clock) in entries)
        {
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(Entries, id, out var existed);
            if (!existed || clock > slot) slot = clock;
        }
    }

    /// <summary>
    /// Removes entries whose <see cref="Orleans.Lattice.HybridLogicalClock.WallClockTicks"/>
    /// is older than <paramref name="minRetainedUtcTicks"/>.
    /// <para>
    /// Pruning prevents unbounded growth of this vector when replicas
    /// enter and leave the cluster over long time horizons. Because a
    /// pruned entry is dropped entirely, the next merge with a replica
    /// whose clock still lives before the cutoff will reinstate it -
    /// pruning must therefore be applied consistently across all
    /// replicas (typically via a per-tree option on continuous-merge
    /// pipelines) to avoid oscillation.
    /// </para>
    /// <para>
    /// Commutativity / associativity are preserved only across replicas
    /// that use the same cutoff; applying a cutoff of <c>0</c> (or never
    /// pruning) is always safe. Returns the number of entries removed.
    /// </para>
    /// </summary>
    public int PruneOlderThan(long minRetainedUtcTicks)
    {
        if (Entries.Count == 0) return 0;

        List<string>? toRemove = null;
        foreach (var (id, clock) in Entries)
        {
            if (clock.WallClockTicks < minRetainedUtcTicks)
                (toRemove ??= []).Add(id);
        }

        if (toRemove is null) return 0;
        foreach (var id in toRemove) Entries.Remove(id);
        return toRemove.Count;
    }
}
