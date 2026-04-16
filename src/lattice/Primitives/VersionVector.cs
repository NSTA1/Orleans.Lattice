using Orleans.Lattice;

namespace Orleans.Lattice.Primitives;

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
public sealed class VersionVector
{
    [Id(0)]
    public Dictionary<string, HybridLogicalClock> Entries { get; set; } = [];

    /// <summary>
    /// Advances the clock for the given <paramref name="replicaId"/> and records
    /// it in this vector. Returns the new clock value.
    /// </summary>
    public HybridLogicalClock Tick(string replicaId)
    {
        var previous = GetClock(replicaId);
        var next = HybridLogicalClock.Tick(previous);
        Entries[replicaId] = next;
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
        var result = new VersionVector();

        foreach (var (id, clock) in left.Entries)
        {
            result.Entries[id] = clock;
        }

        foreach (var (id, clock) in right.Entries)
        {
            if (result.Entries.TryGetValue(id, out var existing))
            {
                result.Entries[id] = existing >= clock ? existing : clock;
            }
            else
            {
                result.Entries[id] = clock;
            }
        }

        return result;
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
        var copy = new VersionVector();
        foreach (var (id, clock) in Entries)
        {
            copy.Entries[id] = clock;
        }
        return copy;
    }
}
