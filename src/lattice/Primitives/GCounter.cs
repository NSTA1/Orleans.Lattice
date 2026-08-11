namespace Orleans.Lattice;

using System.Runtime.InteropServices;

/// <summary>
/// A grow-only (G) counter CRDT. Each replica tracks its own monotonically
/// increasing component; the counter's value is the sum of every replica's
/// component. This is the monotonic-only counter that <see cref="PnCounter"/>
/// is built from (a <see cref="PnCounter"/> is two <see cref="GCounter"/>s),
/// and the correct primitive when a value only ever increments - monotone
/// metrics, event / sequence counters, and quota consumption where decrement
/// is impossible. State-level <see cref="Merge(GCounter, GCounter)"/> is
/// pointwise-max per replica, making the CRDT commutative, associative, and
/// idempotent under arbitrary delivery order.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.GCounter)]
public sealed class GCounter : ICrdt<GCounter>
{
    /// <summary>Per-replica cumulative grow-only component.</summary>
    [Id(0)]
    public Dictionary<string, long> Increments { get; set; } = [];

    /// <inheritdoc />
    /// <remarks>
    /// A <see cref="GCounter"/> is bottom when no replica has recorded any
    /// increment. Because the counter is grow-only every recorded component
    /// is strictly positive, so a bottom counter is exactly one whose value
    /// is zero.
    /// </remarks>
    public bool IsBottom => Increments.Count == 0;

    /// <summary>The counter's current value: the sum of every replica's <see cref="Increments"/> component.</summary>
    public long Value
    {
        get
        {
            long total = 0;
            foreach (var v in Increments.Values) total += v;
            return total;
        }
    }

    /// <summary>
    /// Advances the grow-only component for <paramref name="replicaId"/> by
    /// <paramref name="amount"/>. <paramref name="amount"/> must be non-negative -
    /// a grow-only counter never decreases.
    /// </summary>
    public void Increment(string replicaId, long amount = 1)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentOutOfRangeException.ThrowIfNegative(amount);
        if (amount == 0) return;
        ref var inc = ref CollectionsMarshal.GetValueRefOrAddDefault(Increments, replicaId, out _);
        inc += amount;
    }

    /// <summary>
    /// Lattice merge: pointwise-max per replica on <see cref="Increments"/>.
    /// Commutative, associative, idempotent.
    /// </summary>
    public static GCounter Merge(GCounter left, GCounter right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <summary>
    /// In-place lattice merge: applies the pointwise-max of
    /// <paramref name="other"/>'s components into this counter without
    /// allocating a new instance. Equivalent to
    /// <see cref="Merge(GCounter, GCounter)"/> followed by replacing the
    /// receiver, but avoids the intermediate clone.
    /// </summary>
    public void MergeFrom(GCounter other)
    {
        ArgumentNullException.ThrowIfNull(other);
        MergeSide(Increments, other.Increments);
    }

    /// <summary>Creates a deep copy of this counter.</summary>
    public GCounter Clone() =>
        // The dictionary copy constructor presizes the backing store to the
        // source Count exactly and bulk-copies the entries. string keys and
        // long values are immutable, so the shallow per-entry copy is a deep
        // copy.
        new()
        {
            Increments = new Dictionary<string, long>(Increments),
        };

    /// <summary>
    /// Folds a <see cref="GCounterDelta"/> into this counter: every per-replica
    /// entry in <see cref="GCounterDelta.Increments"/> is pointwise-max'd into
    /// the matching local component. Commutative, associative, and idempotent
    /// against arrival order and duplicate delivery.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. A null inner
    /// dictionary is treated as empty.
    /// </param>
    public void MergeDelta(GCounterDelta delta)
    {
        var inc = delta.Increments;
        if (inc is { Count: > 0 }) MergeSide(Increments, inc);
    }

    private static void MergeSide(Dictionary<string, long> target, Dictionary<string, long> source)
    {
        foreach (var (k, v) in source)
        {
            // Single-probe fold: GetValueRefOrAddDefault hashes k once and
            // returns a ref to the slot (added zero-initialised when absent).
            // The pointwise-max result is: a missing slot is 0 (the add-default
            // value), and an existing slot is bumped only when the incoming
            // value is strictly greater. Mirrors the single-probe fold
            // PnCounter and VersionVector use.
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(target, k, out var existed);
            if (!existed || v > slot) slot = v;
        }
    }
}
