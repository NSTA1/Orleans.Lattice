namespace Orleans.Lattice;

/// <summary>
/// State-based CRDT shape consumed by recursively-mergeable composites
/// such as <see cref="OrMap{TKey, TValue}"/>. A type implementing this
/// interface declares (a) how to merge another instance into itself
/// pointwise and (b) when its current state is the lattice bottom
/// element - i.e. carries no live information, so the composite can
/// drop the slot rather than keep an empty container after a remove.
/// </summary>
/// <typeparam name="TSelf">
/// The implementing type itself. Constrained on every implementer so
/// the composite can call <see cref="MergeFrom(TSelf)"/> without
/// reflection or boxing.
/// </typeparam>
/// <remarks>
/// <para>
/// The contract is intentionally minimal so the primitive types it
/// is mixed into stay free of replication-package or accessor-layer
/// concerns. Every existing primitive (<see cref="OrSet"/>,
/// <see cref="PnCounter"/>, <see cref="VersionVector"/>,
/// <see cref="MvRegister"/>) implements it without behavioural change;
/// the contract is what <see cref="OrMap{TKey, TValue}"/> binds against
/// when it merges a per-key value snapshot through the lattice.
/// </para>
/// <para>
/// <see cref="MergeFrom(TSelf)"/> must be commutative, associative, and
/// idempotent (the standard CRDT join-semilattice axioms). Implementers
/// that already provide an in-place merge typically delegate.
/// <see cref="IsBottom"/> returns <see langword="true"/> when the value
/// carries no live state - an empty <see cref="OrSet"/>, a zero-valued
/// <see cref="PnCounter"/> with no recorded replicas, an empty
/// <see cref="VersionVector"/>, an empty <see cref="MvRegister"/> - so
/// the composite can recognise "absent after remove" without inspecting
/// the type's internal shape.
/// </para>
/// </remarks>
public interface ICrdt<TSelf> where TSelf : ICrdt<TSelf>
{
    /// <summary>
    /// In-place lattice merge: applies <paramref name="other"/> into
    /// this instance. Must be commutative, associative, and idempotent
    /// across calls to the same receiver.
    /// </summary>
    /// <param name="other">The other-side state to merge in.</param>
    void MergeFrom(TSelf other);

    /// <summary>
    /// Returns <see langword="true"/> when this instance carries no
    /// live state. Composites use this signal to drop empty value
    /// slots after a remove rather than retain them, while still
    /// preserving causal-history components (tombstones, dot context)
    /// on the surrounding container.
    /// </summary>
    bool IsBottom { get; }

    /// <summary>
    /// Returns a deep, independent copy of this instance. Mutating the
    /// returned value must never affect the receiver (and vice versa).
    /// Composites rely on this to hand callers a defensively-copied value
    /// - for example the single-live-entry fast path in
    /// <see cref="OrMap{TKey, TValue}.Get(TKey)"/> - without allocating an
    /// identity element and folding it through <see cref="MergeFrom(TSelf)"/>.
    /// </summary>
    /// <returns>A structurally independent copy of this instance.</returns>
    TSelf Clone();
}
