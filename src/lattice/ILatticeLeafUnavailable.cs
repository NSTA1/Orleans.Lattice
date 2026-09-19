namespace Orleans.Lattice;

/// <summary>
/// Marks an exception that means <b>this leaf cannot be activated right now, so
/// any operation that enumerates it cannot make progress</b>.
/// <para>
/// <b>Why a marker interface and not a base class.</b> Two core exceptions carry
/// this condition and they deliberately sit on different bases.
/// <see cref="LeafProjectionStaleException"/> derives from
/// <see cref="InvalidOperationException"/> and registers its own no-op deep
/// copier; <c>LeafSnapshotUnaffordableException</c> derives <i>directly</i> from
/// <see cref="Exception"/> for an explicitly documented reason - the generated
/// same-silo deep copier resolves a base-type copier, which Orleans registers
/// for <see cref="Exception"/> but not for its BCL subclasses, so deriving
/// directly is what lets it cross a co-located grain boundary without one. A
/// shared base class would have to re-parent at least one of them and would
/// silently take that property away. An interface adds the common predicate
/// while leaving both hierarchies, both copier arrangements, and the
/// <c>SerializableExceptionDeepCopyContractTests</c> audit exactly as they are.
/// </para>
/// <para>
/// <b>Why it is public when one implementor is not.</b> Callers above the core -
/// the repository-context store's index reset in particular - must be able to
/// recognise "the leaf is unavailable, fall back to a primitive that does not
/// enumerate" without naming the concrete types. One of those types is
/// <see langword="internal"/>, so before this interface existed no package
/// outside the core could catch the condition at all: the only options were to
/// catch <see cref="Exception"/> broadly, which also swallows deliberate
/// refusals and genuine defects, or to leave the condition unhandled and let a
/// recoverable fault abort the very verb that exists to recover from it. This
/// interface is the narrow, typed seam that removes both. The concrete type may
/// stay internal; the condition is what callers need, and only the condition is
/// exposed.
/// </para>
/// <para>
/// <b>What implementing it commits to.</b> That the failure is a property of the
/// leaf's availability and not of the caller's request, so retrying the same
/// request unchanged will fail the same way until something about the leaf
/// changes, while a non-enumerating primitive (for example
/// <see cref="ILattice.DeleteTreeAsync"/>, which works from shard-root state
/// alone) can still make progress. Do not implement it on a fault that a plain
/// retry would clear, and do not implement it to mean "something went wrong with
/// a leaf" - a handler that sees this is entitled to escalate to a whole-tree
/// operation, and that is far too blunt a response to a transient error.
/// </para>
/// </summary>
public interface ILatticeLeafUnavailable
{
}
