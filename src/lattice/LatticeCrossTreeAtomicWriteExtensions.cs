using System.Linq.Expressions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Public entrypoints for <b>cross-tree atomic writes</b>: an all-or-nothing
/// batch that spans two or more distinct <see cref="ILattice"/> trees and
/// commits with the same atomic-visibility guarantee
/// <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
/// gives <i>within</i> a single tree: either every targeted key across every
/// participating tree becomes visible, or none of them do, observed atomically
/// by readers on the local cluster and on every cluster the trees replicate to.
/// <para>
/// The batch is driven by an internal coordinator grain keyed by the
/// caller-supplied <c>operationId</c>; each participating tree runs the existing
/// single-tree saga in a prepare-and-pause mode and the coordinator records a
/// single global commit/abort decision that flips visibility on every tree at
/// once. A stable <c>operationId</c> is <b>required</b> (no auto-generated
/// overload) because a cross-tree saga touches multiple registries and a stable
/// idempotency key is mandatory for safe retry.
/// </para>
/// </summary>
public static class LatticeCrossTreeAtomicWriteExtensions
{
    /// <summary>
    /// Atomically commits <paramref name="batches"/> across every named tree,
    /// all-or-nothing. Returns <see cref="CrossTreeAtomicWriteOutcome.Committed"/>
    /// when every tree's optional guard passed and all writes committed, or
    /// <see cref="CrossTreeAtomicWriteOutcome.PreconditionFailed"/> when a guard
    /// failed and nothing was committed in any tree. Throws
    /// <see cref="InvalidOperationException"/> if a write fails (after the saga
    /// compensates), or if the same <paramref name="operationId"/> is re-submitted
    /// with a different tree-set or key-set. Re-submitting the same
    /// <paramref name="operationId"/> with the same tree-set/key-set re-attaches
    /// to the in-flight (or completed) saga and returns its memoized outcome.
    /// </summary>
    /// <param name="factory">The grain factory / cluster client.</param>
    /// <param name="batches">Per-tree slices to commit atomically. Tree ids must be distinct and non-empty.</param>
    /// <param name="operationId">Required cross-tree idempotency key. Must not contain '/'.</param>
    /// <param name="cancellationToken">Cancellation observed before dispatch.</param>
    public static Task<CrossTreeAtomicWriteOutcome> SetManyAtomicAsync(
        this IGrainFactory factory,
        IReadOnlyList<LatticeTreeBatch> batches,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(batches);
        ValidateOperationId(operationId);
        cancellationToken.ThrowIfCancellationRequested();

        var coordinator = factory.GetGrain<ILatticeCrossTreeTxGrain>(operationId);
        return coordinator.CommitAsync([.. batches]);
    }

    /// <summary>
    /// Opens a fluent builder for a cross-tree atomic write keyed by
    /// <paramref name="operationId"/>. Add per-tree slices with
    /// <see cref="LatticeAtomicWriteBuilder.ForTree"/> /
    /// <see cref="LatticeAtomicWriteBuilder.Set(string, byte[])"/> /
    /// <see cref="LatticeAtomicWriteBuilder.Set{T}(string, T)"/> /
    /// <see cref="LatticeAtomicWriteBuilder.SetWhere{T}(string, T, Expression{Func{T, bool}})"/>,
    /// or couple a typed CRDT mutation prepared by a CRDT accessor's <c>Stage*</c>
    /// method via <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/>
    /// (the staged CRDT write rides the same all-or-nothing commit as its sibling
    /// last-writer-wins writes), then
    /// <see cref="LatticeAtomicWriteBuilder.CommitAsync"/>.
    /// </summary>
    /// <param name="factory">The grain factory / cluster client.</param>
    /// <param name="operationId">Required cross-tree idempotency key. Must not contain '/'.</param>
    public static LatticeAtomicWriteBuilder BeginAtomicWrite(
        this IGrainFactory factory,
        string operationId)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ValidateOperationId(operationId);
        return new LatticeAtomicWriteBuilder(factory, operationId);
    }

    internal static void ValidateOperationId(string operationId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        if (operationId.Contains('/'))
        {
            throw new ArgumentException(
                "operationId must not contain '/' (reserved as the grain-key separator).",
                nameof(operationId));
        }
    }
}
