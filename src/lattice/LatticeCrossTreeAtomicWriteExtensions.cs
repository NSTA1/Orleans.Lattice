using System.Linq.Expressions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
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
    /// <para>
    /// A saga state write that loses an optimistic-concurrency (ETag) check - for
    /// example because a storage-SDK transport retry landed the first attempt -
    /// makes the affected saga grain deactivate so a fresh activation reloads its
    /// durable state. This method re-attaches by <paramref name="operationId"/> a
    /// bounded number of times when that happens, which is idempotent; if the
    /// conflict persists it throws
    /// <see cref="LatticeStateWriteFailedException"/> with
    /// <see cref="LatticeStateWriteFailedException.Conflict"/> set, and the saga
    /// can be resumed by re-submitting the same batch under the same
    /// <paramref name="operationId"/>. The durable decision is never lost or
    /// applied twice.
    /// </para>
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
        return CommitReattachingOnConflictAsync(coordinator, [.. batches], cancellationToken);
    }

    /// <summary>
    /// Maximum commit attempts when a saga state write conflicts (issue #3572),
    /// including the first.
    /// </summary>
    internal const int MaxConflictAttempts = 3;

    /// <summary>
    /// Invokes <see cref="ILatticeCrossTreeTxGrain.CommitAsync"/>, re-attaching
    /// by operation id after a linear backoff (1 s, 2 s) when a saga grain
    /// reports a state-write conflict. Every other fault propagates unchanged.
    /// The backoff gives the conflicted activation time to deactivate, so the
    /// re-attach lands on a fresh activation that reloads the row.
    /// </summary>
    internal static async Task<CrossTreeAtomicWriteOutcome> CommitReattachingOnConflictAsync(
        ILatticeCrossTreeTxGrain coordinator,
        List<LatticeTreeBatch> batches,
        CancellationToken cancellationToken,
        Func<int, TimeSpan>? backoff = null)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await coordinator.CommitAsync(batches).ConfigureAwait(false);
            }
            catch (Exception ex) when (
                attempt < MaxConflictAttempts && GrainStateWriteFaults.IsTranslatedConflict(ex))
            {
                var delay = backoff?.Invoke(attempt) ?? TimeSpan.FromSeconds(attempt);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
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
