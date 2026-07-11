namespace Orleans.Lattice;

/// <summary>
/// Pluggable post-merge hook consulted after a per-key CRDT / LWW merge
/// completes on the leaf grain. The observer may upcast the decoded merge
/// inputs, validate or normalise the merged result, and choose a
/// <see cref="LatticeMergeOutcome"/>. Modelled on the null-default seam pattern:
/// the core library registers <see cref="NullLatticeMergeObserver"/> (which
/// always returns <see cref="LatticeMergeOutcome.Accept()"/>), so with only
/// <c>AddLattice</c> registered the merge path is byte-for-byte identical to the
/// pre-seam behaviour with no per-merge allocation. A schema/versioning add-on
/// replaces it with a real observer.
/// </summary>
/// <remarks>
/// <para>
/// <b>No hard reject.</b> A lock-free merge has already been applied by the time
/// the observer runs and cannot be rolled back, so the outcome is limited to
/// accept, accept-transformed, or accept-with-event.
/// </para>
/// <para>
/// <b>CRDT non-mutation invariant.</b>
/// <see cref="MergeOutcomeKind.AcceptTransformed"/> is forbidden for any record
/// whose <see cref="LatticeMergeContext.Mode"/> is not
/// <see cref="LatticeMergeMode.LwwRegister"/>: rewriting the canonical merged
/// bytes of a typed CRDT record would break WAL-replay determinism (a cold
/// rebuild folds the durable delta into the prior visible state and must land on
/// identical bytes). The wiring enforces this at runtime by throwing
/// <see cref="System.InvalidOperationException"/> when a non-LWW merge observer
/// returns <see cref="MergeOutcomeKind.AcceptTransformed"/>. Transform remains
/// available for <see cref="LatticeMergeMode.LwwRegister"/> records.
/// </para>
/// <para>
/// Implementations must be safe for concurrent invocation from multiple
/// threads.
/// </para>
/// </remarks>
public interface ILatticeMergeObserver
{
    /// <summary>
    /// Invoked after a per-key merge completes with the decoded inputs, decoded
    /// result, and declared merge mode carried by <paramref name="ctx"/>.
    /// Returns the disposition for the merged value.
    /// </summary>
    /// <param name="ctx">The completed merge's key, mode, decoded inputs and decoded result.</param>
    /// <param name="ct">Cancels the observation.</param>
    /// <returns>
    /// The merge outcome. For a non-<see cref="LatticeMergeMode.LwwRegister"/>
    /// record the wiring rejects a
    /// <see cref="MergeOutcomeKind.AcceptTransformed"/> outcome with
    /// <see cref="System.InvalidOperationException"/>.
    /// </returns>
    ValueTask<LatticeMergeOutcome> OnMergedAsync(in LatticeMergeContext ctx, CancellationToken ct);
}
