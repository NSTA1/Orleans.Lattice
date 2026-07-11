namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ILatticeMergeObserver"/>: accepts every merged
/// value verbatim. Registered by <c>AddLattice</c> as the safe default so the
/// leaf-grain post-merge wiring always resolves an instance even when no schema
/// add-on is registered, preserving "zero cost when unregistered".
/// </summary>
/// <remarks>
/// The accept outcome and its wrapping <see cref="ValueTask{TResult}"/> are
/// cached in a <c>static readonly</c> field, so every call returns the same
/// synchronously-completed result with no per-call allocation. The wiring
/// caches an inactive flag per activation when it resolves this type, so the
/// null-default merge path never even constructs a
/// <see cref="LatticeMergeContext"/>.
/// </remarks>
internal sealed class NullLatticeMergeObserver : ILatticeMergeObserver
{
    private static readonly ValueTask<LatticeMergeOutcome> AcceptResult =
        new(LatticeMergeOutcome.Accept());

    /// <inheritdoc />
    public ValueTask<LatticeMergeOutcome> OnMergedAsync(in LatticeMergeContext ctx, CancellationToken ct) =>
        AcceptResult;
}
