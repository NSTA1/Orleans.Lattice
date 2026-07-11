namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ILatticeWriteInterceptor"/>: accepts every write
/// unchanged. Registered by <c>AddLattice</c> as the safe default so a consumer
/// of the seam always resolves an instance even when no companion package is
/// registered, preserving "zero cost when unregistered". A schema-enforcement or
/// similar add-on replaces it with a real, value-evaluating interceptor.
/// </summary>
/// <remarks>
/// The accept decision and its wrapping <see cref="ValueTask{TResult}"/> are
/// cached in a <c>static readonly</c> field, so every call returns the same
/// synchronously-completed result with no per-call allocation. The choke point
/// additionally detects this type by reference and short-circuits before ever
/// constructing a <see cref="LatticeWriteRequest"/>, so the default write path
/// is byte-for-byte identical to a build without the seam.
/// </remarks>
internal sealed class NullLatticeWriteInterceptor : ILatticeWriteInterceptor
{
    private static readonly ValueTask<LatticeWriteDecision> AcceptResult =
        new(LatticeWriteDecision.Accept());

    /// <inheritdoc />
    public bool InterceptsSystemOrigin => false;

    /// <inheritdoc />
    public ValueTask<LatticeWriteDecision> OnWriteAsync(
        in LatticeWriteRequest request,
        CancellationToken cancellationToken = default) =>
        AcceptResult;
}
