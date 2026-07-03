namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ILatticeAccessGate"/>: authorizes every request.
/// Registered by <c>AddLattice</c> as the safe default so a consumer of the
/// seam always resolves an instance even when no auth add-on is registered,
/// preserving "zero cost when unregistered". <c>Orleans.Lattice.Auth</c>
/// replaces it with a real, policy-evaluating gate.
/// </summary>
/// <remarks>
/// The allow decision and its wrapping <see cref="ValueTask{TResult}"/> are
/// cached in a <c>static readonly</c> field, so every call returns the same
/// synchronously-completed result with no per-call allocation.
/// </remarks>
internal sealed class NullLatticeAccessGate : ILatticeAccessGate
{
    private static readonly ValueTask<LatticeAccessDecision> AllowResult =
        new(LatticeAccessDecision.Allow());

    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default) =>
        AllowResult;
}
