using Orleans.Lattice;

namespace Orleans.Lattice.Api.Replication.Tests;

/// <summary>
/// A non-null <see cref="ILatticeAccessGate"/> that authorizes every request.
/// Unlike the core no-op gate it is not short-circuited by the enforcement
/// primitive, so it exercises the real subject-resolution and gate-consult path
/// while still allowing.
/// </summary>
internal sealed class AllowingAccessGate : ILatticeAccessGate
{
    /// <summary>Every tree id the gate was asked about, in call order.</summary>
    public List<string> AuthorizedTrees { get; } = [];

    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default)
    {
        AuthorizedTrees.Add(request.TreeId);
        return new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow());
    }
}

/// <summary>
/// An <see cref="ILatticeAccessGate"/> that denies every request with a fixed
/// reason. Models a caller with no replication grant (fail-closed).
/// </summary>
internal sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
{
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default) =>
        new(LatticeAccessDecision.Deny(reason));
}

/// <summary>
/// An <see cref="ILatticeAccessGate"/> that denies anonymous callers and allows
/// every named subject. Models the default-deny-anonymous posture.
/// </summary>
internal sealed class AnonymousDenyingAccessGate : ILatticeAccessGate
{
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default) =>
        request.Subject.IsAnonymous
            ? new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Deny("anonymous"))
            : new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow());
}

/// <summary>
/// An <see cref="ILatticeAccessGate"/> that allows a request only when it targets
/// one of the named trees, denying all others. Used to exercise permission-scoped
/// config discovery.
/// </summary>
internal sealed class TreeScopedAccessGate(params string[] allowedTrees) : ILatticeAccessGate
{
    private readonly HashSet<string> _allowed = new(allowedTrees, StringComparer.Ordinal);

    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default) =>
        _allowed.Contains(request.TreeId)
            ? new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow())
            : new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Deny("not in scope"));
}
