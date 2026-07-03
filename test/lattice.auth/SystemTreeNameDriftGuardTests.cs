using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Drift guard for the reserved auth tree ids mirrored into
/// <see cref="LatticeSystemTreeNames"/>. The replication package cannot reference
/// <c>Orleans.Lattice.Auth</c> (it sits below it), so it re-declares the auth
/// policy tree id as its own public constant; this test - which can see both the
/// canonical internal <see cref="AuthConstants"/> and the mirrored public
/// constant - fails the moment the two diverge, so a rename can never silently
/// stop the policy tree from replicating (which would let a revoked user stay
/// authorized on a peer cluster).
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class SystemTreeNameDriftGuardTests
{
    [Test]
    public void Auth_policy_tree_mirror_matches_the_canonical_constant()
    {
        Assert.That(LatticeSystemTreeNames.AuthPolicy, Is.EqualTo(AuthConstants.PolicyTree));
    }

    [Test]
    public void Auth_audit_tree_mirror_stays_within_the_reserved_auth_namespace()
    {
        // The optional audit tree has no dogfooded backing constant today (auth
        // derives its history as a view over the policy tree), so the invariant we
        // can assert is that the mirrored id stays inside the reserved auth
        // namespace and therefore can never collide with an application tree.
        Assert.That(
            LatticeSystemTreeNames.AuthAudit,
            Does.StartWith(AuthConstants.ReservedTreePrefix));
    }
}
