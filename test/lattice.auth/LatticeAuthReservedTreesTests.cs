using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the public reserved-namespace guard
/// <see cref="LatticeAuthReservedTrees"/>: application tree ids must not shadow
/// the reserved <c>sys-auth-*</c> namespace that backs the policy store.
/// </summary>
[TestFixture]
public class LatticeAuthReservedTreesTests
{
    [Test]
    public void PolicyTreeId_is_within_the_reserved_prefix()
    {
        Assert.That(LatticeAuthReservedTrees.PolicyTreeId, Does.StartWith(LatticeAuthReservedTrees.Prefix));
        Assert.That(LatticeAuthReservedTrees.IsReserved(LatticeAuthReservedTrees.PolicyTreeId), Is.True);
    }

    [Test]
    public void IsReserved_is_true_for_a_sys_auth_prefixed_tree()
    {
        Assert.That(LatticeAuthReservedTrees.IsReserved("sys-auth-policy"), Is.True);
        Assert.That(LatticeAuthReservedTrees.IsReserved("sys-auth-anything"), Is.True);
    }

    [Test]
    public void IsReserved_is_false_for_an_ordinary_tree()
    {
        Assert.That(LatticeAuthReservedTrees.IsReserved("orders"), Is.False);
        Assert.That(LatticeAuthReservedTrees.IsReserved("sys-membership-users"), Is.False);
        // Guard is case-sensitive and prefix-anchored, matching the core reserved-prefix check.
        Assert.That(LatticeAuthReservedTrees.IsReserved("SYS-AUTH-policy"), Is.False);
        Assert.That(LatticeAuthReservedTrees.IsReserved("my-sys-auth-policy"), Is.False);
    }

    [Test]
    public void IsReserved_with_null_throws()
    {
        Assert.That(() => LatticeAuthReservedTrees.IsReserved(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ThrowIfReserved_throws_for_a_reserved_tree()
    {
        Assert.That(() => LatticeAuthReservedTrees.ThrowIfReserved("sys-auth-policy"), Throws.ArgumentException);
    }

    [Test]
    public void ThrowIfReserved_throws_for_null_or_empty()
    {
        Assert.That(() => LatticeAuthReservedTrees.ThrowIfReserved(null!), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => LatticeAuthReservedTrees.ThrowIfReserved(""), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ThrowIfReserved_returns_for_an_ordinary_tree()
    {
        Assert.That(() => LatticeAuthReservedTrees.ThrowIfReserved("orders"), Throws.Nothing);
    }
}
