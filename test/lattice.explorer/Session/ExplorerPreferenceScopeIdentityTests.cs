using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// The scope token that keeps one operator's remembered view out of another's.
/// </summary>
[TestFixture]
public sealed class ExplorerPreferenceScopeIdentityTests
{
    [Test]
    public void Empty_IsSignedOutAndUnconfigured()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerPreferenceScopeIdentity.Empty.User,
                Is.EqualTo(ExplorerPreferenceScopeIdentity.Anonymous));
            Assert.That(
                ExplorerPreferenceScopeIdentity.Empty.Cluster,
                Is.EqualTo(ExplorerPreferenceScopeIdentity.Unconfigured));
        });
    }

    [Test]
    public void ToScopeToken_IsStableForTheSameIdentity()
    {
        var left = new ExplorerPreferenceScopeIdentity("alice", "https://a");
        var right = new ExplorerPreferenceScopeIdentity("alice", "https://a");

        Assert.That(
            left.ToScopeToken(ExplorerPreferenceScope.UserAndCluster),
            Is.EqualTo(right.ToScopeToken(ExplorerPreferenceScope.UserAndCluster)));
    }

    [Test]
    public void ToScopeToken_DiffersByUser()
    {
        var alice = new ExplorerPreferenceScopeIdentity("alice", "https://a");
        var bob = new ExplorerPreferenceScopeIdentity("bob", "https://a");

        Assert.That(
            alice.ToScopeToken(ExplorerPreferenceScope.UserAndCluster),
            Is.Not.EqualTo(bob.ToScopeToken(ExplorerPreferenceScope.UserAndCluster)));
    }

    [Test]
    public void ToScopeToken_DiffersByCluster()
    {
        var a = new ExplorerPreferenceScopeIdentity("alice", "https://a");
        var b = new ExplorerPreferenceScopeIdentity("alice", "https://b");

        Assert.That(
            a.ToScopeToken(ExplorerPreferenceScope.UserAndCluster),
            Is.Not.EqualTo(b.ToScopeToken(ExplorerPreferenceScope.UserAndCluster)));
    }

    [Test]
    public void ToScopeToken_UserScope_IgnoresTheCluster()
    {
        var a = new ExplorerPreferenceScopeIdentity("alice", "https://a");
        var b = new ExplorerPreferenceScopeIdentity("alice", "https://b");

        Assert.That(
            a.ToScopeToken(ExplorerPreferenceScope.User),
            Is.EqualTo(b.ToScopeToken(ExplorerPreferenceScope.User)));
    }

    [Test]
    public void ToScopeToken_UserScope_StillDiffersByUser()
    {
        var alice = new ExplorerPreferenceScopeIdentity("alice", "https://a");
        var bob = new ExplorerPreferenceScopeIdentity("bob", "https://a");

        Assert.That(
            alice.ToScopeToken(ExplorerPreferenceScope.User),
            Is.Not.EqualTo(bob.ToScopeToken(ExplorerPreferenceScope.User)));
    }

    [Test]
    public void ToScopeToken_IsFixedWidthAndCanonical()
    {
        var identity = new ExplorerPreferenceScopeIdentity("a very long user name indeed", "https://a/b/c?d=e");

        var user = identity.ToScopeToken(ExplorerPreferenceScope.User);
        var both = identity.ToScopeToken(ExplorerPreferenceScope.UserAndCluster);

        Assert.Multiple(() =>
        {
            Assert.That(user, Has.Length.EqualTo(17));
            Assert.That(both, Has.Length.EqualTo(34));

            // The token becomes part of a stored key, so it must obey the same
            // canonical-spelling rule as everything else in the contract.
            Assert.That(
                Orleans.Lattice.Explorer.Core.Navigation.ExplorerRouteSlug.IsCanonical(user),
                Is.True);
            Assert.That(
                Orleans.Lattice.Explorer.Core.Navigation.ExplorerRouteSlug.IsCanonical(both),
                Is.True);
        });
    }

    [Test]
    public void ToScopeToken_DoesNotEmbedTheRawIdentity()
    {
        var identity = new ExplorerPreferenceScopeIdentity("alice", "https://cluster.example");

        Assert.That(
            identity.ToScopeToken(ExplorerPreferenceScope.UserAndCluster),
            Does.Not.Contain("alice").And.Not.Contain("cluster"));
    }

    [Test]
    public void ToScopeToken_EmptyParts_StillProducesAToken()
    {
        var identity = new ExplorerPreferenceScopeIdentity(string.Empty, string.Empty);

        Assert.That(
            identity.ToScopeToken(ExplorerPreferenceScope.UserAndCluster),
            Has.Length.EqualTo(34));
    }

    [Test]
    public void Equality_IsByValue()
    {
        Assert.That(
            new ExplorerPreferenceScopeIdentity("alice", "https://a"),
            Is.EqualTo(new ExplorerPreferenceScopeIdentity("alice", "https://a")));
    }
}
