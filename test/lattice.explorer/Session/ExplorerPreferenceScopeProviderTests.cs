using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// The identity preferences are remembered against, and its reaction to a
/// sign-in or a cluster change mid-session.
/// </summary>
[TestFixture]
public sealed class ExplorerPreferenceScopeProviderTests
{
    [Test]
    public void Current_WithNoSources_IsTheSignedOutUnconfiguredIdentity()
    {
        using var provider = new ExplorerPreferenceScopeProvider(auth: null, session: null);

        Assert.That(provider.Current, Is.EqualTo(ExplorerPreferenceScopeIdentity.Empty));
    }

    [Test]
    public void Current_ReadsTheUsernameAndEndpoint()
    {
        var auth = Substitute.For<IExplorerAuthSession>();
        auth.Username.Returns("alice");
        var session = Substitute.For<IExplorerSession>();
        session.Current.Returns(new ExplorerConfiguration { Endpoint = "https://cluster-a" });

        using var provider = new ExplorerPreferenceScopeProvider(auth, session);

        Assert.That(
            provider.Current,
            Is.EqualTo(new ExplorerPreferenceScopeIdentity("alice", "https://cluster-a")));
    }

    [Test]
    public void Current_SignedOut_UsesTheAnonymousStandIn()
    {
        var auth = Substitute.For<IExplorerAuthSession>();
        auth.Username.Returns((string?)null);

        using var provider = new ExplorerPreferenceScopeProvider(auth, session: null);

        Assert.That(provider.Current.User, Is.EqualTo(ExplorerPreferenceScopeIdentity.Anonymous));
    }

    [Test]
    public void Current_EmptyEndpoint_UsesTheUnconfiguredStandIn()
    {
        var session = Substitute.For<IExplorerSession>();
        session.Current.Returns(new ExplorerConfiguration { Endpoint = string.Empty });

        using var provider = new ExplorerPreferenceScopeProvider(auth: null, session);

        Assert.That(provider.Current.Cluster, Is.EqualTo(ExplorerPreferenceScopeIdentity.Unconfigured));
    }

    [Test]
    public void AuthenticationChanged_ToADifferentUser_MovesTheScopeAndAnnounces()
    {
        var auth = Substitute.For<IExplorerAuthSession>();
        auth.Username.Returns((string?)null);
        using var provider = new ExplorerPreferenceScopeProvider(auth, session: null);
        var changes = 0;
        provider.ScopeChanged += () => changes++;

        auth.Username.Returns("alice");
        auth.AuthenticationChanged += Raise.Event<Action>();

        Assert.Multiple(() =>
        {
            Assert.That(provider.Current.User, Is.EqualTo("alice"));
            Assert.That(changes, Is.EqualTo(1));
        });
    }

    [Test]
    public void AuthenticationChanged_ToTheSameUser_DoesNotAnnounce()
    {
        var auth = Substitute.For<IExplorerAuthSession>();
        auth.Username.Returns("alice");
        using var provider = new ExplorerPreferenceScopeProvider(auth, session: null);
        var changes = 0;
        provider.ScopeChanged += () => changes++;

        auth.AuthenticationChanged += Raise.Event<Action>();

        Assert.That(changes, Is.Zero);
    }

    [Test]
    public void ConfigurationChanged_ToADifferentEndpoint_MovesTheScope()
    {
        var session = Substitute.For<IExplorerSession>();
        session.Current.Returns(new ExplorerConfiguration { Endpoint = "https://cluster-a" });
        using var provider = new ExplorerPreferenceScopeProvider(auth: null, session);

        session.Current.Returns(new ExplorerConfiguration { Endpoint = "https://cluster-b" });
        session.ConfigurationChanged += Raise.Event<Action>();

        Assert.That(provider.Current.Cluster, Is.EqualTo("https://cluster-b"));
    }

    [Test]
    public void Dispose_StopsListening()
    {
        var auth = Substitute.For<IExplorerAuthSession>();
        auth.Username.Returns("alice");
        var provider = new ExplorerPreferenceScopeProvider(auth, session: null);
        var changes = 0;
        provider.ScopeChanged += () => changes++;

        provider.Dispose();
        auth.Username.Returns("bob");
        auth.AuthenticationChanged += Raise.Event<Action>();

        Assert.That(changes, Is.Zero);
    }

    [Test]
    public void Dispose_Twice_IsHarmless()
    {
        var provider = new ExplorerPreferenceScopeProvider(auth: null, session: null);

        provider.Dispose();

        Assert.That(provider.Dispose, Throws.Nothing);
    }
}
