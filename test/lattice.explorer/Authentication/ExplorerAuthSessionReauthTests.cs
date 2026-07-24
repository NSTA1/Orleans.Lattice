using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// Tests that <see cref="ExplorerAuthSession"/> surfaces a token provider's
/// revoked transition through its own <see cref="IExplorerAuthSession.ReauthRequired"/>
/// event, so a UI head can trap it and drive a graceful re-authentication.
/// </summary>
[TestFixture]
public class ExplorerAuthSessionReauthTests
{
    private const string TokenScheme = "reauth";

    private static (ExplorerAuthSession session, FakeReauthAuthMethod method) CreateSession()
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        connection
            .ConfigureAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var explorerSession = Substitute.For<IExplorerSession>();
        explorerSession.Connection.Returns(connection);
        explorerSession.Current.Returns(new ExplorerConfiguration
        {
            Endpoint = "https://cluster.internal:443",
            AllowUnencryptedHttp2 = false,
        });

        var method = new FakeReauthAuthMethod(TokenScheme);
        var session = new ExplorerAuthSession(
            explorerSession,
            new InMemoryCredentialStore(),
            seed: null,
            methods: new IExplorerAuthMethod[] { method });
        return (session, method);
    }

    [Test]
    public async Task ProviderRevoked_afterTokenSignIn_raisesSessionReauthRequired()
    {
        var (session, method) = CreateSession();
        await session.LoginWithMethodAsync(TokenScheme);

        var raised = 0;
        session.ReauthRequired += () => raised++;

        method.LastProvider!.TriggerReauth();

        Assert.That(raised, Is.EqualTo(1));
    }

    [Test]
    public async Task ProviderRevoked_afterLogout_doesNotRaise()
    {
        var (session, method) = CreateSession();
        await session.LoginWithMethodAsync(TokenScheme);
        var provider = method.LastProvider!;
        await session.LogoutAsync();

        var raised = false;
        session.ReauthRequired += () => raised = true;

        provider.TriggerReauth();

        Assert.That(raised, Is.False, "logging out detaches the re-authentication hook");
    }

    [Test]
    public async Task NewSignIn_detachesPreviousProviderHook()
    {
        var (session, method) = CreateSession();
        await session.LoginWithMethodAsync(TokenScheme);
        var first = method.LastProvider!;

        await session.LoginWithMethodAsync(TokenScheme);

        var raised = false;
        session.ReauthRequired += () => raised = true;

        // The superseded provider must no longer drive the session's event.
        first.TriggerReauth();

        Assert.That(raised, Is.False, "a fresh sign-in re-arms the hook onto the new provider only");
    }

    [Test]
    public async Task BasicSignIn_hasNoReauthSource_soTriggerIsInert()
    {
        var (session, _) = CreateSession();

        var raised = false;
        session.ReauthRequired += () => raised = true;

        // A Basic sign-in's static credential provider is not an
        // IReauthRequiredSource, so nothing hooks and nothing fires.
        await session.LoginAsync("alice", "Password1");

        Assert.That(raised, Is.False);
    }
}
