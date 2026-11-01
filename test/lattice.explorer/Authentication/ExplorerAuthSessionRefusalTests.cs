using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// A sign-in the cluster refuses (issue #5415): the credential is checked against the
/// endpoint before anything is applied, and a refusal leaves the session signed out -
/// nothing persisted, the connection not reconfigured, no sign-in announced - while a
/// caller the cluster recognises, granted or not, signs in as before.
/// </summary>
[TestFixture]
public sealed class ExplorerAuthSessionRefusalTests
{
    private const string Endpoint = "https://cluster.internal:443";

    [Test]
    public async Task LoginAsync_with_a_password_the_cluster_refuses_establishes_no_session()
    {
        var (session, connection, store) = CreateSession(LatticeCredentialCheck.Refused);
        var raised = false;
        session.AuthenticationChanged += () => raised = true;

        var ex = Assert.ThrowsAsync<ExplorerSignInRefusedException>(() => session.LoginAsync("olivia", "wrong"));
        var stored = await store.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Is.EqualTo(ExplorerSignInRefusedException.PasswordRefusedMessage));
            Assert.That(ex.Message, Does.Not.Contain("wrong"), "the password is never echoed");
            Assert.That(session.IsAuthenticated, Is.False);
            Assert.That(session.Username, Is.Null);
            Assert.That(stored, Is.Null, "a refused password is never persisted");
            Assert.That(raised, Is.False);
        });
        await connection.DidNotReceiveWithAnyArgs().ConfigureAsync(default!, default);
    }

    [Test]
    public async Task LoginAsync_checks_the_credential_it_is_signing_in_with_against_the_configured_endpoint()
    {
        var (session, connection, _) = CreateSession(LatticeCredentialCheck.Accepted);

        await session.LoginAsync("olivia", "right");

        await connection.Received(1).VerifyCredentialAsync(
            Arg.Is<LatticeConnectionSettings>(s => s.Authentication != null && s.Address.StartsWith("https://cluster.internal", StringComparison.Ordinal)),
            Arg.Any<CancellationToken>());
        Assert.That(session.IsAuthenticated, Is.True);
    }

    [TestCase(LatticeCredentialCheck.Accepted)]
    [TestCase(LatticeCredentialCheck.Unverified)]
    public async Task LoginAsync_a_credential_the_cluster_does_not_refuse_signs_in(LatticeCredentialCheck check)
    {
        var (session, _, store) = CreateSession(check);

        await session.LoginAsync("dave", "right");
        var stored = await store.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(session.IsAuthenticated, Is.True, "a caller without grants still signs in and meets its no-grant states");
            Assert.That(stored, Is.EqualTo(new StoredCredential("dave", "right")));
        });
    }

    [Test]
    public void LoginWithMethodAsync_a_refused_token_sign_in_names_no_password()
    {
        var method = Substitute.For<IExplorerAuthMethod>();
        method.SchemeId.Returns("custom");
        method.CanHandle("custom").Returns(true);
        method.ChallengeAsync(Arg.Any<ExplorerAuthChallengeContext>(), Arg.Any<CancellationToken>())
            .Returns(new ExplorerAuthSignIn
            {
                SchemeId = "custom",
                DisplayName = "olivia",
                Authentication = LatticeCallAuthentication.Basic("olivia", "token"),
            });
        var (session, _, _) = CreateSession(LatticeCredentialCheck.Refused, method);

        var ex = Assert.ThrowsAsync<ExplorerSignInRefusedException>(() => session.LoginWithMethodAsync("custom"));

        Assert.That(ex!.Message, Is.EqualTo(ExplorerSignInRefusedException.SignInRefusedMessage));
    }

    [Test]
    public async Task LoginAsync_in_a_scope_whose_session_was_never_loaded_loads_it_and_checks_the_credential()
    {
        // The web head's /auth/login runs in a fresh request scope: the explorer
        // session there has not loaded its configuration, and the check must not be
        // skipped for want of an endpoint.
        var connection = Substitute.For<ILatticeStateConnection>();
        connection.VerifyCredentialAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(LatticeCredentialCheck.Refused));
        var explorerSession = Substitute.For<IExplorerSession>();
        explorerSession.Connection.Returns(connection);
        ExplorerConfiguration? current = null;
        explorerSession.Current.Returns(_ => current);
        explorerSession.InitializeAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            current = new ExplorerConfiguration { Endpoint = Endpoint };
            return Task.FromResult(true);
        });
        var store = new InMemoryCredentialStore();
        var session = new ExplorerAuthSession(explorerSession, store);

        Assert.ThrowsAsync<ExplorerSignInRefusedException>(() => session.LoginAsync("olivia", "wrong"));

        await explorerSession.Received(1).InitializeAsync(Arg.Any<CancellationToken>());
        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task LoginAsync_with_no_endpoint_configured_is_not_checked()
    {
        var (session, connection, _) = CreateSession(LatticeCredentialCheck.Refused, configured: false);

        await session.LoginAsync("olivia", "any");

        await connection.DidNotReceiveWithAnyArgs().VerifyCredentialAsync(default!, default);
    }

    [Test]
    public void ExplorerSignInRefusedException_constructors_carry_their_message()
    {
        var inner = new InvalidOperationException("inner");

        Assert.Multiple(() =>
        {
            Assert.That(new ExplorerSignInRefusedException().Message, Is.EqualTo(ExplorerSignInRefusedException.SignInRefusedMessage));
            Assert.That(new ExplorerSignInRefusedException("m").Message, Is.EqualTo("m"));
            Assert.That(new ExplorerSignInRefusedException("m", inner).InnerException, Is.SameAs(inner));
        });
    }

    private static (ExplorerAuthSession Session, ILatticeStateConnection Connection, InMemoryCredentialStore Store) CreateSession(
        LatticeCredentialCheck check,
        IExplorerAuthMethod? method = null,
        bool configured = true)
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        connection.ConfigureAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        connection.VerifyCredentialAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(check));

        var explorerSession = Substitute.For<IExplorerSession>();
        explorerSession.Connection.Returns(connection);
        explorerSession.Current.Returns(configured ? new ExplorerConfiguration { Endpoint = Endpoint } : null);

        var store = new InMemoryCredentialStore();
        var session = new ExplorerAuthSession(explorerSession, store, methods: method is null ? null : [method]);
        return (session, connection, store);
    }
}
