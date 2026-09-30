using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// A sign-in is bound to the endpoint it was minted for (issue #4020):
/// <see cref="IExplorerAuthSession.GetAuthenticationFor"/> offers the credential for
/// that endpoint only, and a sign-in whose endpoint moved while its challenge ran is
/// refused rather than bound to the endpoint the console moved to.
/// </summary>
[TestFixture]
public sealed class ExplorerAuthSessionEndpointBindingTests
{
    private const string EndpointA = "https://cluster-a:443";
    private const string EndpointB = "https://cluster-b:443";

    [Test]
    public async Task GetAuthenticationFor_offers_the_sign_in_only_to_the_endpoint_it_was_minted_for()
    {
        var (session, explorerSession, _, _) = Create(EndpointA);
        await session.LoginAsync("alice", "Password1");

        Assert.Multiple(() =>
        {
            Assert.That(session.GetAuthenticationFor(EndpointA), Is.SameAs(session.CurrentAuthentication));
            Assert.That(session.GetAuthenticationFor("HTTPS://CLUSTER-A:443/"), Is.SameAs(session.CurrentAuthentication), "case and a trailing slash are the same endpoint");
            Assert.That(session.GetAuthenticationFor(EndpointB), Is.Null);
            Assert.That(session.GetAuthenticationFor(string.Empty), Is.Null);
        });

        // Repointing the session does not re-bind the sign-in: until it is dropped it is
        // still offered to the endpoint it was minted for, and to no other.
        explorerSession.Current.Returns(Configuration(EndpointB));
        Assert.Multiple(() =>
        {
            Assert.That(session.IsAuthenticated, Is.True, "the premise: the sign-in has not been dropped yet");
            Assert.That(session.GetAuthenticationFor(EndpointB), Is.Null);
            Assert.That(session.GetAuthenticationFor(EndpointA), Is.Not.Null);
        });
    }

    [Test]
    public void GetAuthenticationFor_is_null_when_signed_out()
    {
        var (session, _, _, _) = Create(EndpointA);

        Assert.That(session.GetAuthenticationFor(EndpointA), Is.Null);
    }

    [Test]
    public void The_default_implementation_fails_closed()
    {
        IExplorerAuthSession session = new FakeAuthSession();

        Assert.That(session.GetAuthenticationFor(EndpointA), Is.Null);
    }

    [Test]
    public async Task A_sign_in_whose_endpoint_moved_during_its_challenge_is_refused_and_not_persisted()
    {
        var method = new GatedAuthMethod();
        var (session, explorerSession, applied, store) = Create(EndpointA, method);

        var login = session.LoginWithMethodAsync(GatedAuthMethod.Scheme);
        var context = await method.Entered.WaitAsync(TimeSpan.FromSeconds(30));
        explorerSession.Current.Returns(Configuration(EndpointB));
        method.Release.SetResult();

        Assert.That(async () => await login, Throws.InvalidOperationException.With.Message.EqualTo(ExplorerAuthSession.EndpointChangedDuringSignInMessage));
        var persisted = await store.GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(context.Endpoint, Is.EqualTo(EndpointA), "the premise: the challenge was taken for the old endpoint");
            Assert.That(session.IsAuthenticated, Is.False);
            Assert.That(session.GetAuthenticationFor(EndpointB), Is.Null);
            Assert.That(applied.Where(settings => settings.Authentication is not null), Is.Empty, "the credential reached a connection");
            Assert.That(persisted, Is.Null);
            Assert.That(method.Provider.Disposed, Is.True, "the refused sign-in's token provider was not released");
        });
    }

    [Test]
    public async Task A_sign_in_on_an_unchanged_endpoint_is_bound_to_it()
    {
        var method = new GatedAuthMethod();
        var (session, _, applied, _) = Create(EndpointA, method);

        var login = session.LoginWithMethodAsync(GatedAuthMethod.Scheme);
        await method.Entered.WaitAsync(TimeSpan.FromSeconds(30));
        method.Release.SetResult();
        await login;

        Assert.Multiple(() =>
        {
            Assert.That(session.GetAuthenticationFor(EndpointA), Is.Not.Null);
            Assert.That(applied[^1].Authentication, Is.Not.Null);
            Assert.That(method.Provider.Disposed, Is.False);
        });
    }

    private static ExplorerConfiguration Configuration(string endpoint) => new() { Endpoint = endpoint };

    private static (ExplorerAuthSession Session, IExplorerSession ExplorerSession, List<LatticeConnectionSettings> Applied, InMemoryCredentialStore Store)
        Create(string endpoint, IExplorerAuthMethod? method = null)
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        var applied = new List<LatticeConnectionSettings>();
        connection
            .ConfigureAsync(Arg.Do<LatticeConnectionSettings>(applied.Add), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var explorerSession = Substitute.For<IExplorerSession>();
        explorerSession.Connection.Returns(connection);
        explorerSession.Current.Returns(Configuration(endpoint));

        var store = new InMemoryCredentialStore();
        var session = new ExplorerAuthSession(explorerSession, store, methods: method is null ? null : [method]);
        return (session, explorerSession, applied, store);
    }

    /// <summary>A token scheme whose challenge waits until the test releases it.</summary>
    private sealed class GatedAuthMethod : IExplorerAuthMethod
    {
        public const string Scheme = "gated";

        private readonly TaskCompletionSource<ExplorerAuthChallengeContext> _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string SchemeId => Scheme;

        public Task<ExplorerAuthChallengeContext> Entered => _entered.Task;

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public DisposableProvider Provider { get; } = new();

        public bool CanHandle(string advertisedScheme) => advertisedScheme == Scheme;

        public async Task<ExplorerAuthSignIn> ChallengeAsync(ExplorerAuthChallengeContext context, CancellationToken cancellationToken = default)
        {
            _entered.SetResult(context);
            await Release.Task;
            return new ExplorerAuthSignIn
            {
                SchemeId = Scheme,
                DisplayName = "alice",
                Authentication = LatticeCallAuthentication.Bearer(Provider),
            };
        }
    }

    /// <summary>A token provider that records its disposal.</summary>
    private sealed class DisposableProvider : ILatticeCallCredentialProvider, IDisposable
    {
        public bool Disposed { get; private set; }

        public ValueTask<string?> GetAuthorizationHeaderAsync(CancellationToken cancellationToken = default) =>
            new("Bearer token");

        public ValueTask<bool> RefreshAsync(CancellationToken cancellationToken = default) => new(true);

        public void Dispose() => Disposed = true;
    }
}
