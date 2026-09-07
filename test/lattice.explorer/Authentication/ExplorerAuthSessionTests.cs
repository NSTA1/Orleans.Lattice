using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

[TestFixture]
public class ExplorerAuthSessionTests
{
    private static (ExplorerAuthSession session, ILatticeStateConnection connection, List<LatticeConnectionSettings> applied, InMemoryCredentialStore store)
        CreateSession(ExplorerConfiguration? current = null)
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        var applied = new List<LatticeConnectionSettings>();
        connection
            .ConfigureAsync(Arg.Do<LatticeConnectionSettings>(applied.Add), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var explorerSession = Substitute.For<IExplorerSession>();
        explorerSession.Connection.Returns(connection);
        explorerSession.Current.Returns(current ?? new ExplorerConfiguration
        {
            Endpoint = "https://cluster.internal:443",
            AllowUnencryptedHttp2 = false,
        });

        var store = new InMemoryCredentialStore();
        var session = new ExplorerAuthSession(explorerSession, store);
        return (session, connection, applied, store);
    }

    [Test]
    public async Task LoginAsync_setsAuthenticatedStateAndUsername()
    {
        var (session, _, _, _) = CreateSession();

        await session.LoginAsync("alice", "Password1");

        Assert.That(session.IsAuthenticated, Is.True);
        Assert.That(session.Username, Is.EqualTo("alice"));
    }

    [Test]
    public async Task LoginAsync_reconfiguresConnectionWithBasicHeader()
    {
        var (session, _, applied, _) = CreateSession();

        await session.LoginAsync("alice", "Password1");

        Assert.That(applied, Has.Count.EqualTo(1));
        var auth = applied[0].Authentication;
        Assert.That(auth, Is.Not.Null);
        var expected = "Basic " + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("alice:Password1"));
        Assert.That(auth!.Headers!["authorization"], Is.EqualTo(expected));
    }

    [Test]
    public async Task LoginAsync_persistsCredentialToStore()
    {
        var (session, _, _, store) = CreateSession();

        await session.LoginAsync("alice", "Password1");

        Assert.That(await store.GetAsync(), Is.EqualTo(new StoredCredential("alice", "Password1")));
    }

    [Test]
    public async Task LoginAsync_raisesAuthenticationChanged()
    {
        var (session, _, _, _) = CreateSession();
        var raised = false;
        session.AuthenticationChanged += () => raised = true;

        await session.LoginAsync("alice", "Password1");

        Assert.That(raised, Is.True);
    }

    [Test]
    public async Task LogoutAsync_clearsStateAndStoreAndReconfiguresAnonymously()
    {
        var (session, _, applied, store) = CreateSession();
        await session.LoginAsync("alice", "Password1");
        applied.Clear();

        await session.LogoutAsync();

        Assert.That(session.IsAuthenticated, Is.False);
        Assert.That(session.Username, Is.Null);
        Assert.That(await store.GetAsync(), Is.Null);
        Assert.That(applied, Has.Count.EqualTo(1));
        Assert.That(applied[0].Authentication, Is.Null);
    }

    [Test]
    public async Task InitializeAsync_withStoredCredential_appliesHeader()
    {
        var (session, _, applied, store) = CreateSession();
        await store.SetAsync(new StoredCredential("alice", "Password1"));

        await session.InitializeAsync();

        Assert.That(session.IsAuthenticated, Is.True);
        Assert.That(applied, Has.Count.EqualTo(1));
        Assert.That(applied[0].Authentication, Is.Not.Null);
    }

    [Test]
    public async Task InitializeAsync_withNoStoredCredential_doesNotReconfigure()
    {
        var (session, _, applied, _) = CreateSession();

        await session.InitializeAsync();

        Assert.That(session.IsAuthenticated, Is.False);
        Assert.That(applied, Is.Empty);
    }

    [Test]
    public async Task InitializeAsync_isIdempotent()
    {
        var (session, _, applied, store) = CreateSession();
        await store.SetAsync(new StoredCredential("alice", "Password1"));

        await session.InitializeAsync();
        await session.InitializeAsync();

        Assert.That(applied, Has.Count.EqualTo(1));
    }

    [Test]
    public void LoginAsync_emptyUsername_throws()
    {
        var (session, _, _, _) = CreateSession();

        Assert.That(async () => await session.LoginAsync("  ", "Password1"), Throws.ArgumentException);
    }

    [Test]
    public void LoginAsync_nullPassword_throws()
    {
        var (session, _, _, _) = CreateSession();

        Assert.That(async () => await session.LoginAsync("alice", null!), Throws.ArgumentNullException);
    }

    private static (ExplorerAuthSession session, IExplorerSession explorerSession, ConfigureRecorder recorder, InMemoryCredentialStore store)
        CreateSessionWithSubstitute(ExplorerConfiguration current)
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        var recorder = new ConfigureRecorder();
        connection
            .ConfigureAsync(Arg.Do<LatticeConnectionSettings>(recorder.Record), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var explorerSession = Substitute.For<IExplorerSession>();
        explorerSession.Connection.Returns(connection);
        explorerSession.Current.Returns(current);

        var store = new InMemoryCredentialStore();
        var session = new ExplorerAuthSession(explorerSession, store);
        return (session, explorerSession, recorder, store);
    }

    /// <summary>
    /// Records every <see cref="ILatticeStateConnection.ConfigureAsync"/> call and
    /// hands out a task that completes on the next one, so a test can await the
    /// fire-and-forget reconfiguration a configuration change triggers instead of
    /// sleeping on it.
    /// </summary>
    private sealed class ConfigureRecorder
    {
        private readonly object _sync = new();
        private readonly List<LatticeConnectionSettings> _applied = [];
        private TaskCompletionSource _next = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IReadOnlyList<LatticeConnectionSettings> Applied
        {
            get { lock (_sync) { return _applied.ToArray(); } }
        }

        public Task NextApplied
        {
            get { lock (_sync) { return _next.Task; } }
        }

        public void Record(LatticeConnectionSettings settings)
        {
            TaskCompletionSource completed;
            lock (_sync)
            {
                _applied.Add(settings);
                completed = _next;
                _next = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            completed.TrySetResult();
        }
    }

    private static ExplorerConfiguration ConfigurationFor(string endpoint)
        => new() { Endpoint = endpoint, AllowUnencryptedHttp2 = false };

    [Test]
    public async Task ConfigurationChange_toADifferentEndpoint_doesNotPresentTheSignInToTheNewEndpoint()
    {
        // Credential-replay regression: a sign-in is minted for one endpoint and
        // must not be re-attached to another. Endpoint B would otherwise receive
        // endpoint A's Basic password, or a silently-renewed bearer token for A's
        // audience, with no operator action beyond repointing the console.
        var (session, explorerSession, recorder, _) = CreateSessionWithSubstitute(ConfigurationFor("https://cluster-a:443"));
        await session.LoginAsync("alice", "Password1");

        var next = recorder.NextApplied;
        explorerSession.Current.Returns(ConfigurationFor("https://cluster-b:443"));
        explorerSession.ConfigurationChanged += Raise.Event<Action>();
        await next.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Applied[^1].Address, Is.EqualTo("https://cluster-b:443"));
            Assert.That(recorder.Applied[^1].Authentication, Is.Null);
            Assert.That(session.IsAuthenticated, Is.False);
        });
    }

    [Test]
    public async Task ConfigurationChange_toADifferentEndpoint_clearsThePersistedCredential()
    {
        // The persisted Basic credential belongs to the endpoint it was entered
        // for; leaving it behind would let InitializeAsync replay it to the new
        // endpoint on the next launch.
        var (session, explorerSession, recorder, store) = CreateSessionWithSubstitute(ConfigurationFor("https://cluster-a:443"));
        await session.LoginAsync("alice", "Password1");
        Assert.That(await store.GetAsync(), Is.Not.Null);

        var next = recorder.NextApplied;
        explorerSession.Current.Returns(ConfigurationFor("https://cluster-b:443"));
        explorerSession.ConfigurationChanged += Raise.Event<Action>();
        await next.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task ConfigurationChange_onTheSameEndpoint_keepsTheSignInApplied()
    {
        // A non-endpoint configuration change (transport posture, headers) must
        // still re-apply the credential - that is what the re-apply exists for.
        var (session, explorerSession, recorder, _) = CreateSessionWithSubstitute(ConfigurationFor("https://cluster-a:443"));
        await session.LoginAsync("alice", "Password1");

        var next = recorder.NextApplied;
        explorerSession.Current.Returns(new ExplorerConfiguration
        {
            Endpoint = "https://cluster-a:443",
            AllowUnencryptedHttp2 = true,
        });
        explorerSession.ConfigurationChanged += Raise.Event<Action>();
        await next.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Applied[^1].Authentication, Is.Not.Null);
            Assert.That(session.IsAuthenticated, Is.True);
        });
    }

    [Test]
    public void Constructor_nullSession_throws()
    {
        Assert.That(() => new ExplorerAuthSession(null!, new InMemoryCredentialStore()), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_nullStore_throws()
    {
        Assert.That(() => new ExplorerAuthSession(Substitute.For<IExplorerSession>(), null!), Throws.ArgumentNullException);
    }
}
