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
