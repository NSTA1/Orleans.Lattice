using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Configuration;

[TestFixture]
public class EnvironmentExplorerBootstrapTests
{
    private sealed class FakeEnvironment : IExplorerEnvironment
    {
        private readonly Dictionary<string, string> _values = new(StringComparer.Ordinal);

        public FakeEnvironment Set(string name, string value)
        {
            _values[name] = value;
            return this;
        }

        public string? GetVariable(string name) =>
            _values.TryGetValue(name, out var value) ? value : null;
    }

    private static IExplorerConfigurationSeed ConfigSeed(FakeEnvironment env) =>
        new EnvironmentExplorerBootstrap(env);

    private static IExplorerCredentialSeed CredentialSeed(FakeEnvironment env) =>
        new EnvironmentExplorerBootstrap(env);

    [Test]
    public void Constructor_nullEnvironment_throws()
    {
        Assert.That(() => new EnvironmentExplorerBootstrap(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void TrySeed_config_withNoEndpoint_returnsNull()
    {
        var seed = ConfigSeed(new FakeEnvironment());

        Assert.That(seed.TrySeed(), Is.Null);
    }

    [Test]
    public void TrySeed_config_withEndpointOnly_seedsSecureEndpoint()
    {
        var env = new FakeEnvironment().Set(EnvironmentExplorerBootstrap.EndpointVariable, "https://cluster:443");
        var seed = ConfigSeed(env);

        var config = seed.TrySeed();

        Assert.That(config, Is.Not.Null);
        Assert.That(config!.Endpoint, Is.EqualTo("https://cluster:443"));
        Assert.That(config.TransportMode, Is.EqualTo(ExplorerTransportMode.Secure));
        Assert.That(config.AllowUnencryptedHttp2, Is.False);
    }

    [Test]
    public void TrySeed_config_withInsecureDevTruthy_seedsLoopbackDevMode()
    {
        var env = new FakeEnvironment()
            .Set(EnvironmentExplorerBootstrap.EndpointVariable, "http://localhost:5001")
            .Set(EnvironmentExplorerBootstrap.InsecureDevVariable, "true");
        var seed = ConfigSeed(env);

        var config = seed.TrySeed();

        Assert.That(config, Is.Not.Null);
        Assert.That(config!.TransportMode, Is.EqualTo(ExplorerTransportMode.InsecureLoopbackDev));
        Assert.That(config.AllowUnencryptedHttp2, Is.True);
    }

    [Test]
    [TestCase("1")]
    [TestCase("TRUE")]
    [TestCase("Yes")]
    [TestCase("on")]
    public void TrySeed_config_acceptsTruthyVariants(string value)
    {
        var env = new FakeEnvironment()
            .Set(EnvironmentExplorerBootstrap.EndpointVariable, "http://localhost:5001")
            .Set(EnvironmentExplorerBootstrap.InsecureDevVariable, value);

        var config = ConfigSeed(env).TrySeed();

        Assert.That(config!.TransportMode, Is.EqualTo(ExplorerTransportMode.InsecureLoopbackDev));
    }

    [Test]
    [TestCase("0")]
    [TestCase("false")]
    [TestCase("")]
    [TestCase("nonsense")]
    public void TrySeed_config_treatsNonTruthyAsSecure(string value)
    {
        var env = new FakeEnvironment()
            .Set(EnvironmentExplorerBootstrap.EndpointVariable, "https://cluster:443")
            .Set(EnvironmentExplorerBootstrap.InsecureDevVariable, value);

        var config = ConfigSeed(env).TrySeed();

        Assert.That(config!.TransportMode, Is.EqualTo(ExplorerTransportMode.Secure));
    }

    [Test]
    public void TrySeed_config_neverSeedsACredential()
    {
        var env = new FakeEnvironment()
            .Set(EnvironmentExplorerBootstrap.EndpointVariable, "http://localhost:5001")
            .Set(EnvironmentExplorerBootstrap.UsernameVariable, "alice")
            .Set(EnvironmentExplorerBootstrap.PasswordVariable, "Password1");

        var config = ConfigSeed(env).TrySeed();

        Assert.That(config!.Headers, Is.Null);
        Assert.That(config.ToConnectionSettings().Authentication, Is.Null);
    }

    [Test]
    public void TrySeed_config_trimsEndpointWhitespace()
    {
        var env = new FakeEnvironment().Set(EnvironmentExplorerBootstrap.EndpointVariable, "  http://localhost:5001  ");

        var config = ConfigSeed(env).TrySeed();

        Assert.That(config!.Endpoint, Is.EqualTo("http://localhost:5001"));
    }

    [Test]
    public void TrySeed_credential_withBothSet_seedsCredential()
    {
        var env = new FakeEnvironment()
            .Set(EnvironmentExplorerBootstrap.UsernameVariable, "alice")
            .Set(EnvironmentExplorerBootstrap.PasswordVariable, "Password1");

        var credential = CredentialSeed(env).TrySeed();

        Assert.That(credential, Is.EqualTo(new StoredCredential("alice", "Password1")));
    }

    [Test]
    public void TrySeed_credential_withMissingUsername_returnsNull()
    {
        var env = new FakeEnvironment().Set(EnvironmentExplorerBootstrap.PasswordVariable, "Password1");

        Assert.That(CredentialSeed(env).TrySeed(), Is.Null);
    }

    [Test]
    public void TrySeed_credential_withMissingPassword_returnsNull()
    {
        var env = new FakeEnvironment().Set(EnvironmentExplorerBootstrap.UsernameVariable, "alice");

        Assert.That(CredentialSeed(env).TrySeed(), Is.Null);
    }

    [Test]
    public async Task AddExplorerEnvironmentBootstrap_registersBothSeedsOverOneInstance()
    {
        var services = new ServiceCollection();
        services.AddExplorerEnvironmentBootstrap();
        await using var provider = services.BuildServiceProvider();

        var configSeed = provider.GetRequiredService<IExplorerConfigurationSeed>();
        var credentialSeed = provider.GetRequiredService<IExplorerCredentialSeed>();

        Assert.That(configSeed, Is.InstanceOf<EnvironmentExplorerBootstrap>());
        Assert.That(credentialSeed, Is.SameAs(configSeed));
        Assert.That(provider.GetRequiredService<IExplorerEnvironment>(), Is.InstanceOf<ProcessExplorerEnvironment>());
    }

    [Test]
    public async Task ExplorerSession_withEmptyStoreAndSeed_connectsToSeededEndpoint_withoutPersisting()
    {
        var store = Substitute.For<IExplorerConfigStore>();
        store.LoadAsync(Arg.Any<CancellationToken>()).Returns((ExplorerConfiguration?)null);
        var connection = Substitute.For<ILatticeStateConnection>();
        var seed = ConfigSeed(new FakeEnvironment()
            .Set(EnvironmentExplorerBootstrap.EndpointVariable, "http://localhost:5001")
            .Set(EnvironmentExplorerBootstrap.InsecureDevVariable, "true"));
        var session = new ExplorerSession(store, connection, seed);

        var configured = await session.InitializeAsync();

        Assert.That(configured, Is.True);
        Assert.That(session.IsConfigured, Is.True);
        Assert.That(session.Current!.Endpoint, Is.EqualTo("http://localhost:5001"));
        await connection.Received(1).ConfigureAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>());
        await store.DidNotReceive().SaveAsync(Arg.Any<ExplorerConfiguration>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExplorerSession_withStoredConfig_ignoresSeed()
    {
        var store = Substitute.For<IExplorerConfigStore>();
        store.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(new ExplorerConfiguration { Endpoint = "https://stored:443" });
        var connection = Substitute.For<ILatticeStateConnection>();
        var seed = ConfigSeed(new FakeEnvironment()
            .Set(EnvironmentExplorerBootstrap.EndpointVariable, "http://localhost:5001"));
        var session = new ExplorerSession(store, connection, seed);

        await session.InitializeAsync();

        Assert.That(session.Current!.Endpoint, Is.EqualTo("https://stored:443"));
    }

    [Test]
    public async Task ExplorerSession_withEmptyStoreAndNoSeed_reportsUnconfigured()
    {
        var store = Substitute.For<IExplorerConfigStore>();
        store.LoadAsync(Arg.Any<CancellationToken>()).Returns((ExplorerConfiguration?)null);
        var connection = Substitute.For<ILatticeStateConnection>();
        var seed = ConfigSeed(new FakeEnvironment());
        var session = new ExplorerSession(store, connection, seed);

        var configured = await session.InitializeAsync();

        Assert.That(configured, Is.False);
        await connection.DidNotReceive().ConfigureAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExplorerAuthSession_withEmptyStoreAndSeed_authenticatesInMemory_withoutPersisting()
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        var applied = new List<LatticeConnectionSettings>();
        connection
            .ConfigureAsync(Arg.Do<LatticeConnectionSettings>(applied.Add), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        var explorerSession = Substitute.For<IExplorerSession>();
        explorerSession.Connection.Returns(connection);
        explorerSession.Current.Returns(new ExplorerConfiguration
        {
            Endpoint = "http://localhost:5001",
            TransportMode = ExplorerTransportMode.InsecureLoopbackDev,
            AllowUnencryptedHttp2 = true,
        });
        var store = Substitute.For<ICredentialStore>();
        store.GetAsync(Arg.Any<CancellationToken>()).Returns((StoredCredential?)null);
        var seed = CredentialSeed(new FakeEnvironment()
            .Set(EnvironmentExplorerBootstrap.UsernameVariable, "alice")
            .Set(EnvironmentExplorerBootstrap.PasswordVariable, "Password1"));
        var session = new ExplorerAuthSession(explorerSession, store, seed);

        await session.InitializeAsync();

        Assert.That(session.IsAuthenticated, Is.True);
        Assert.That(session.Username, Is.EqualTo("alice"));
        Assert.That(applied, Has.Count.EqualTo(1));
        Assert.That(applied[0].Authentication, Is.Not.Null);
        await store.DidNotReceive().SetAsync(Arg.Any<StoredCredential>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExplorerAuthSession_withStoredCredential_ignoresSeed()
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        var explorerSession = Substitute.For<IExplorerSession>();
        explorerSession.Connection.Returns(connection);
        explorerSession.Current.Returns(new ExplorerConfiguration { Endpoint = "https://cluster:443" });
        var store = Substitute.For<ICredentialStore>();
        store.GetAsync(Arg.Any<CancellationToken>()).Returns(new StoredCredential("stored", "Stored1"));
        var seed = CredentialSeed(new FakeEnvironment()
            .Set(EnvironmentExplorerBootstrap.UsernameVariable, "alice")
            .Set(EnvironmentExplorerBootstrap.PasswordVariable, "Password1"));
        var session = new ExplorerAuthSession(explorerSession, store, seed);

        await session.InitializeAsync();

        Assert.That(session.Username, Is.EqualTo("stored"));
    }

    [Test]
    public async Task ExplorerAuthSession_withNoSeedAndEmptyStore_staysAnonymous()
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        var explorerSession = Substitute.For<IExplorerSession>();
        explorerSession.Connection.Returns(connection);
        explorerSession.Current.Returns(new ExplorerConfiguration { Endpoint = "http://localhost:5001" });
        var store = Substitute.For<ICredentialStore>();
        store.GetAsync(Arg.Any<CancellationToken>()).Returns((StoredCredential?)null);
        var session = new ExplorerAuthSession(explorerSession, store, CredentialSeed(new FakeEnvironment()));

        await session.InitializeAsync();

        Assert.That(session.IsAuthenticated, Is.False);
    }
}
