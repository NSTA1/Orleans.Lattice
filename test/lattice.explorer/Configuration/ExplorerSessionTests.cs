using NSubstitute;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Configuration;

[TestFixture]
public class ExplorerSessionTests
{
    private static ExplorerConfiguration ValidConfig(string endpoint = "http://localhost:5199") =>
        new() { Endpoint = endpoint, AllowUnencryptedHttp2 = true };

    [Test]
    public async Task InitializeAsync_WithStoredConfig_ConnectsAndReportsConfigured()
    {
        var store = Substitute.For<IExplorerConfigStore>();
        store.LoadAsync(Arg.Any<CancellationToken>()).Returns(ValidConfig());
        var connection = Substitute.For<ILatticeStateConnection>();
        var session = new ExplorerSession(store, connection);

        var configured = await session.InitializeAsync();

        Assert.That(configured, Is.True);
        Assert.That(session.IsConfigured, Is.True);
        Assert.That(session.Current!.Endpoint, Is.EqualTo("http://localhost:5199"));
        await connection.Received(1).ConfigureAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task InitializeAsync_WithNoConfig_ReportsUnconfigured_AndDoesNotConnect()
    {
        var store = Substitute.For<IExplorerConfigStore>();
        store.LoadAsync(Arg.Any<CancellationToken>()).Returns((ExplorerConfiguration?)null);
        var connection = Substitute.For<ILatticeStateConnection>();
        var session = new ExplorerSession(store, connection);

        var configured = await session.InitializeAsync();

        Assert.That(configured, Is.False);
        Assert.That(session.IsConfigured, Is.False);
        await connection.DidNotReceive().ConfigureAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task InitializeAsync_WithStoredButInvalidEndpoint_ReportsUnconfigured()
    {
        var store = Substitute.For<IExplorerConfigStore>();
        store.LoadAsync(Arg.Any<CancellationToken>()).Returns(new ExplorerConfiguration { Endpoint = "not-a-url" });
        var connection = Substitute.For<ILatticeStateConnection>();
        var session = new ExplorerSession(store, connection);

        var configured = await session.InitializeAsync();

        Assert.That(configured, Is.False);
        await connection.DidNotReceive().ConfigureAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task InitializeAsync_IsIdempotent()
    {
        var store = Substitute.For<IExplorerConfigStore>();
        store.LoadAsync(Arg.Any<CancellationToken>()).Returns(ValidConfig());
        var connection = Substitute.For<ILatticeStateConnection>();
        var session = new ExplorerSession(store, connection);

        await session.InitializeAsync();
        await session.InitializeAsync();

        await store.Received(1).LoadAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_SavesReconfiguresAndRaisesEvent()
    {
        var store = Substitute.For<IExplorerConfigStore>();
        var connection = Substitute.For<ILatticeStateConnection>();
        var session = new ExplorerSession(store, connection);
        var raised = 0;
        session.ConfigurationChanged += () => raised++;

        await session.ApplyAsync(ValidConfig("https://host:443"));

        await store.Received(1).SaveAsync(Arg.Any<ExplorerConfiguration>(), Arg.Any<CancellationToken>());
        await connection.Received(1).ConfigureAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>());
        Assert.That(session.IsConfigured, Is.True);
        Assert.That(session.Current!.Endpoint, Is.EqualTo("https://host:443"));
        Assert.That(raised, Is.EqualTo(1));
    }

    [Test]
    public async Task InitializeAsync_ExposesConfiguration_BeforeConfiguringConnection()
    {
        var store = Substitute.For<IExplorerConfigStore>();
        store.LoadAsync(Arg.Any<CancellationToken>()).Returns(ValidConfig());
        var connection = Substitute.For<ILatticeStateConnection>();
        var session = new ExplorerSession(store, connection);

        bool? configuredWhenConnecting = null;
        ExplorerConfiguration? currentWhenConnecting = null;
        connection
            .When(c => c.ConfigureAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>()))
            .Do(_ =>
            {
                configuredWhenConnecting = session.IsConfigured;
                currentWhenConnecting = session.Current;
            });

        await session.InitializeAsync();

        Assert.That(configuredWhenConnecting, Is.True);
        Assert.That(currentWhenConnecting, Is.Not.Null);
    }

    [Test]
    public async Task ApplyAsync_ExposesConfiguration_BeforeConfiguringConnection()
    {
        var store = Substitute.For<IExplorerConfigStore>();
        var connection = Substitute.For<ILatticeStateConnection>();
        var session = new ExplorerSession(store, connection);

        bool? configuredWhenConnecting = null;
        ExplorerConfiguration? currentWhenConnecting = null;
        connection
            .When(c => c.ConfigureAsync(Arg.Any<LatticeConnectionSettings>(), Arg.Any<CancellationToken>()))
            .Do(_ =>
            {
                configuredWhenConnecting = session.IsConfigured;
                currentWhenConnecting = session.Current;
            });

        await session.ApplyAsync(ValidConfig("https://host:443"));

        Assert.That(configuredWhenConnecting, Is.True);
        Assert.That(currentWhenConnecting!.Endpoint, Is.EqualTo("https://host:443"));
    }
}
