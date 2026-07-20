using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

public partial class LatticeStateConnectionTests
{
    [Test]
    public async Task ConfigureAsync_EndpointChange_DisposesPreviousClient()
    {
        var created = new List<FakeStateClient>();
        var (connection, _) = NewConnection(_ =>
        {
            var client = new FakeStateClient();
            created.Add(client);
            return client;
        });

        await connection.ConfigureAsync(Settings("http://host:1"));
        await connection.ConfigureAsync(Settings("http://host:2"));

        Assert.That(created, Has.Count.EqualTo(2));
        Assert.That(created[0].DisposeCount, Is.EqualTo(1));
        Assert.That(created[1].DisposeCount, Is.EqualTo(0));
        Assert.That(connection.Status.Endpoint, Is.EqualTo("http://host:2"));
    }

    [Test]
    public async Task ReconnectAsync_WhenNeverConfigured_ReturnsFalse()
    {
        var (connection, _) = NewConnection(_ => new FakeStateClient());

        var reconnected = await connection.ReconnectAsync();

        Assert.That(reconnected, Is.False);
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Disconnected));
    }

    [Test]
    public async Task ReconnectAsync_RebuildsChannel_AndProbes()
    {
        var fail = true;
        var created = new List<FakeStateClient>();
        var (connection, _) = NewConnection(_ =>
        {
            var client = new FakeStateClient
            {
                ListTreesHandler = _ => fail ? throw Transient() : Task.FromResult(new TreeCatalogPage()),
            };
            created.Add(client);
            return client;
        });

        await connection.ConfigureAsync(Settings());
        Assert.That(connection.Status.IsDisconnected || connection.Status.State == LatticeConnectionState.Reconnecting, Is.True);

        fail = false;
        var reconnected = await connection.ReconnectAsync();

        Assert.That(reconnected, Is.True);
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
        Assert.That(created, Has.Count.EqualTo(2));
        Assert.That(created[0].DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task DisposeAsync_DisposesClient_AndIgnoresFurtherConfigure()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());

        await connection.DisposeAsync();

        Assert.That(client.DisposeCount, Is.EqualTo(1));
        await connection.ConfigureAsync(Settings("http://host:9"));
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected).Or.EqualTo(LatticeConnectionState.Disconnected));
    }

    [Test]
    public async Task AddLatticeStateConnection_RegistersScopedPerCircuit()
    {
        var services = new ServiceCollection();
        services.AddLatticeStateConnection();
        await using var provider = services.BuildServiceProvider();

        await using var scopeA = provider.CreateAsyncScope();
        await using var scopeB = provider.CreateAsyncScope();
        var first = scopeA.ServiceProvider.GetRequiredService<ILatticeStateConnection>();
        var second = scopeB.ServiceProvider.GetRequiredService<ILatticeStateConnection>();

        Assert.Multiple(() =>
        {
            Assert.That(scopeA.ServiceProvider.GetRequiredService<ILatticeStateConnection>(), Is.SameAs(first));
            Assert.That(second, Is.Not.SameAs(first));
            Assert.That(first, Is.InstanceOf<LatticeStateConnection>());
        });
    }
}
