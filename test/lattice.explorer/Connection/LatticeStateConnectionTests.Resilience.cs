using Grpc.Core;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

public partial class LatticeStateConnectionTests
{
    [Test]
    public async Task TransientFailure_RetriesInline_ThenSucceeds()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());

        var attempts = 0;
        client.ListTreesHandler = _ =>
        {
            attempts++;
            return attempts < 2
                ? throw Transient()
                : Task.FromResult(new TreeCatalogPage());
        };

        var page = await connection.ListTreesAsync(new CatalogRequest());

        Assert.That(page, Is.Not.Null);
        Assert.That(attempts, Is.EqualTo(2));
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
    }

    [Test]
    public async Task TransientFailure_ExhaustsRetries_ThrowsTransient_AndEntersReconnecting()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        client.ListTreesHandler = _ => throw Transient();

        LatticeStateApiException? captured = null;
        try
        {
            await connection.ListTreesAsync(new CatalogRequest());
        }
        catch (LatticeStateApiException ex)
        {
            captured = ex;
        }

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.IsTransient, Is.True);
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Reconnecting));
        Assert.That(connection.Status.IsUsable, Is.True);
    }

    [Test]
    public async Task ConfigureAsync_TransientEndpoint_EntersReconnecting_WithinGraceWindow()
    {
        var client = new FakeStateClient { ListTreesHandler = _ => throw Transient() };
        var (connection, _) = NewConnection(_ => client);

        await connection.ConfigureAsync(Settings());

        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Reconnecting));
    }

    [Test]
    public async Task Reconnecting_DegradesToFaulted_AfterGraceWindow()
    {
        var client = new FakeStateClient { ListTreesHandler = _ => throw Transient() };
        var (connection, time) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Reconnecting));

        time.Advance(TimeSpan.FromSeconds(6));
        await connection.CheckHealthAsync();

        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Faulted));
        Assert.That(connection.Status.IsDisconnected, Is.True);
    }

    [Test]
    public async Task HealthMonitor_AutoRecovers_WhenEndpointReturns()
    {
        var fail = true;
        var client = new FakeStateClient
        {
            ListTreesHandler = _ => fail ? throw Transient() : Task.FromResult(new TreeCatalogPage()),
        };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Reconnecting));

        fail = false;
        await connection.CheckHealthAsync();

        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
    }

    [Test]
    public async Task CheckHealthAsync_WhenConnected_DoesNotProbe()
    {
        var probes = 0;
        var client = new FakeStateClient
        {
            ListTreesHandler = _ =>
            {
                probes++;
                return Task.FromResult(new TreeCatalogPage());
            },
        };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        var afterConfigure = probes;

        await connection.CheckHealthAsync();

        Assert.That(probes, Is.EqualTo(afterConfigure));
    }

    [Test]
    public async Task ObserveMetricsAsync_TransientFault_StopsStream_AndEntersReconnecting()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        client.ObserveMetricsHandler = _ => ThrowingMetricStream();

        var received = new List<TreeMetricsSnapshot>();
        await foreach (var snapshot in connection.ObserveMetricsAsync(new TreeMetricsRequest()))
        {
            received.Add(snapshot);
        }

        Assert.That(received, Is.Empty);
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Reconnecting));
    }

    private static async IAsyncEnumerable<TreeMetricsSnapshot> ThrowingMetricStream()
    {
        await Task.CompletedTask;
        if (DateTimeOffset.UtcNow.Year > 0)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, "stream lost"));
        }

        yield break;
    }
}
