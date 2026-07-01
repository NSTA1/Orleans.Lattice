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
    public async Task ObserveMetricsAsync_TransientFault_Resubscribes_ThenRecovers()
    {
        var attempt = 0;
        var client = new FakeStateClient
        {
            ObserveMetricsHandler = _ => ++attempt == 1 ? ThrowingMetricStream() : OneMetricStream(),
        };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());

        var received = new List<TreeMetricsSnapshot>();
        await foreach (var snapshot in connection.ObserveMetricsAsync(new TreeMetricsRequest()))
        {
            received.Add(snapshot);
        }

        Assert.That(attempt, Is.EqualTo(2));
        Assert.That(received, Has.Count.EqualTo(1));
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
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

    private static async IAsyncEnumerable<TreeMetricsSnapshot> OneMetricStream()
    {
        await Task.CompletedTask;
        yield return new TreeMetricsSnapshot();
    }

    [Test]
    public async Task BusyScan_ResourceExhausted_SurfacesFriendlyError_WithoutRetryOrFault()
    {
        // A saturated tree sheds a snapshot-cursor open (issue #1053), which the
        // state API maps to gRPC ResourceExhausted. The Explorer must NOT auto-
        // retry it (retrying re-issues the expensive open and amplifies the
        // storm the shed exists to stop), must NOT fault the whole connection
        // (other trees stay browsable), and must surface a non-expert message
        // rather than the raw gRPC status code.
        var attempts = 0;
        var client = new FakeStateClient
        {
            ScanEntriesHandler = _ =>
            {
                attempts++;
                throw new RpcException(new Status(StatusCode.ResourceExhausted, "tree 't' saturated"));
            },
        };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());

        LatticeStateApiException? captured = null;
        try
        {
            await connection.ScanEntriesAsync(new EntryScanRequest { TreeId = "t" });
        }
        catch (LatticeStateApiException ex)
        {
            captured = ex;
        }

        Assert.That(captured, Is.Not.Null);
        Assert.That(attempts, Is.EqualTo(1),
            "a saturation shed must not be auto-retried - retrying amplifies the storm the shed exists to stop");
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected),
            "a per-tree busy shed must not fault the whole connection");
        Assert.That(captured!.IsTransient, Is.True,
            "the error is user-retryable once the tree drains");
        Assert.That(captured.Message, Does.Contain("busy"),
            "the message must be a non-expert explanation");
        Assert.That(captured.Message, Does.Not.Contain("ResourceExhausted"),
            "the raw gRPC status code must not leak to the user");
    }
}
