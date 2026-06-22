using Grpc.Core;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

[TestFixture]
public partial class LatticeStateConnectionTests
{
    private static readonly DateTimeOffset Origin = new(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static RpcException Transient() => new(new Status(StatusCode.Unavailable, "unavailable"));

    private static RpcException Permanent() => new(new Status(StatusCode.Unauthenticated, "denied"));

    private static LatticeConnectionSettings Settings(string address = "http://localhost:1") => new()
    {
        Address = address,
        AllowUnencryptedHttp2 = true,
        DegradeAfter = TimeSpan.FromSeconds(5),
        HealthCheckInterval = TimeSpan.FromHours(1),
        TransientRetryBackoff = TimeSpan.FromMilliseconds(1),
        MaxTransientRetries = 2,
    };

    private static (LatticeStateConnection Connection, ControllableTimeProvider Time) NewConnection(
        Func<LatticeConnectionSettings, ILatticeStateClient> factory)
    {
        var time = new ControllableTimeProvider(Origin);
        return (new LatticeStateConnection(factory, time), time);
    }

    [Test]
    public void Status_DefaultsToDisconnected()
    {
        var (connection, _) = NewConnection(_ => new FakeStateClient());

        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Disconnected));
        Assert.That(connection.Status.IsDisconnected, Is.True);
    }

    [Test]
    public void ConfigureAsync_NullSettings_Throws()
    {
        var (connection, _) = NewConnection(_ => new FakeStateClient());

        Assert.That(
            () => connection.ConfigureAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task ConfigureAsync_ReachableEndpoint_TransitionsToConnected()
    {
        var statuses = new List<LatticeConnectionStatus>();
        var (connection, _) = NewConnection(_ => new FakeStateClient());
        connection.StatusChanged += statuses.Add;

        await connection.ConfigureAsync(Settings("http://host:5000"));

        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
        Assert.That(connection.Status.Endpoint, Is.EqualTo("http://host:5000"));
        Assert.That(connection.Status.IsUsable, Is.True);
        Assert.That(statuses.Select(s => s.State), Does.Contain(LatticeConnectionState.Connecting));
        Assert.That(statuses[^1].State, Is.EqualTo(LatticeConnectionState.Connected));
    }

    [Test]
    public async Task ListTreesAsync_WhenNotConfigured_ThrowsStateApiException()
    {
        var (connection, _) = NewConnection(_ => new FakeStateClient());

        Assert.That(
            async () => await connection.ListTreesAsync(new CatalogRequest()),
            Throws.TypeOf<LatticeStateApiException>());
    }

    [Test]
    public async Task ReadCall_Success_ReturnsResult_AndStaysConnected()
    {
        var client = new FakeStateClient
        {
            ScanEntriesHandler = _ => Task.FromResult(new EntryScanResponse { TreeId = "t" }),
        };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());

        var result = await connection.ScanEntriesAsync(new EntryScanRequest { TreeId = "t" });

        Assert.That(result, Is.Not.Null);
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
    }

    [Test]
    public async Task ReadCall_CancelledToken_PropagatesOperationCanceled()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await connection.ListTreesAsync(new CatalogRequest(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task PermanentFailure_FaultsImmediately_AndThrowsNonTransient()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        client.ListTreesHandler = _ => throw Permanent();

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
        Assert.That(captured!.IsTransient, Is.False);
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Faulted));
        Assert.That(connection.Status.IsDisconnected, Is.True);
    }
}
