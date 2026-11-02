using Grpc.Core;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// Issue #5428: a status that answers one call - a tree no longer visible, an
/// expired scan cursor, a change-feed position past retention - is surfaced to
/// that call and leaves the connection, and the banner's pill, as it was. The
/// statuses that describe the endpoint still fault it.
/// </summary>
public partial class LatticeStateConnectionTests
{
    [TestCase(StatusCode.NotFound)]
    [TestCase(StatusCode.InvalidArgument)]
    [TestCase(StatusCode.FailedPrecondition)]
    [TestCase(StatusCode.AlreadyExists)]
    [TestCase(StatusCode.OutOfRange)]
    public async Task ACallScopedStatus_onAUnaryRead_leavesTheConnectionConnected(StatusCode status)
    {
        var client = new FakeStateClient { ScanEntriesHandler = _ => throw new RpcException(new Status(status, "t/acme/orders")) };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        var changes = new List<LatticeConnectionStatus>();
        connection.StatusChanged += changes.Add;

        var ex = Assert.ThrowsAsync<LatticeStateApiException>(
            () => connection.ScanEntriesAsync(new EntryScanRequest { TreeId = "orders", ContinuationToken = "expired" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.InnerException, Is.TypeOf<RpcException>().With.Property(nameof(RpcException.StatusCode)).EqualTo(status),
                "the caller still reads the call's own outcome");
            Assert.That(ex.IsTransient, Is.False);
            Assert.That(ex.RequiresAuthentication, Is.False);
            Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
            Assert.That(changes, Is.Empty, "the pill does not move");
        });
    }

    [TestCase(StatusCode.NotFound)]
    [TestCase(StatusCode.FailedPrecondition)]
    [TestCase(StatusCode.InvalidArgument)]
    public async Task ACallScopedStatus_onAStream_leavesTheConnectionConnected(StatusCode status)
    {
        var client = new FakeStateClient { ObserveMetricsHandler = _ => FailingMetricStream(status) };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        var changes = new List<LatticeConnectionStatus>();
        connection.StatusChanged += changes.Add;

        var ex = Assert.ThrowsAsync<LatticeStateApiException>(async () =>
        {
            await foreach (var _ in connection.ObserveMetricsAsync(new TreeMetricsRequest()))
            {
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(ex!.InnerException, Is.TypeOf<RpcException>().With.Property(nameof(RpcException.StatusCode)).EqualTo(status));
            Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
            Assert.That(changes, Is.Empty, "a refused feed is neither resubscribed nor reported as the endpoint failing");
        });
    }

    [Test]
    public async Task AnEndpointWithoutTheStateApi_stillFaultsTheConnection()
    {
        // The contrast: Unimplemented says the endpoint itself does not serve the API.
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        client.ScanEntriesHandler = _ => throw new RpcException(new Status(StatusCode.Unimplemented, "no state api"));

        Assert.ThrowsAsync<LatticeStateApiException>(
            () => connection.ScanEntriesAsync(new EntryScanRequest { TreeId = "orders" }));

        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Faulted));
    }

    private static async IAsyncEnumerable<TreeMetricsSnapshot> FailingMetricStream(StatusCode status)
    {
        await Task.CompletedTask;
        if (DateTimeOffset.UtcNow.Year > 0)
        {
            throw new RpcException(new Status(status, "refused"));
        }

        yield break;
    }
}
