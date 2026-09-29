using Grpc.Core;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// A cancelled call is never evidence about the endpoint (issue #3831): the Explorer
/// cancels its own calls whenever a page, live tail or scan is left, and a call in
/// flight when the channel is rebuilt ends the same way. Neither may move the
/// connection's state, which is what raised a storm of "Disconnected ... (Cancelled)"
/// toasts during ordinary navigation.
/// </summary>
public partial class LatticeStateConnectionTests
{
    private static RpcException CancelledCall() => new(new Status(StatusCode.Cancelled, "cancelled"));

    [Test]
    public async Task A_call_the_caller_cancelled_surfaces_as_cancellation_and_leaves_the_connection_connected()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        var changes = new List<LatticeConnectionStatus>();
        connection.StatusChanged += changes.Add;
        using var cancel = new CancellationTokenSource();
        client.ListTreesHandler = _ =>
        {
            cancel.Cancel();
            throw CancelledCall();
        };

        Assert.That(
            async () => await connection.ListTreesAsync(new CatalogRequest(), cancel.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.Multiple(() =>
        {
            Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
            Assert.That(changes, Is.Empty, "a cancellation reports nothing");
        });
    }

    [Test]
    public async Task A_call_cancelled_under_the_caller_fails_alone_and_leaves_the_connection_connected()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        var changes = new List<LatticeConnectionStatus>();
        connection.StatusChanged += changes.Add;
        client.ListTreesHandler = _ => throw CancelledCall();

        var failure = Assert.ThrowsAsync<LatticeStateApiException>(async () => await connection.ListTreesAsync(new CatalogRequest()));

        Assert.Multiple(() =>
        {
            Assert.That(failure!.IsTransient, Is.True);
            Assert.That(failure.RequiresAuthentication, Is.False);
            Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
            Assert.That(changes, Is.Empty);
        });
    }

    [Test]
    public async Task A_cancelled_probe_leaves_the_connection_as_it_was()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        var changes = new List<LatticeConnectionStatus>();
        connection.StatusChanged += changes.Add;
        client.ListTreesHandler = _ => throw CancelledCall();

        var reachable = await connection.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reachable, Is.False);
            Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
            Assert.That(changes, Is.Empty);
        });
    }

    [Test]
    public async Task A_cancelled_stream_leaves_the_connection_connected()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        client.ObserveMetricsHandler = _ => Fail();

        Assert.That(
            async () =>
            {
                await foreach (var _ in connection.ObserveMetricsAsync(new TreeMetricsRequest { TreeIds = ["orders"] }))
                {
                }
            },
            Throws.InstanceOf<LatticeStateApiException>());
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));

        static async IAsyncEnumerable<TreeMetricsSnapshot> Fail()
        {
            await Task.Yield();
            throw CancelledCall();
#pragma warning disable CS0162 // An iterator needs a yield to be one.
            yield break;
#pragma warning restore CS0162
        }
    }

    [Test]
    public async Task A_genuine_fault_is_still_reported()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        client.ListTreesHandler = _ => throw new RpcException(new Status(StatusCode.Unimplemented, "no state api"));

        Assert.That(async () => await connection.ListTreesAsync(new CatalogRequest()), Throws.InstanceOf<LatticeStateApiException>());
        Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Faulted));
    }
}
