using Grpc.Core;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// Issue #5419: a grant narrowed to part of a tree refuses that tree's whole-tree
/// metrics sample with <c>PermissionDenied</c>. That refusal is a fact about the
/// call, so it must reach the caller as a denial and leave the connection - and
/// the banner's pill - exactly as it was.
/// </summary>
public partial class LatticeStateConnectionTests
{
    [Test]
    public async Task APerCallDenial_onAUnaryRead_leavesTheConnectionConnected()
    {
        var client = new FakeStateClient { MetricsHandler = _ => throw Denied() };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        var changes = new List<LatticeConnectionStatus>();
        connection.StatusChanged += changes.Add;

        var ex = Assert.ThrowsAsync<LatticeStateApiException>(
            () => connection.GetMetricsSnapshotAsync(new TreeMetricsRequest { TreeIds = ["export-ledger"] }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.IsPermissionDenied, Is.True, "the caller still learns the read was refused");
            Assert.That(ex.IsTransient, Is.False);
            Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
            Assert.That(connection.Status.RequiresAuthentication, Is.False, "a signed-in caller is not asked to sign in");
            Assert.That(changes, Is.Empty, "the pill does not move");
        });
    }

    [Test]
    public async Task APerCallDenial_onAStream_leavesTheConnectionConnected()
    {
        var client = new FakeStateClient { ObserveMetricsHandler = _ => DeniedMetricStream() };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        var changes = new List<LatticeConnectionStatus>();
        connection.StatusChanged += changes.Add;

        var ex = Assert.ThrowsAsync<LatticeStateApiException>(async () =>
        {
            await foreach (var _ in connection.ObserveMetricsAsync(new TreeMetricsRequest { TreeIds = ["export-ledger"] }))
            {
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(ex!.IsPermissionDenied, Is.True);
            Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Connected));
            Assert.That(changes, Is.Empty, "a denied subscription is not resubscribed and does not move the pill");
        });
    }

    [Test]
    public async Task AnUnauthenticatedCall_stillFaultsTheConnectionWithASignInRequirement()
    {
        // The contrast: a refused credential is a fact about the session, not one call.
        var client = new FakeStateClient { MetricsHandler = _ => throw Permanent() };
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());

        Assert.ThrowsAsync<LatticeStateApiException>(
            () => connection.GetMetricsSnapshotAsync(new TreeMetricsRequest()));

        Assert.Multiple(() =>
        {
            Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Faulted));
            Assert.That(connection.Status.RequiresAuthentication, Is.True);
        });
    }

    private static async IAsyncEnumerable<TreeMetricsSnapshot> DeniedMetricStream()
    {
        await Task.CompletedTask;
        if (DateTimeOffset.UtcNow.Year > 0)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, "denied"));
        }

        yield break;
    }
}
