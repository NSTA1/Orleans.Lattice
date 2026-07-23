using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeReplicationControl"/>, the remote-host
/// adapter that fronts <see cref="ILatticeReplicationControl"/> over the
/// replication-API gRPC client. Every facade member is wire-backed (the
/// replication surface has a full gRPC binding), so each is proven to forward its
/// request and unwrap the response; the config projection and the argument guard
/// are covered. Deterministic over a <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeReplicationControlTests
{
    private static GrpcLatticeReplicationControl Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.ReplicationClient(invoker));

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeReplicationControl(null!), Throws.ArgumentNullException);

    [Test]
    public async Task EnableReplicationAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new ReplicationEnableResponse
        {
            TreeId = "orders",
            Mode = LatticeMergeMode.OrSet,
            AlreadyEnabled = false,
            BootstrapRequested = true,
        });

        var result = await Adapter(invoker).EnableReplicationAsync("orders", LatticeMergeMode.OrSet, "cluster-b");

        var sent = (ReplicationEnableRequestMessage)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(sent.BootstrapSourceClusterId, Is.EqualTo("cluster-b"));
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(result.AlreadyEnabled, Is.False);
            Assert.That(result.BootstrapRequested, Is.True);
        });
    }

    [Test]
    public async Task EnableReplicationAsync_omitted_bootstrap_forwards_null()
    {
        var invoker = new FakeCallInvoker(_ => new ReplicationEnableResponse
        {
            TreeId = "orders",
            Mode = LatticeMergeMode.OrSet,
        });

        await Adapter(invoker).EnableReplicationAsync("orders", LatticeMergeMode.OrSet);

        Assert.That(((ReplicationEnableRequestMessage)invoker.LastRequest!).BootstrapSourceClusterId, Is.Null);
    }

    [Test]
    public void EnableReplicationAsync_empty_tree_throws()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => new ReplicationEnableResponse { TreeId = "x" }))
                .EnableReplicationAsync("", LatticeMergeMode.OrSet),
            Throws.ArgumentException);

    [Test]
    public async Task DisableReplicationAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new ReplicationDisableResponse
        {
            TreeId = "orders",
            AlreadyDisabled = true,
        });

        var result = await Adapter(invoker).DisableReplicationAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((ReplicationDisableRequestMessage)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.AlreadyDisabled, Is.True);
        });
    }

    [Test]
    public void DisableReplicationAsync_empty_tree_throws()
        => Assert.That(
            async () => await Adapter(new FakeCallInvoker(_ => new ReplicationDisableResponse { TreeId = "x" }))
                .DisableReplicationAsync(""),
            Throws.ArgumentException);

    [Test]
    public async Task GetReplicationConfigAsync_projects_every_tree_entry()
    {
        var invoker = new FakeCallInvoker(_ => new ReplicationConfigResponse
        {
            Trees = new[]
            {
                new ReplicationTreeConfigMessage
                {
                    TreeId = "orders",
                    Enabled = true,
                    HasMode = true,
                    Mode = LatticeMergeMode.OrSet,
                    Ambiguous = false,
                },
                new ReplicationTreeConfigMessage
                {
                    TreeId = "carts",
                    Enabled = true,
                    HasMode = false,
                    Ambiguous = true,
                },
            },
        });

        var report = await Adapter(invoker).GetReplicationConfigAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Trees, Has.Count.EqualTo(2));
            Assert.That(report.Trees[0].TreeId, Is.EqualTo("orders"));
            Assert.That(report.Trees[0].Mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(report.Trees[0].Ambiguous, Is.False);
            Assert.That(report.Trees[1].TreeId, Is.EqualTo("carts"));
            Assert.That(report.Trees[1].Mode, Is.Null);
            Assert.That(report.Trees[1].Ambiguous, Is.True);
        });
    }

    [Test]
    public async Task GetReplicationConfigAsync_empty_report_projects_no_entries()
    {
        var report = await Adapter(new FakeCallInvoker(_ => new ReplicationConfigResponse()))
            .GetReplicationConfigAsync();

        Assert.That(report.Trees, Is.Empty);
    }
}
