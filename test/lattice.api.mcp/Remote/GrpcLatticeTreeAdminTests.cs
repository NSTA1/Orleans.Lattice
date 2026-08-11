using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeTreeAdmin"/>, the remote-host adapter that
/// fronts <see cref="ILatticeTreeAdmin"/> over the tree-administration-API gRPC
/// client. At this scaffolding stage the facade exposes the capability probe, so the
/// adapter is proven to forward its request and unwrap the response, plus the
/// argument guards. Deterministic over a <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeTreeAdminTests
{
    private static GrpcLatticeTreeAdmin Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.TreeAdminClient(invoker));

    private static LatticeTreeAdminCapabilities Caps(string tree) => new()
    {
        TreeId = tree,
        CanAdministerTree = false,
        Schema = new LatticeSchemaCapabilities { TreeId = tree },
    };

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeTreeAdmin(null!), Throws.ArgumentNullException);

    [Test]
    public async Task ProbeCapabilitiesAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => Caps("orders"));

        var result = await Adapter(invoker).ProbeCapabilitiesAsync("orders");

        var sent = (TreeAdminTreeRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.CanAdministerTree, Is.False);
            Assert.That(result.Schema.TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void ProbeCapabilitiesAsync_empty_tree_throws()
        => Assert.ThrowsAsync<ArgumentException>(
            async () => await Adapter(new FakeCallInvoker(_ => Caps("x"))).ProbeCapabilitiesAsync(""));

    [Test]
    public async Task GetShardHotnessAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeHotnessReport
        {
            TreeId = "orders",
            Shards = System.Collections.Immutable.ImmutableArray<ShardHotnessSnapshot>.Empty,
        });

        var result = await Adapter(invoker).GetShardHotnessAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task GetDiagnosticsAsync_forwards_the_deep_flag()
    {
        var invoker = new FakeCallInvoker(_ => new TreeAdminDiagnosticReport
        {
            TreeId = "orders",
            Deep = true,
            Shards = System.Collections.Immutable.ImmutableArray<ShardDiagnosticSnapshot>.Empty,
        });

        var result = await Adapter(invoker).GetDiagnosticsAsync("orders", deep: true);

        var sent = (TreeAdminDiagnosticsRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Deep, Is.True);
            Assert.That(result.Deep, Is.True);
        });
    }

    [Test]
    public async Task InspectShardMapAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new ShardMapInspection { TreeId = "orders", PhysicalTreeId = "phys" });

        var result = await Adapter(invoker).InspectShardMapAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.PhysicalTreeId, Is.EqualTo("phys"));
        });
    }

    [Test]
    public async Task GetProjectionDigestAsync_forwards_the_shard_index()
    {
        var invoker = new FakeCallInvoker(_ => new ShardProjectionDigestReport
        {
            TreeId = "orders",
            ShardIndex = 3,
            HashHex = "ab",
        });

        var result = await Adapter(invoker).GetProjectionDigestAsync("orders", 3);

        var sent = (TreeAdminShardRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.ShardIndex, Is.EqualTo(3));
            Assert.That(result.ShardIndex, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task GetTreeStatsAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeStatsReport { TreeId = "orders" });

        var result = await Adapter(invoker).GetTreeStatsAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task GetStorageUsageAsync_forwards_the_deep_flag()
    {
        var invoker = new FakeCallInvoker(_ => new ClusterStorageUsageSummary
        {
            Deep = true,
            Trees = System.Collections.Immutable.ImmutableArray<TreeStorageUsageSnapshot>.Empty,
        });

        var result = await Adapter(invoker).GetStorageUsageAsync(deep: true);

        var sent = (TreeAdminStorageUsageRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.Deep, Is.True);
            Assert.That(result.Deep, Is.True);
        });
    }
}
