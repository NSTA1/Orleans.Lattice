using System.Collections.Immutable;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TreeAdminDiagnosticsToolHandlers"/>, the thin adapter
/// methods behind the tree-administration read-only diagnostics tools. Every test
/// drives a handler with a substituted <see cref="ILatticeTreeAdmin"/> facade and
/// proves the handler forwards the tool-call arguments verbatim and returns the
/// facade result unchanged - it re-implements no authorization, read, or projection
/// logic. Covers each of the six read-only operations plus the null-facade guards.
/// Deterministic - fakes, no cluster.
/// </summary>
[TestFixture]
public sealed class TreeAdminDiagnosticsToolHandlersTests
{
    private static ILatticeTreeAdmin TreeAdmin() => Substitute.For<ILatticeTreeAdmin>();

    [Test]
    public async Task GetShardHotnessAsync_forwards_the_tree_id_and_returns_the_report()
    {
        var admin = TreeAdmin();
        var expected = new TreeHotnessReport { TreeId = "orders", Shards = ImmutableArray<ShardHotnessSnapshot>.Empty };
        admin.GetShardHotnessAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminDiagnosticsToolHandlers.GetShardHotnessAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetShardHotnessAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetDiagnosticsAsync_forwards_the_deep_flag_and_returns_the_report()
    {
        var admin = TreeAdmin();
        var expected = new TreeAdminDiagnosticReport { TreeId = "orders", Deep = true, Shards = ImmutableArray<ShardDiagnosticSnapshot>.Empty };
        admin.GetDiagnosticsAsync("orders", true, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminDiagnosticsToolHandlers.GetDiagnosticsAsync(admin, "orders", deep: true, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetDiagnosticsAsync("orders", true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetDiagnosticsAsync_defaults_deep_to_false()
    {
        var admin = TreeAdmin();
        admin.GetDiagnosticsAsync("orders", false, Arg.Any<CancellationToken>())
            .Returns(new TreeAdminDiagnosticReport { TreeId = "orders", Shards = ImmutableArray<ShardDiagnosticSnapshot>.Empty });

        await TreeAdminDiagnosticsToolHandlers.GetDiagnosticsAsync(admin, "orders");

        await admin.Received(1).GetDiagnosticsAsync("orders", false, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task InspectShardMapAsync_forwards_the_tree_id_and_returns_the_inspection()
    {
        var admin = TreeAdmin();
        var expected = new ShardMapInspection { TreeId = "orders", PhysicalTreeId = "phys" };
        admin.InspectShardMapAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminDiagnosticsToolHandlers.InspectShardMapAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).InspectShardMapAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetProjectionDigestAsync_forwards_the_tree_id_and_shard_index()
    {
        var admin = TreeAdmin();
        var expected = new ShardProjectionDigestReport { TreeId = "orders", ShardIndex = 2, HashHex = "abcd" };
        admin.GetProjectionDigestAsync("orders", 2, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminDiagnosticsToolHandlers.GetProjectionDigestAsync(admin, "orders", 2, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetProjectionDigestAsync("orders", 2, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetTreeStatsAsync_forwards_the_tree_id_and_returns_the_stats()
    {
        var admin = TreeAdmin();
        var expected = new TreeStatsReport { TreeId = "orders" };
        admin.GetTreeStatsAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminDiagnosticsToolHandlers.GetTreeStatsAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetTreeStatsAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetStorageUsageAsync_forwards_the_deep_flag_and_returns_the_summary()
    {
        var admin = TreeAdmin();
        var expected = new ClusterStorageUsageSummary { Deep = true, Trees = ImmutableArray<TreeStorageUsageSnapshot>.Empty };
        admin.GetStorageUsageAsync(true, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminDiagnosticsToolHandlers.GetStorageUsageAsync(admin, deep: true, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetStorageUsageAsync(true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetStorageUsageAsync_defaults_deep_to_false()
    {
        var admin = TreeAdmin();
        admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageSummary { Trees = ImmutableArray<TreeStorageUsageSnapshot>.Empty });

        await TreeAdminDiagnosticsToolHandlers.GetStorageUsageAsync(admin);

        await admin.Received(1).GetStorageUsageAsync(false, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Handlers_reject_a_null_facade()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TreeAdminDiagnosticsToolHandlers.GetShardHotnessAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminDiagnosticsToolHandlers.GetDiagnosticsAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminDiagnosticsToolHandlers.InspectShardMapAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminDiagnosticsToolHandlers.GetProjectionDigestAsync(null!, "t", 0), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminDiagnosticsToolHandlers.GetTreeStatsAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminDiagnosticsToolHandlers.GetStorageUsageAsync(null!), Throws.ArgumentNullException);
        });
    }
}
