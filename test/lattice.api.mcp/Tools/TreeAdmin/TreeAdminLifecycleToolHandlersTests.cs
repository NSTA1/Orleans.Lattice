using System.Collections.Immutable;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TreeAdminLifecycleToolHandlers"/>, the thin adapter
/// methods behind the tree-administration lifecycle tools. Every test drives a
/// handler with a substituted <see cref="ILatticeTreeAdmin"/> facade and proves the
/// handler forwards the tool-call arguments verbatim and returns the facade result
/// unchanged - it re-implements no authorization, registration, or configuration
/// logic. Covers each of the seven lifecycle operations, the flat-parameter to
/// <see cref="TreeConfigurationUpdate"/> assembly in the config setter, and the
/// null-facade guards. Deterministic - fakes, no cluster.
/// </summary>
[TestFixture]
public sealed class TreeAdminLifecycleToolHandlersTests
{
    private static ILatticeTreeAdmin TreeAdmin() => Substitute.For<ILatticeTreeAdmin>();

    [Test]
    public async Task CreateTreeAsync_forwards_the_sizing_and_returns_the_result()
    {
        var admin = TreeAdmin();
        var expected = new TreeCreationResult { TreeId = "orders", Created = true, ShardCount = 8 };
        admin.CreateTreeAsync("orders", 8, 64, 32, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.CreateTreeAsync(admin, "orders", 8, 64, 32, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).CreateTreeAsync("orders", 8, 64, 32, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CreateTreeAsync_defaults_all_sizing_to_null()
    {
        var admin = TreeAdmin();
        admin.CreateTreeAsync("orders", null, null, null, Arg.Any<CancellationToken>())
            .Returns(new TreeCreationResult { TreeId = "orders" });

        await TreeAdminLifecycleToolHandlers.CreateTreeAsync(admin, "orders");

        await admin.Received(1).CreateTreeAsync("orders", null, null, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CheckTreeExistsAsync_forwards_the_tree_id_and_returns_the_result()
    {
        var admin = TreeAdmin();
        var expected = new TreeExistenceResult { TreeId = "orders", Exists = true };
        admin.CheckTreeExistsAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.CheckTreeExistsAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).CheckTreeExistsAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetTreeAliasAsync_forwards_both_ids_and_returns_the_resolution()
    {
        var admin = TreeAdmin();
        var expected = new TreeAliasResolution { TreeId = "orders", PhysicalTreeId = "phys", IsAliased = true };
        admin.SetTreeAliasAsync("orders", "phys", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.SetTreeAliasAsync(admin, "orders", "phys", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).SetTreeAliasAsync("orders", "phys", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ResolveTreeAliasAsync_forwards_the_tree_id_and_returns_the_resolution()
    {
        var admin = TreeAdmin();
        var expected = new TreeAliasResolution { TreeId = "orders", PhysicalTreeId = "phys", IsAliased = true };
        admin.ResolveTreeAliasAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.ResolveTreeAliasAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).ResolveTreeAliasAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetTreeConfigAsync_forwards_the_tree_id_and_returns_the_report()
    {
        var admin = TreeAdmin();
        var expected = new TreeConfigurationReport { TreeId = "orders", Exists = true };
        admin.GetTreeConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.GetTreeConfigAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetTreeConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetTreeConfigAsync_assembles_the_update_from_the_flat_parameters()
    {
        var admin = TreeAdmin();
        var expected = new TreeConfigurationReport { TreeId = "orders", Exists = true };
        admin.SetTreeConfigAsync("orders", Arg.Any<TreeConfigurationUpdate>(), Arg.Any<CancellationToken>())
            .Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.SetTreeConfigAsync(
            admin,
            "orders",
            applyPublishEvents: true,
            publishEvents: false,
            applyMaintainProjectionDigest: true,
            maintainProjectionDigest: true,
            applyHistoryRetention: true,
            historyRetentionMode: HistoryRetentionMode.FullValue,
            historyRetentionWindowTicks: 500,
            CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).SetTreeConfigAsync(
            "orders",
            Arg.Is<TreeConfigurationUpdate>(u =>
                u.ApplyPublishEvents == true &&
                u.PublishEvents == false &&
                u.ApplyMaintainProjectionDigest == true &&
                u.MaintainProjectionDigest == true &&
                u.ApplyHistoryRetention == true &&
                u.HistoryRetentionMode == HistoryRetentionMode.FullValue &&
                u.HistoryRetentionWindowTicks == 500),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetTreeConfigAsync_defaults_leave_every_override_unchanged()
    {
        var admin = TreeAdmin();
        admin.SetTreeConfigAsync("orders", Arg.Any<TreeConfigurationUpdate>(), Arg.Any<CancellationToken>())
            .Returns(new TreeConfigurationReport { TreeId = "orders" });

        await TreeAdminLifecycleToolHandlers.SetTreeConfigAsync(admin, "orders");

        await admin.Received(1).SetTreeConfigAsync(
            "orders",
            Arg.Is<TreeConfigurationUpdate>(u =>
                !u.ApplyPublishEvents &&
                !u.ApplyMaintainProjectionDigest &&
                !u.ApplyHistoryRetention),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetShardMapAsync_forwards_the_tree_id_and_returns_the_view()
    {
        var admin = TreeAdmin();
        var expected = new TreeShardMapView { TreeId = "orders", PhysicalShardIndices = ImmutableArray<int>.Empty };
        admin.GetShardMapAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.GetShardMapAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetShardMapAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetTreeDeletionStatusAsync_forwards_the_tree_id_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeDeletionStatus { TreeId = "orders", IsDeleted = true };
        admin.GetTreeDeletionStatusAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.GetTreeDeletionStatusAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).GetTreeDeletionStatusAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteTreeAsync_forwards_the_tree_id_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeDeletionStatus { TreeId = "orders", IsDeleted = true, CanRecover = true };
        admin.DeleteTreeAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.DeleteTreeAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).DeleteTreeAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RecoverTreeAsync_forwards_the_tree_id_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeDeletionStatus { TreeId = "orders", IsDeleted = false };
        admin.RecoverTreeAsync("orders", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.RecoverTreeAsync(admin, "orders", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).RecoverTreeAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PurgeTreeAsync_forwards_the_confirmation_flag_and_returns_the_status()
    {
        var admin = TreeAdmin();
        var expected = new TreeDeletionStatus { TreeId = "orders", IsDeleted = true, PurgeComplete = true };
        admin.PurgeTreeAsync("orders", true, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await TreeAdminLifecycleToolHandlers.PurgeTreeAsync(admin, "orders", confirm: true, CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).PurgeTreeAsync("orders", true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PurgeTreeAsync_defaults_confirmation_to_false()
    {
        var admin = TreeAdmin();
        admin.PurgeTreeAsync("orders", false, Arg.Any<CancellationToken>())
            .Returns(new TreeDeletionStatus { TreeId = "orders" });

        await TreeAdminLifecycleToolHandlers.PurgeTreeAsync(admin, "orders");

        await admin.Received(1).PurgeTreeAsync("orders", false, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Handlers_reject_a_null_facade()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TreeAdminLifecycleToolHandlers.CreateTreeAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.CheckTreeExistsAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.SetTreeAliasAsync(null!, "t", "p"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.ResolveTreeAliasAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.GetTreeConfigAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.SetTreeConfigAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.GetShardMapAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.GetTreeDeletionStatusAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.DeleteTreeAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.RecoverTreeAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminLifecycleToolHandlers.PurgeTreeAsync(null!, "t"), Throws.ArgumentNullException);
        });
    }
}
