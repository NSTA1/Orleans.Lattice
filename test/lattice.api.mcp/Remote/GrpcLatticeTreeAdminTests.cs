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

    [Test]
    public async Task CreateTreeAsync_forwards_the_sizing_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCreationResult { TreeId = "orders", Created = true, ShardCount = 8 });

        var result = await Adapter(invoker).CreateTreeAsync("orders", shardCount: 8, maxLeafKeys: 64, maxInternalChildren: 32);

        var sent = (TreeAdminCreateRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.ShardCount, Is.EqualTo(8));
            Assert.That(sent.MaxLeafKeys, Is.EqualTo(64));
            Assert.That(sent.MaxInternalChildren, Is.EqualTo(32));
            Assert.That(result.Created, Is.True);
        });
    }

    [Test]
    public async Task CreateViewAsync_forwards_provider_payload_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeViewStatus
        {
            ViewName = "orders-by-region",
            SourceTreeId = "orders",
            ProviderKey = "app.region.v1",
            ProjectionVersion = "v1",
        });

        var result = await Adapter(invoker).CreateViewAsync(
            "orders-by-region",
            "orders",
            "app.region.v1",
            [1, 2, 3]);

        var sent = (TreeAdminCreateViewRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.ViewName, Is.EqualTo("orders-by-region"));
            Assert.That(sent.SourceTreeId, Is.EqualTo("orders"));
            Assert.That(sent.ProviderKey, Is.EqualTo("app.region.v1"));
            Assert.That(sent.Payload, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(result.ProjectionVersion, Is.EqualTo("v1"));
        });
    }

    [Test]
    public async Task CheckTreeExistsAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeExistenceResult { TreeId = "orders", Exists = true });

        var result = await Adapter(invoker).CheckTreeExistsAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.Exists, Is.True);
        });
    }

    [Test]
    public async Task SetTreeAliasAsync_forwards_both_ids_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeAliasResolution { TreeId = "orders", PhysicalTreeId = "phys", IsAliased = true });

        var result = await Adapter(invoker).SetTreeAliasAsync("orders", "phys");

        var sent = (TreeAdminSetAliasRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.PhysicalTreeId, Is.EqualTo("phys"));
            Assert.That(result.IsAliased, Is.True);
        });
    }

    [Test]
    public async Task ResolveTreeAliasAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeAliasResolution { TreeId = "orders", PhysicalTreeId = "phys", IsAliased = true });

        var result = await Adapter(invoker).ResolveTreeAliasAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.PhysicalTreeId, Is.EqualTo("phys"));
        });
    }

    [Test]
    public async Task GetTreeConfigAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeConfigurationReport { TreeId = "orders", Exists = true });

        var result = await Adapter(invoker).GetTreeConfigAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.Exists, Is.True);
        });
    }

    [Test]
    public async Task SetTreeConfigAsync_forwards_the_update_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeConfigurationReport { TreeId = "orders", Exists = true, PublishEvents = false });

        var result = await Adapter(invoker).SetTreeConfigAsync("orders", new TreeConfigurationUpdate
        {
            ApplyPublishEvents = true,
            PublishEvents = false,
        });

        var sent = (TreeAdminSetConfigRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Update.ApplyPublishEvents, Is.True);
            Assert.That(sent.Update.PublishEvents, Is.False);
            Assert.That(result.PublishEvents, Is.False);
        });
    }

    [Test]
    public async Task GetShardMapAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeShardMapView
        {
            TreeId = "orders",
            HasCustomMap = true,
            PhysicalShardIndices = System.Collections.Immutable.ImmutableArray.Create(0, 1),
        });

        var result = await Adapter(invoker).GetShardMapAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.HasCustomMap, Is.True);
            Assert.That(result.PhysicalShardIndices, Is.EqualTo(new[] { 0, 1 }));
        });
    }

    [Test]
    public async Task RestoreTreeAsync_forwards_the_ids_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeRestoreResult
        {
            BackupId = "bk-1",
            TargetTreeId = "orders",
            Mode = TreeRestoreMode.ShadowCutover,
            OperationId = "op-1",
            ManifestChain = ["bk-1"],
            EntriesApplied = 4,
        });

        var result = await Adapter(invoker).RestoreTreeAsync("orders", "bk-1", "op-1");

        var sent = (TreeAdminRestoreRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.BackupId, Is.EqualTo("bk-1"));
            Assert.That(sent.OperationId, Is.EqualTo("op-1"));
            Assert.That(result.TargetTreeId, Is.EqualTo("orders"));
            Assert.That(result.EntriesApplied, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task RestoreTreeSetAsync_forwards_the_set_id_and_unwraps_the_member_results()
    {
        var invoker = new FakeCallInvoker(_ => new TreeRestoreSetResult
        {
            Results =
            [
                new TreeRestoreResult { BackupId = "bk-a", TargetTreeId = "a", Mode = TreeRestoreMode.ShadowCutover, OperationId = "op", ManifestChain = [], EntriesApplied = 1 },
            ],
        });

        var result = await Adapter(invoker).RestoreTreeSetAsync("nightly");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminRestoreSetRequest)invoker.LastRequest!).SetId, Is.EqualTo("nightly"));
            Assert.That(result, Has.Count.EqualTo(1));
            Assert.That(result[0].TargetTreeId, Is.EqualTo("a"));
        });
    }

    [Test]
    public async Task RevertTreeRestoreAsync_forwards_the_result()
    {
        var restore = new TreeRestoreResult
        {
            BackupId = "bk-1",
            TargetTreeId = "orders",
            Mode = TreeRestoreMode.ShadowCutover,
            OperationId = "op-1",
            ManifestChain = [],
            EntriesApplied = 0,
            ShadowPhysicalTreeId = "phys-new",
            PreviousPhysicalTreeId = "phys-old",
        };
        var invoker = new FakeCallInvoker(_ => restore);

        await Adapter(invoker).RevertTreeRestoreAsync(restore);

        var sent = (TreeRestoreResult)invoker.LastRequest!;
        Assert.That(sent.TargetTreeId, Is.EqualTo("orders"));
        Assert.That(sent.PreviousPhysicalTreeId, Is.EqualTo("phys-old"));
    }

    [Test]
    public async Task ReshardTreeAsync_forwards_the_target_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeReshardStatus
        {
            TreeId = "orders",
            InProgress = false,
            CurrentPhysicalShardCount = 4,
            VirtualShardCount = 4096,
            MapVersion = 3,
            RequestedShardCount = 4,
        });

        var result = await Adapter(invoker).ReshardTreeAsync("orders", 4);

        var sent = (TreeAdminReshardRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.TargetShardCount, Is.EqualTo(4));
            Assert.That(result.CurrentPhysicalShardCount, Is.EqualTo(4));
            Assert.That(result.RequestedShardCount, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task GetReshardStatusAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeReshardStatus
        {
            TreeId = "orders",
            InProgress = true,
            CurrentPhysicalShardCount = 2,
            VirtualShardCount = 4096,
            MapVersion = 1,
        });

        var result = await Adapter(invoker).GetReshardStatusAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.InProgress, Is.True);
            Assert.That(result.CurrentPhysicalShardCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task ResizeTreeAsync_forwards_the_capacity_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeResizeStatus
        {
            TreeId = "orders",
            InProgress = false,
            CurrentMaxLeafKeys = 256,
            CurrentMaxInternalChildren = 128,
            RequestedMaxLeafKeys = 256,
            RequestedMaxInternalChildren = 128,
        });

        var result = await Adapter(invoker).ResizeTreeAsync("orders", 256, 128);

        var sent = (TreeAdminResizeRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.NewMaxLeafKeys, Is.EqualTo(256));
            Assert.That(sent.NewMaxInternalChildren, Is.EqualTo(128));
            Assert.That(result.CurrentMaxLeafKeys, Is.EqualTo(256));
            Assert.That(result.RequestedMaxLeafKeys, Is.EqualTo(256));
        });
    }

    [Test]
    public async Task UndoTreeResizeAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeResizeStatus
        {
            TreeId = "orders",
            InProgress = false,
            CurrentMaxLeafKeys = 64,
            CurrentMaxInternalChildren = 32,
        });

        var result = await Adapter(invoker).UndoTreeResizeAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.CurrentMaxLeafKeys, Is.EqualTo(64));
            Assert.That(result.RequestedMaxLeafKeys, Is.Null);
        });
    }

    [Test]
    public async Task GetResizeStatusAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeResizeStatus
        {
            TreeId = "orders",
            InProgress = true,
            CurrentMaxLeafKeys = 128,
            CurrentMaxInternalChildren = 128,
        });

        var result = await Adapter(invoker).GetResizeStatusAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.InProgress, Is.True);
            Assert.That(result.CurrentMaxLeafKeys, Is.EqualTo(128));
        });
    }

    [Test]
    public async Task SnapshotTreeAsync_forwards_the_destination_and_mode_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeSnapshotStatus
        {
            TreeId = "orders",
            InProgress = true,
            RequestedDestinationTreeId = "orders-snap",
            RequestedMode = TreeSnapshotMode.Online,
        });

        var result = await Adapter(invoker).SnapshotTreeAsync("orders", "orders-snap", TreeSnapshotMode.Online, 128, 64);

        var sent = (TreeAdminSnapshotRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.DestinationTreeId, Is.EqualTo("orders-snap"));
            Assert.That(sent.Mode, Is.EqualTo(TreeSnapshotMode.Online));
            Assert.That(sent.MaxLeafKeys, Is.EqualTo(128));
            Assert.That(sent.MaxInternalChildren, Is.EqualTo(64));
            Assert.That(result.RequestedDestinationTreeId, Is.EqualTo("orders-snap"));
            Assert.That(result.RequestedMode, Is.EqualTo(TreeSnapshotMode.Online));
        });
    }

    [Test]
    public async Task GetSnapshotStatusAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeSnapshotStatus
        {
            TreeId = "orders",
            InProgress = false,
        });

        var result = await Adapter(invoker).GetSnapshotStatusAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.InProgress, Is.False);
        });
    }

    [Test]
    public async Task GetWalPlacementAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeWalPlacement { TreeId = "orders", Version = 3 });

        var result = await Adapter(invoker).GetWalPlacementAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.Version, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task AuditWalPlacementAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeWalPlacementAudit { TreeId = "orders", PartitionCount = 2 });

        var result = await Adapter(invoker).AuditWalPlacementAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.PartitionCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task PlanWalMoveAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeWalMovePlan
        {
            TreeId = "orders",
            Partition = 1,
            ToProviderKey = "wal-secondary",
        });

        var result = await Adapter(invoker).PlanWalMoveAsync("orders", 1, "wal-secondary");

        var sent = (TreeAdminWalMovePlanRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Partition, Is.EqualTo(1));
            Assert.That(sent.TargetProviderKey, Is.EqualTo("wal-secondary"));
            Assert.That(result.ToProviderKey, Is.EqualTo("wal-secondary"));
        });
    }

    [Test]
    public async Task ExecuteWalMoveAsync_forwards_request_with_options_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeWalMoveReceipt
        {
            TreeId = "orders",
            Partition = 1,
            Outcome = TreeWalMoveOutcome.Moved,
        });

        var options = new TreeWalMoveOptions
        {
            QuiesceLeaseSeconds = 45,
            CopyPageSize = 128,
            DisableVerifyAfterCopy = true,
        };
        var result = await Adapter(invoker).ExecuteWalMoveAsync("orders", 1, "wal-secondary", options);

        var sent = (TreeAdminWalMoveExecuteRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Partition, Is.EqualTo(1));
            Assert.That(sent.TargetProviderKey, Is.EqualTo("wal-secondary"));
            Assert.That(sent.Options, Is.EqualTo(options));
            Assert.That(result.Outcome, Is.EqualTo(TreeWalMoveOutcome.Moved));
        });
    }

    [Test]
    public async Task ReclaimMovedWalSourceAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeWalMoveReceipt
        {
            TreeId = "orders",
            Partition = 1,
            Outcome = TreeWalMoveOutcome.SourceReclaimed,
        });

        var result = await Adapter(invoker).ReclaimMovedWalSourceAsync("orders", 1, "wal-primary");

        var sent = (TreeAdminWalReclaimRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Partition, Is.EqualTo(1));
            Assert.That(sent.SourceProviderKey, Is.EqualTo("wal-primary"));
            Assert.That(result.Outcome, Is.EqualTo(TreeWalMoveOutcome.SourceReclaimed));
        });
    }

    [Test]
    public async Task ListViewsAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeViewCatalog
        {
            Views = System.Collections.Immutable.ImmutableArray<TreeViewInfo>.Empty,
        });

        var result = await Adapter(invoker).ListViewsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.InstanceOf<TreeAdminViewListRequest>());
            Assert.That(result.Views, Is.Empty);
        });
    }

    [Test]
    public async Task GetViewStatusAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeViewStatus
        {
            ViewName = "orders-by-region",
            SourceTreeId = "orders",
            ApplyLag = 4,
        });

        var result = await Adapter(invoker).GetViewStatusAsync("orders-by-region");

        var sent = (TreeAdminViewRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.ViewName, Is.EqualTo("orders-by-region"));
            Assert.That(result.SourceTreeId, Is.EqualTo("orders"));
            Assert.That(result.ApplyLag, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task RebuildViewAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeViewStatus
        {
            ViewName = "orders-by-region",
            SourceTreeId = "orders",
        });

        var result = await Adapter(invoker).RebuildViewAsync("orders-by-region");

        var sent = (TreeAdminViewRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.ViewName, Is.EqualTo("orders-by-region"));
            Assert.That(result.ViewName, Is.EqualTo("orders-by-region"));
        });
    }

    [Test]
    public async Task ReconcileViewAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeViewReconcileResult
        {
            ViewName = "orders-by-region",
            SourceTreeId = "orders",
            DriftRepaired = true,
        });

        var result = await Adapter(invoker).ReconcileViewAsync("orders-by-region");

        var sent = (TreeAdminViewRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.ViewName, Is.EqualTo("orders-by-region"));
            Assert.That(result.DriftRepaired, Is.True);
        });
    }

    [Test]
    public async Task DropViewAsync_forwards_request_and_completes()
    {
        var invoker = new FakeCallInvoker(_ => new TreeAdminViewRequest { ViewName = "orders-by-region" });

        await Adapter(invoker).DropViewAsync("orders-by-region");

        var sent = (TreeAdminViewRequest)invoker.LastRequest!;
        Assert.That(sent.ViewName, Is.EqualTo("orders-by-region"));
    }

    [Test]
    public async Task ListTagIndexesAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeTagIndexCatalog
        {
            Indexes = System.Collections.Immutable.ImmutableArray<TreeTagIndexInfo>.Empty,
        });

        var result = await Adapter(invoker).ListTagIndexesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.InstanceOf<TreeAdminTagIndexListRequest>());
            Assert.That(result.Indexes, Is.Empty);
        });
    }

    [Test]
    public async Task GetTagIndexStatusAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeTagIndexStatus
        {
            IndexName = "by-tag",
            TreeId = "tag-by-tag",
        });

        var result = await Adapter(invoker).GetTagIndexStatusAsync("by-tag");

        var sent = (TreeAdminTagIndexRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.IndexName, Is.EqualTo("by-tag"));
            Assert.That(result.TreeId, Is.EqualTo("tag-by-tag"));
        });
    }

    [Test]
    public async Task ReconcileTagIndexAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeTagReconcileReport
        {
            IndexName = "by-tag",
            TreeId = "tag-by-tag",
            OrphanRowsRemoved = 3,
        });

        var result = await Adapter(invoker).ReconcileTagIndexAsync("by-tag");

        var sent = (TreeAdminTagIndexRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.IndexName, Is.EqualTo("by-tag"));
            Assert.That(result.OrphanRowsRemoved, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task TriggerShardCompactionAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeCompactionTriggerResult
        {
            TreeId = "orders",
            ShardIndex = 2,
            Accepted = true,
        });

        var result = await Adapter(invoker).TriggerShardCompactionAsync("orders", 2);

        var sent = (TreeAdminShardRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.ShardIndex, Is.EqualTo(2));
            Assert.That(result.Accepted, Is.True);
        });
    }

    [Test]
    public async Task GetHistoryRetentionAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeHistoryRetention
        {
            TreeId = "orders",
            Mode = TreeHistoryRetentionMode.Hybrid,
            Window = TimeSpan.FromHours(6),
        });

        var result = await Adapter(invoker).GetHistoryRetentionAsync("orders");

        var sent = (TreeAdminTreeRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(result.Mode, Is.EqualTo(TreeHistoryRetentionMode.Hybrid));
            Assert.That(result.Window, Is.EqualTo(TimeSpan.FromHours(6)));
        });
    }

    [Test]
    public async Task SetHistoryRetentionAsync_forwards_request_and_unwraps_response()
    {
        var invoker = new FakeCallInvoker(_ => new TreeHistoryRetention
        {
            TreeId = "orders",
            Mode = TreeHistoryRetentionMode.FullValue,
            Window = TimeSpan.FromHours(1),
        });

        var result = await Adapter(invoker).SetHistoryRetentionAsync("orders", TreeHistoryRetentionMode.FullValue, TimeSpan.FromHours(1));

        var sent = (TreeAdminSetRetentionRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Mode, Is.EqualTo(TreeHistoryRetentionMode.FullValue));
            Assert.That(sent.Window, Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(result.Mode, Is.EqualTo(TreeHistoryRetentionMode.FullValue));
        });
    }
}
