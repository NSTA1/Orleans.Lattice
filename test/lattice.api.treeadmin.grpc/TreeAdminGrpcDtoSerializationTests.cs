using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Round-trips the gRPC-layer wire messages (the <c>Model</c> request / response
/// records the binding marshals with the Orleans serializer) to prove the transport
/// contract is coherent across the wire, and asserts alias hygiene: every gRPC wire
/// message carries a unique <c>[Alias]</c> drawn from the
/// <see cref="GrpcTreeAdminTypeAliases"/> registry under the reserved <c>oitg.</c>
/// prefix. The transport-agnostic facade DTOs (such as
/// <c>LatticeTreeAdminCapabilities</c>) are covered in the
/// <c>Orleans.Lattice.Api.TreeAdmin</c> test project; this fixture covers the
/// gRPC-only envelopes.
/// </summary>
[TestFixture]
public sealed class TreeAdminGrpcDtoSerializationTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void TreeAdminTreeRequest_round_trips()
    {
        Assert.That(RoundTrip(new TreeAdminTreeRequest { TreeId = "orders" }).TreeId, Is.EqualTo("orders"));
    }

    [Test]
    public void TreeAdminShardRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminShardRequest { TreeId = "orders", ShardIndex = 3 });
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.ShardIndex, Is.EqualTo(3));
        });
    }

    [Test]
    public void TreeAdminDiagnosticsRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminDiagnosticsRequest { TreeId = "orders", Deep = true });
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Deep, Is.True);
        });
    }

    [Test]
    public void TreeAdminStorageUsageRequest_round_trips()
    {
        Assert.That(RoundTrip(new TreeAdminStorageUsageRequest { Deep = true }).Deep, Is.True);
    }

    [Test]
    public void TreeHotnessReport_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeHotnessReport
        {
            TreeId = "orders",
            ShardCount = 1,
            TotalReads = 5,
            TotalWrites = 2,
            TotalOpsPerSecond = 7,
            SampledAt = DateTimeOffset.UnixEpoch,
            Shards = System.Collections.Immutable.ImmutableArray.Create(
                new ShardHotnessSnapshot { ShardIndex = 0, Reads = 5, Writes = 2, OpsPerSecond = 7, WindowSeconds = 1 }),
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Shards, Has.Length.EqualTo(1));
            Assert.That(copy.Shards[0].Reads, Is.EqualTo(5));
        });
    }

    [Test]
    public void ClusterStorageUsageSummary_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new ClusterStorageUsageSummary
        {
            TreeCount = 1,
            TotalBytes = 60,
            Deep = true,
            SampledAt = DateTimeOffset.UnixEpoch,
            Trees = System.Collections.Immutable.ImmutableArray.Create(
                new TreeStorageUsageSnapshot { TreeId = "orders", TotalBytes = 60, LiveKeys = 5, SampledAt = DateTimeOffset.UnixEpoch }),
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Deep, Is.True);
            Assert.That(copy.Trees, Has.Length.EqualTo(1));
            Assert.That(copy.Trees[0].TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public void AuthSchemeAdvertisementRequest_round_trips() =>
        Assert.That(RoundTrip(new AuthSchemeAdvertisementRequest()), Is.Not.Null);

    [Test]
    public void AuthSchemeDescriptor_round_trips_with_parameters()
    {
        var copy = RoundTrip(new AuthSchemeDescriptor
        {
            SchemeId = "entra",
            DisplayName = "Microsoft Entra",
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal) { ["authority"] = "https://login" },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.SchemeId, Is.EqualTo("entra"));
            Assert.That(copy.DisplayName, Is.EqualTo("Microsoft Entra"));
            Assert.That(copy.Parameters["authority"], Is.EqualTo("https://login"));
        });
    }

    [Test]
    public void AuthSchemeAdvertisement_round_trips_its_schemes()
    {
        var copy = RoundTrip(new AuthSchemeAdvertisement
        {
            Schemes = new[] { new AuthSchemeDescriptor { SchemeId = "basic" } },
        });

        Assert.That(copy.Schemes, Has.Count.EqualTo(1));
        Assert.That(copy.Schemes[0].SchemeId, Is.EqualTo("basic"));
    }

    [Test]
    public void TreeAdminCreateRequest_round_trips_with_optional_sizing()
    {
        var copy = RoundTrip(new TreeAdminCreateRequest
        {
            TreeId = "orders",
            ShardCount = 8,
            MaxLeafKeys = 64,
            MaxInternalChildren = null,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.ShardCount, Is.EqualTo(8));
            Assert.That(copy.MaxLeafKeys, Is.EqualTo(64));
            Assert.That(copy.MaxInternalChildren, Is.Null);
        });
    }

    [Test]
    public void TreeAdminSetAliasRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminSetAliasRequest { TreeId = "orders", PhysicalTreeId = "phys-orders" });
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.PhysicalTreeId, Is.EqualTo("phys-orders"));
        });
    }

    [Test]
    public void TreeAdminSetConfigRequest_round_trips_its_nested_update()
    {
        var copy = RoundTrip(new TreeAdminSetConfigRequest
        {
            TreeId = "orders",
            Update = new TreeConfigurationUpdate
            {
                ApplyPublishEvents = true,
                PublishEvents = false,
                ApplyHistoryRetention = true,
                HistoryRetentionMode = HistoryRetentionMode.FullValue,
                HistoryRetentionWindowTicks = 500,
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Update.ApplyPublishEvents, Is.True);
            Assert.That(copy.Update.PublishEvents, Is.False);
            Assert.That(copy.Update.ApplyHistoryRetention, Is.True);
            Assert.That(copy.Update.HistoryRetentionMode, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(copy.Update.HistoryRetentionWindowTicks, Is.EqualTo(500));
        });
    }

    [Test]
    public void TreeCreationResult_response_round_trips()
    {
        var copy = RoundTrip(new TreeCreationResult
        {
            TreeId = "orders",
            Created = true,
            ShardCount = 8,
            MaxLeafKeys = 64,
            MaxInternalChildren = 32,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Created, Is.True);
            Assert.That(copy.ShardCount, Is.EqualTo(8));
        });
    }

    [Test]
    public void TreeConfigurationReport_response_round_trips()
    {
        var copy = RoundTrip(new TreeConfigurationReport
        {
            TreeId = "orders",
            Exists = true,
            PublishEvents = true,
            ProjectionDigestPermanentlyDisabled = true,
            HistoryRetentionMode = HistoryRetentionMode.FullValue,
            HistoryRetentionWindowTicks = 99,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Exists, Is.True);
            Assert.That(copy.PublishEvents, Is.True);
            Assert.That(copy.ProjectionDigestPermanentlyDisabled, Is.True);
            Assert.That(copy.HistoryRetentionMode, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(copy.HistoryRetentionWindowTicks, Is.EqualTo(99));
        });
    }

    [Test]
    public void TreeShardMapView_response_round_trips()
    {
        var copy = RoundTrip(new TreeShardMapView
        {
            TreeId = "orders",
            HasCustomMap = true,
            MapVersion = 5,
            VirtualShardCount = 4,
            PhysicalShardCount = 2,
            PhysicalShardIndices = System.Collections.Immutable.ImmutableArray.Create(0, 1),
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.HasCustomMap, Is.True);
            Assert.That(copy.MapVersion, Is.EqualTo(5));
            Assert.That(copy.PhysicalShardIndices, Is.EqualTo(new[] { 0, 1 }));
        });
    }

    [Test]
    public void TreeExistenceResult_and_TreeAliasResolution_round_trip()
    {
        var existence = RoundTrip(new TreeExistenceResult { TreeId = "orders", Exists = true });
        var alias = RoundTrip(new TreeAliasResolution { TreeId = "orders", PhysicalTreeId = "phys", IsAliased = true });

        Assert.Multiple(() =>
        {
            Assert.That(existence.Exists, Is.True);
            Assert.That(alias.PhysicalTreeId, Is.EqualTo("phys"));
            Assert.That(alias.IsAliased, Is.True);
        });
    }

    [Test]
    public void TreeAdminBulkLoadSessionRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminBulkLoadSessionRequest { TreeId = "orders", OperationId = "load-1" });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.OperationId, Is.EqualTo("load-1"));
        });
    }

    [Test]
    public void TreeAdminBulkLoadAppendRequest_round_trips_its_ordered_entries()
    {
        var copy = RoundTrip(new TreeAdminBulkLoadAppendRequest
        {
            TreeId = "orders",
            OperationId = "load-1",
            ChunkIndex = 5,
            Entries =
            [
                new Orleans.Lattice.Api.Data.DataEntry { Key = "a", Value = [1, 2] },
                new Orleans.Lattice.Api.Data.DataEntry { Key = "b", Value = [3] },
            ],
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.OperationId, Is.EqualTo("load-1"));
            Assert.That(copy.ChunkIndex, Is.EqualTo(5));
            Assert.That(copy.Entries, Has.Count.EqualTo(2));
            Assert.That(copy.Entries[0].Key, Is.EqualTo("a"));
            Assert.That(copy.Entries[0].Value, Is.EqualTo(new byte[] { 1, 2 }));
            Assert.That(copy.Entries[1].Key, Is.EqualTo("b"));
        });
    }

    [Test]
    public void TreeAdminRestoreRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminRestoreRequest
        {
            TreeId = "orders",
            BackupId = "bk-1",
            OperationId = "restore-1",
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.BackupId, Is.EqualTo("bk-1"));
            Assert.That(copy.OperationId, Is.EqualTo("restore-1"));
        });
    }

    [Test]
    public void TreeAdminRestoreSetRequest_round_trips()
    {
        Assert.That(RoundTrip(new TreeAdminRestoreSetRequest { SetId = "nightly" }).SetId, Is.EqualTo("nightly"));
    }

    [Test]
    public void TreeAdminReshardRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminReshardRequest { TreeId = "orders", TargetShardCount = 8 });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.TargetShardCount, Is.EqualTo(8));
        });
    }

    [Test]
    public void TreeReshardStatus_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeReshardStatus
        {
            TreeId = "orders",
            InProgress = true,
            CurrentPhysicalShardCount = 2,
            VirtualShardCount = 4,
            MapVersion = 7,
            RequestedShardCount = 4,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.InProgress, Is.True);
            Assert.That(copy.CurrentPhysicalShardCount, Is.EqualTo(2));
            Assert.That(copy.VirtualShardCount, Is.EqualTo(4));
            Assert.That(copy.MapVersion, Is.EqualTo(7));
            Assert.That(copy.RequestedShardCount, Is.EqualTo(4));
        });
    }

    [Test]
    public void TreeAdminResizeRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminResizeRequest
        {
            TreeId = "orders",
            NewMaxLeafKeys = 256,
            NewMaxInternalChildren = 128,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.NewMaxLeafKeys, Is.EqualTo(256));
            Assert.That(copy.NewMaxInternalChildren, Is.EqualTo(128));
        });
    }

    [Test]
    public void TreeResizeStatus_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeResizeStatus
        {
            TreeId = "orders",
            InProgress = true,
            CurrentMaxLeafKeys = 64,
            CurrentMaxInternalChildren = 32,
            RequestedMaxLeafKeys = 256,
            RequestedMaxInternalChildren = 128,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.InProgress, Is.True);
            Assert.That(copy.CurrentMaxLeafKeys, Is.EqualTo(64));
            Assert.That(copy.CurrentMaxInternalChildren, Is.EqualTo(32));
            Assert.That(copy.RequestedMaxLeafKeys, Is.EqualTo(256));
            Assert.That(copy.RequestedMaxInternalChildren, Is.EqualTo(128));
        });
    }

    [Test]
    public void TreeAdminSnapshotRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminSnapshotRequest
        {
            TreeId = "orders",
            DestinationTreeId = "orders-snap",
            Mode = TreeSnapshotMode.Online,
            MaxLeafKeys = 128,
            MaxInternalChildren = 64,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.DestinationTreeId, Is.EqualTo("orders-snap"));
            Assert.That(copy.Mode, Is.EqualTo(TreeSnapshotMode.Online));
            Assert.That(copy.MaxLeafKeys, Is.EqualTo(128));
            Assert.That(copy.MaxInternalChildren, Is.EqualTo(64));
        });
    }

    [Test]
    public void TreeSnapshotStatus_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeSnapshotStatus
        {
            TreeId = "orders",
            InProgress = true,
            RequestedDestinationTreeId = "orders-snap",
            RequestedMode = TreeSnapshotMode.Offline,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.InProgress, Is.True);
            Assert.That(copy.RequestedDestinationTreeId, Is.EqualTo("orders-snap"));
            Assert.That(copy.RequestedMode, Is.EqualTo(TreeSnapshotMode.Offline));
        });
    }

    [Test]
    public void TreeAdminWalMovePlanRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminWalMovePlanRequest
        {
            TreeId = "orders",
            Partition = 3,
            TargetProviderKey = "wal-secondary",
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Partition, Is.EqualTo(3));
            Assert.That(copy.TargetProviderKey, Is.EqualTo("wal-secondary"));
        });
    }

    [Test]
    public void TreeAdminWalMoveExecuteRequest_round_trips_with_options()
    {
        var copy = RoundTrip(new TreeAdminWalMoveExecuteRequest
        {
            TreeId = "orders",
            Partition = 1,
            TargetProviderKey = "wal-secondary",
            Options = new TreeWalMoveOptions
            {
                QuiesceLeaseSeconds = 45,
                CopyPageSize = 128,
                DisableVerifyAfterCopy = true,
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Partition, Is.EqualTo(1));
            Assert.That(copy.TargetProviderKey, Is.EqualTo("wal-secondary"));
            Assert.That(copy.Options!.Value.QuiesceLeaseSeconds, Is.EqualTo(45));
            Assert.That(copy.Options!.Value.CopyPageSize, Is.EqualTo(128));
            Assert.That(copy.Options!.Value.DisableVerifyAfterCopy, Is.True);
        });
    }

    [Test]
    public void TreeAdminWalReclaimRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminWalReclaimRequest
        {
            TreeId = "orders",
            Partition = 2,
            SourceProviderKey = "wal-primary",
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Partition, Is.EqualTo(2));
            Assert.That(copy.SourceProviderKey, Is.EqualTo("wal-primary"));
        });
    }

    [Test]
    public void TreeWalPlacement_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeWalPlacement
        {
            TreeId = "orders",
            Version = 7,
            DefaultProviderKey = "wal-primary",
            Partitions = System.Collections.Immutable.ImmutableArray.Create(
                new TreeWalPartitionPlacement { Partition = 0, ProviderKey = "wal-primary", ResolvableOnThisSilo = true },
                new TreeWalPartitionPlacement { Partition = 1, ProviderKey = "wal-secondary", ResolvableOnThisSilo = false }),
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Version, Is.EqualTo(7));
            Assert.That(copy.Partitions, Has.Length.EqualTo(2));
            Assert.That(copy.Partitions[1].ProviderKey, Is.EqualTo("wal-secondary"));
            Assert.That(copy.Partitions[1].ResolvableOnThisSilo, Is.False);
        });
    }

    [Test]
    public void TreeWalPlacementAudit_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeWalPlacementAudit
        {
            TreeId = "orders",
            Version = 3,
            PartitionCount = 1,
            Partitions = System.Collections.Immutable.ImmutableArray.Create(
                new TreeWalPartitionPlacement { Partition = 0, ProviderKey = "wal-primary", ResolvableOnThisSilo = true }),
            AllResolvableOnThisSilo = false,
            KnownProviderKeys = System.Collections.Immutable.ImmutableArray.Create("wal-primary", "wal-secondary"),
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.PartitionCount, Is.EqualTo(1));
            Assert.That(copy.AllResolvableOnThisSilo, Is.False);
            Assert.That(copy.KnownProviderKeys, Is.EquivalentTo(new[] { "wal-primary", "wal-secondary" }));
        });
    }

    [Test]
    public void TreeWalMovePlan_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeWalMovePlan
        {
            TreeId = "orders",
            Partition = 1,
            FromProviderKey = "wal-primary",
            ToProviderKey = "wal-secondary",
            PlacementVersion = 9,
            SourceLowestOffset = 0,
            SourceHighestOffset = 41,
            EntriesToCopy = 42,
            TargetResolvableOnThisSilo = true,
            AlreadyAtTarget = false,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.EntriesToCopy, Is.EqualTo(42));
            Assert.That(copy.PlacementVersion, Is.EqualTo(9));
            Assert.That(copy.TargetResolvableOnThisSilo, Is.True);
        });
    }

    [Test]
    public void TreeWalMoveReceipt_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeWalMoveReceipt
        {
            TreeId = "orders",
            Partition = 1,
            FromProviderKey = "wal-primary",
            ToProviderKey = "wal-secondary",
            PreviousPlacementVersion = 4,
            NewPlacementVersion = 5,
            CopiedFromOffset = 0,
            CopiedThroughOffset = 41,
            SourceHighestOffset = 41,
            TargetHighestOffset = 41,
            SourceRetained = true,
            Outcome = TreeWalMoveOutcome.Moved,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.NewPlacementVersion, Is.EqualTo(5));
            Assert.That(copy.SourceRetained, Is.True);
            Assert.That(copy.Outcome, Is.EqualTo(TreeWalMoveOutcome.Moved));
        });
    }

    [Test]
    public void TreeAdminViewRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminViewRequest { ViewName = "orders-by-region" });

        Assert.That(copy.ViewName, Is.EqualTo("orders-by-region"));
    }

    [Test]
    public void TreeAdminViewListRequest_round_trips()
    {
        var copy = RoundTrip(new TreeAdminViewListRequest());

        Assert.That(copy, Is.Not.Null);
    }

    [Test]
    public void TreeViewCatalog_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeViewCatalog
        {
            Views = System.Collections.Immutable.ImmutableArray.Create(
                new TreeViewInfo { ViewName = "v1", SourceTreeId = "s1", IsAggregation = false, Accumulative = true },
                new TreeViewInfo { ViewName = "v2", SourceTreeId = "s2", IsAggregation = true, Accumulative = false }),
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Views, Has.Length.EqualTo(2));
            Assert.That(copy.Views[0].ViewName, Is.EqualTo("v1"));
            Assert.That(copy.Views[0].Accumulative, Is.True);
            Assert.That(copy.Views[1].IsAggregation, Is.True);
        });
    }

    [Test]
    public void TreeViewStatus_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeViewStatus
        {
            ViewName = "orders-by-region",
            SourceTreeId = "orders",
            IsAggregation = true,
            ApplyLag = 42,
            ActiveTreeId = "view-orders-by-region",
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.ViewName, Is.EqualTo("orders-by-region"));
            Assert.That(copy.SourceTreeId, Is.EqualTo("orders"));
            Assert.That(copy.IsAggregation, Is.True);
            Assert.That(copy.ApplyLag, Is.EqualTo(42));
            Assert.That(copy.ActiveTreeId, Is.EqualTo("view-orders-by-region"));
        });
    }

    [Test]
    public void TreeViewReconcileResult_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeViewReconcileResult
        {
            ViewName = "orders-by-region",
            SourceTreeId = "orders",
            DriftRepaired = true,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.ViewName, Is.EqualTo("orders-by-region"));
            Assert.That(copy.SourceTreeId, Is.EqualTo("orders"));
            Assert.That(copy.DriftRepaired, Is.True);
        });
    }

    [Test]
    public void TreeAdminTagIndexRequest_round_trips()
    {
        Assert.That(RoundTrip(new TreeAdminTagIndexRequest { IndexName = "by-tag" }).IndexName, Is.EqualTo("by-tag"));
    }

    [Test]
    public void TreeAdminTagIndexListRequest_round_trips()
    {
        Assert.That(RoundTrip(new TreeAdminTagIndexListRequest()), Is.Not.Null);
    }

    [Test]
    public void TreeTagIndexCatalog_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeTagIndexCatalog
        {
            Indexes = System.Collections.Immutable.ImmutableArray.Create(
                new TreeTagIndexInfo
                {
                    IndexName = "by-tag",
                    TreeId = "tag-by-tag",
                    ShardCount = 8,
                    CoveredTrees = System.Collections.Immutable.ImmutableArray.Create("orders", "widgets"),
                }),
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Indexes, Has.Length.EqualTo(1));
            Assert.That(copy.Indexes[0].IndexName, Is.EqualTo("by-tag"));
            Assert.That(copy.Indexes[0].TreeId, Is.EqualTo("tag-by-tag"));
            Assert.That(copy.Indexes[0].ShardCount, Is.EqualTo(8));
            Assert.That(copy.Indexes[0].CoveredTrees, Is.EquivalentTo(new[] { "orders", "widgets" }));
        });
    }

    [Test]
    public void TreeTagIndexStatus_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeTagIndexStatus
        {
            IndexName = "by-tag",
            TreeId = "tag-by-tag",
            ShardCount = 4,
            CoveredTrees = System.Collections.Immutable.ImmutableArray.Create("orders"),
            ReconcileIdle = false,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.IndexName, Is.EqualTo("by-tag"));
            Assert.That(copy.TreeId, Is.EqualTo("tag-by-tag"));
            Assert.That(copy.ShardCount, Is.EqualTo(4));
            Assert.That(copy.CoveredTrees, Is.EquivalentTo(new[] { "orders" }));
            Assert.That(copy.ReconcileIdle, Is.False);
        });
    }

    [Test]
    public void TreeTagReconcileReport_response_round_trips_through_the_marshaller()
    {
        var copy = RoundTrip(new TreeTagReconcileReport
        {
            IndexName = "by-tag",
            TreeId = "tag-by-tag",
            TreesCovered = 2,
            KeysScanned = 100,
            MembershipRowsScanned = 40,
            OrphanRowsRemoved = 3,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.IndexName, Is.EqualTo("by-tag"));
            Assert.That(copy.TreeId, Is.EqualTo("tag-by-tag"));
            Assert.That(copy.TreesCovered, Is.EqualTo(2));
            Assert.That(copy.KeysScanned, Is.EqualTo(100));
            Assert.That(copy.MembershipRowsScanned, Is.EqualTo(40));
            Assert.That(copy.OrphanRowsRemoved, Is.EqualTo(3));
        });
    }

    [Test]
    public void Every_registry_alias_is_unique_and_uses_the_reserved_prefix()
    {
        var aliases = RegistryAliasValues();

        Assert.Multiple(() =>
        {
            Assert.That(GrpcTreeAdminTypeAliases.AliasPrefix, Is.EqualTo("oitg."));
            Assert.That(aliases, Is.Unique);
            Assert.That(aliases, Is.All.StartsWith(GrpcTreeAdminTypeAliases.AliasPrefix));
        });
    }

    [Test]
    public void Every_grpc_wire_message_carries_a_unique_registry_alias()
    {
        var registry = new HashSet<string>(RegistryAliasValues(), StringComparer.Ordinal);

        var wireMessages = typeof(GrpcTreeAdminTypeAliases).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null)
            .Where(t => t.GetCustomAttribute<AliasAttribute>()?.Alias
                is { } alias && alias.StartsWith(GrpcTreeAdminTypeAliases.AliasPrefix, StringComparison.Ordinal))
            .ToList();

        var seen = new HashSet<string>(StringComparer.Ordinal);
        Assert.That(wireMessages, Is.Not.Empty);
        foreach (var type in wireMessages)
        {
            var alias = type.GetCustomAttribute<AliasAttribute>()!.Alias;
            Assert.Multiple(() =>
            {
                Assert.That(registry, Does.Contain(alias), $"{type.Name} alias '{alias}' is not in GrpcTreeAdminTypeAliases.");
                Assert.That(seen.Add(alias), Is.True, $"Alias '{alias}' is used by more than one wire message.");
            });
        }
    }

    private static IReadOnlyList<string> RegistryAliasValues() =>
        typeof(GrpcTreeAdminTypeAliases)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Where(f => f.Name != nameof(GrpcTreeAdminTypeAliases.AliasPrefix))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
}
