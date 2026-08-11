using System.Collections.Immutable;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTreeAdmin"/>: the tree-administration facade
/// composes the schema control facade (<see cref="ILatticeSchemaControl"/>) for the
/// capability probe and wraps the public grain surface (<see cref="ILattice"/>,
/// <see cref="ILatticeAdmin"/>) for the read-only diagnostics operations, gating each
/// read through the shared fail-closed access gate before dialing the grain. Driven
/// purely with substitutes and a hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminTests
{
    private const string Tree = "orders";

    /// <summary>A hand-written gate that uniformly allows or denies, avoiding NSubstitute's awkward <c>in</c>-parameter mocking.</summary>
    private sealed class FixedGate : ILatticeAccessGate
    {
        private readonly bool _allow;
        public FixedGate(bool allow) => _allow = allow;

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static LatticeTreeAdmin Create(
        ILatticeSchemaControl schemaControl,
        IGrainFactory? grainFactory = null,
        bool allow = true)
        => new(
            schemaControl,
            grainFactory ?? Substitute.For<IGrainFactory>(),
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()));

    private static LatticeSchemaCapabilities SchemaCaps(string tree, bool granted) => new()
    {
        TreeId = tree,
        CanViewPolicy = granted,
        CanViewDeadLetters = granted,
        CanViewVersionConfig = granted,
        CanViewRemediationStatus = granted,
        CanScanCompliance = granted,
        CanManagePolicy = granted,
        CanManageVersion = granted,
        CanRemediate = granted,
    };

    private static ILattice Lattice(IGrainFactory factory, string tree = Tree)
    {
        var lattice = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(tree).Returns(lattice);
        return lattice;
    }

    private static TreeDiagnosticReport CoreDiagnostics(bool deep = false) => new()
    {
        TreeId = Tree,
        ShardCount = 2,
        VirtualShardCount = 8,
        TotalLiveKeys = 30,
        TotalTombstones = 4,
        Deep = deep,
        SampledAt = DateTimeOffset.UnixEpoch,
        RecentSplits = ImmutableArray.Create(new RecentSplit { ShardIndex = 0, AtUtc = DateTime.UnixEpoch }),
        Shards = ImmutableArray.Create(
            new ShardDiagnosticReport
            {
                ShardIndex = 0,
                Depth = 2,
                RootIsLeaf = false,
                LiveKeys = 20,
                Tombstones = 3,
                TombstoneRatio = 0.13,
                OpsPerSecond = 12.5,
                Reads = 100,
                Writes = 25,
                HotnessWindow = TimeSpan.FromSeconds(10),
                SplitInProgress = false,
                BulkOperationPending = false,
            },
            new ShardDiagnosticReport
            {
                ShardIndex = 1,
                Depth = 1,
                RootIsLeaf = true,
                LiveKeys = 10,
                Tombstones = 1,
                TombstoneRatio = 0.09,
                OpsPerSecond = 4.0,
                Reads = 30,
                Writes = 10,
                HotnessWindow = TimeSpan.FromSeconds(10),
                SplitInProgress = true,
                BulkOperationPending = true,
            }),
    };

    // ----- ProbeCapabilities -----

    [Test]
    public async Task ProbeCapabilitiesAsync_delegates_to_schema_control_and_composes_result()
    {
        var schemaControl = Substitute.For<ILatticeSchemaControl>();
        var schemaCaps = SchemaCaps(Tree, granted: true);
        schemaControl.ProbeCapabilitiesAsync(Tree, Arg.Any<CancellationToken>()).Returns(schemaCaps);
        var facade = Create(schemaControl, allow: true);

        var caps = await facade.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(caps.TreeId, Is.EqualTo(Tree));
            Assert.That(caps.Schema, Is.SameAs(schemaCaps));
            Assert.That(caps.CanAdministerTree, Is.True);
            // The read gate allows, so the diagnostics capability probe reports true.
            Assert.That(caps.CanViewDiagnostics, Is.True);
        });
        await schemaControl.Received(1).ProbeCapabilitiesAsync(Tree, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_reports_diagnostics_denied_when_read_gate_denies()
    {
        var schemaControl = Substitute.For<ILatticeSchemaControl>();
        schemaControl.ProbeCapabilitiesAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(SchemaCaps(Tree, granted: false));
        var facade = Create(schemaControl, allow: false);

        var caps = await facade.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(caps.CanViewDiagnostics, Is.False);
            Assert.That(caps.CanAdministerTree, Is.False);
            Assert.That(caps.Schema.CanViewPolicy, Is.False);
        });
    }

    [Test]
    public void ProbeCapabilitiesAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<ILatticeSchemaControl>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.ProbeCapabilitiesAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.ProbeCapabilitiesAsync(""), Throws.ArgumentException);
        });
    }

    // ----- GetShardHotness -----

    [Test]
    public async Task GetShardHotnessAsync_projects_per_shard_counters_and_totals()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Lattice(factory);
        lattice.DiagnoseAsync(false, Arg.Any<CancellationToken>()).Returns(CoreDiagnostics());
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), factory);

        var report = await facade.GetShardHotnessAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.ShardCount, Is.EqualTo(2));
            Assert.That(report.Shards, Has.Length.EqualTo(2));
            Assert.That(report.TotalReads, Is.EqualTo(130));
            Assert.That(report.TotalWrites, Is.EqualTo(35));
            Assert.That(report.TotalOpsPerSecond, Is.EqualTo(16.5));
            Assert.That(report.Shards[0].ShardIndex, Is.EqualTo(0));
            Assert.That(report.Shards[0].WindowSeconds, Is.EqualTo(10));
            Assert.That(report.Shards[0].OpsPerSecond, Is.EqualTo(12.5));
        });
    }

    [Test]
    public void GetShardHotnessAsync_denied_by_read_gate_throws_and_does_not_dial_grain()
    {
        var factory = Substitute.For<IGrainFactory>();
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), factory, allow: false);

        Assert.That(async () => await facade.GetShardHotnessAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        factory.DidNotReceive().GetGrain<ILattice>(Arg.Any<string>());
    }

    [Test]
    public void GetShardHotnessAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<ILatticeSchemaControl>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.GetShardHotnessAsync(null!), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.GetShardHotnessAsync(""), Throws.ArgumentException);
        });
    }

    // ----- GetDiagnostics -----

    [Test]
    public async Task GetDiagnosticsAsync_maps_every_shard_field_and_rollups()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Lattice(factory);
        lattice.DiagnoseAsync(true, Arg.Any<CancellationToken>()).Returns(CoreDiagnostics(deep: true));
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), factory);

        var report = await facade.GetDiagnosticsAsync(Tree, deep: true);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.ShardCount, Is.EqualTo(2));
            Assert.That(report.VirtualShardCount, Is.EqualTo(8));
            Assert.That(report.TotalLiveKeys, Is.EqualTo(30));
            Assert.That(report.TotalTombstones, Is.EqualTo(4));
            Assert.That(report.Deep, Is.True);
            Assert.That(report.RecentSplitCount, Is.EqualTo(1));
            Assert.That(report.Shards, Has.Length.EqualTo(2));
            Assert.That(report.Shards[1].RootIsLeaf, Is.True);
            Assert.That(report.Shards[1].SplitInProgress, Is.True);
            Assert.That(report.Shards[1].BulkOperationPending, Is.True);
            Assert.That(report.Shards[0].WindowSeconds, Is.EqualTo(10));
        });
    }

    [Test]
    public async Task GetDiagnosticsAsync_defaults_to_cheap_projection()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Lattice(factory);
        lattice.DiagnoseAsync(false, Arg.Any<CancellationToken>()).Returns(CoreDiagnostics(deep: false));
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), factory);

        await facade.GetDiagnosticsAsync(Tree);

        await lattice.Received(1).DiagnoseAsync(false, Arg.Any<CancellationToken>());
    }

    [Test]
    public void GetDiagnosticsAsync_denied_by_read_gate_throws()
    {
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), allow: false);

        Assert.That(async () => await facade.GetDiagnosticsAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ----- InspectShardMap -----

    [Test]
    public async Task InspectShardMapAsync_summarises_topology_with_distinct_sorted_physical_shards()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Lattice(factory);
        var map = new ShardMap { Slots = new[] { 1, 0, 1, 0 }, Version = 5 };
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>()).Returns(new RoutingInfo("phys-orders", map));
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), factory);

        var inspection = await facade.InspectShardMapAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(inspection.TreeId, Is.EqualTo(Tree));
            Assert.That(inspection.PhysicalTreeId, Is.EqualTo("phys-orders"));
            Assert.That(inspection.VirtualShardCount, Is.EqualTo(4));
            Assert.That(inspection.PhysicalShardCount, Is.EqualTo(2));
            Assert.That(inspection.MapVersion, Is.EqualTo(5));
            Assert.That(inspection.PhysicalShardIndices, Is.EqualTo(new[] { 0, 1 }));
        });
    }

    [Test]
    public void InspectShardMapAsync_denied_by_read_gate_throws()
    {
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), allow: false);

        Assert.That(async () => await facade.InspectShardMapAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ----- GetProjectionDigest -----

    [Test]
    public async Task GetProjectionDigestAsync_hex_encodes_the_hash_and_carries_counts()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Lattice(factory);
        lattice.GetLeafProjectionDigestAsync(1, Arg.Any<CancellationToken>())
            .Returns(new LeafProjectionDigest
            {
                Hash = new byte[] { 0xAB, 0xCD, 0x01 },
                EntryCount = 42,
                CheckpointOffset = 900,
                Version = 7,
            });
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), factory);

        var digest = await facade.GetProjectionDigestAsync(Tree, 1);

        Assert.Multiple(() =>
        {
            Assert.That(digest.TreeId, Is.EqualTo(Tree));
            Assert.That(digest.ShardIndex, Is.EqualTo(1));
            Assert.That(digest.HashHex, Is.EqualTo("abcd01"));
            Assert.That(digest.EntryCount, Is.EqualTo(42));
            Assert.That(digest.CheckpointOffset, Is.EqualTo(900));
            Assert.That(digest.Version, Is.EqualTo(7));
        });
    }

    [Test]
    public void GetProjectionDigestAsync_negative_shard_index_throws()
    {
        var facade = Create(Substitute.For<ILatticeSchemaControl>());

        Assert.That(async () => await facade.GetProjectionDigestAsync(Tree, -1),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    // ----- GetTreeStats -----

    [Test]
    public async Task GetTreeStatsAsync_joins_diagnostics_and_storage()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Lattice(factory);
        lattice.DiagnoseAsync(false, Arg.Any<CancellationToken>()).Returns(CoreDiagnostics());
        lattice.GetStorageUsageAsync(Arg.Any<CancellationToken>()).Returns(new TreeStorageUsageReport
        {
            TreeId = Tree,
            WalRetainedBytes = 100,
            SnapshotBytes = 200,
            LeafStateBytes = 300,
            TotalBytes = 600,
            Partial = true,
            SampledAt = DateTimeOffset.UnixEpoch,
            LiveKeys = 30,
        });
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), factory);

        var stats = await facade.GetTreeStatsAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(stats.TreeId, Is.EqualTo(Tree));
            Assert.That(stats.ShardCount, Is.EqualTo(2));
            Assert.That(stats.VirtualShardCount, Is.EqualTo(8));
            Assert.That(stats.TotalLiveKeys, Is.EqualTo(30));
            Assert.That(stats.TotalTombstones, Is.EqualTo(4));
            Assert.That(stats.LeafStateBytes, Is.EqualTo(300));
            Assert.That(stats.SnapshotBytes, Is.EqualTo(200));
            Assert.That(stats.WalRetainedBytes, Is.EqualTo(100));
            Assert.That(stats.TotalBytes, Is.EqualTo(600));
            Assert.That(stats.PartialStorage, Is.True);
        });
    }

    [Test]
    public void GetTreeStatsAsync_denied_by_read_gate_throws()
    {
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), allow: false);

        Assert.That(async () => await facade.GetTreeStatsAsync(Tree),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ----- GetStorageUsage -----

    [Test]
    public async Task GetStorageUsageAsync_cheap_path_uses_cached_aggregate_and_maps_trees()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = Substitute.For<ILatticeAdmin>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>()).Returns(new ClusterStorageUsageReport
        {
            TreeCount = 1,
            WalRetainedBytes = 10,
            SnapshotBytes = 20,
            LeafStateBytes = 30,
            TotalBytes = 60,
            Partial = false,
            SampledAt = DateTimeOffset.UnixEpoch,
            Trees = ImmutableArray.Create(new TreeStorageUsageReport
            {
                TreeId = Tree,
                WalRetainedBytes = 10,
                SnapshotBytes = 20,
                LeafStateBytes = 30,
                TotalBytes = 60,
                Partial = false,
                SampledAt = DateTimeOffset.UnixEpoch,
                LiveKeys = 5,
            }),
        });
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), factory);

        var summary = await facade.GetStorageUsageAsync(deep: false);

        Assert.Multiple(() =>
        {
            Assert.That(summary.TreeCount, Is.EqualTo(1));
            Assert.That(summary.TotalBytes, Is.EqualTo(60));
            Assert.That(summary.Deep, Is.False);
            Assert.That(summary.Trees, Has.Length.EqualTo(1));
            Assert.That(summary.Trees[0].TreeId, Is.EqualTo(Tree));
            Assert.That(summary.Trees[0].LiveKeys, Is.EqualTo(5));
        });
        await admin.Received(1).GetTotalStorageUsageAsync(Arg.Any<CancellationToken>());
        await admin.DidNotReceive().RefreshStorageUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetStorageUsageAsync_deep_path_forces_a_refresh_and_flags_deep()
    {
        var factory = Substitute.For<IGrainFactory>();
        var admin = Substitute.For<ILatticeAdmin>();
        factory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey).Returns(admin);
        admin.RefreshStorageUsageAsync(Arg.Any<CancellationToken>()).Returns(new ClusterStorageUsageReport
        {
            TreeCount = 0,
            Trees = ImmutableArray<TreeStorageUsageReport>.Empty,
            SampledAt = DateTimeOffset.UnixEpoch,
        });
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), factory);

        var summary = await facade.GetStorageUsageAsync(deep: true);

        Assert.Multiple(() =>
        {
            Assert.That(summary.Deep, Is.True);
            Assert.That(summary.TreeCount, Is.EqualTo(0));
            Assert.That(summary.Trees, Is.Empty);
        });
        await admin.Received(1).RefreshStorageUsageAsync(Arg.Any<CancellationToken>());
        await admin.DidNotReceive().GetTotalStorageUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void GetStorageUsageAsync_denied_by_telemetry_gate_throws()
    {
        var facade = Create(Substitute.For<ILatticeSchemaControl>(), allow: false);

        Assert.That(async () => await facade.GetStorageUsageAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ----- Constructor guards -----

    [Test]
    public void Constructor_null_dependencies_throw()
    {
        var schemaControl = Substitute.For<ILatticeSchemaControl>();
        var factory = Substitute.For<IGrainFactory>();
        var authorizer = new TreeAdminAccessAuthorizer(new FixedGate(true));
        var options = Options.Create(new LatticeApiTreeAdminOptions());

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeTreeAdmin(null!, factory, authorizer, options), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTreeAdmin(schemaControl, null!, authorizer, options), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTreeAdmin(schemaControl, factory, null!, options), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTreeAdmin(schemaControl, factory, authorizer, null!), Throws.ArgumentNullException);
        });
    }
}
