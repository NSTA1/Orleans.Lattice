using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

[TestFixture]
public sealed class TenantRegionBackfillServiceTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public async Task AdvanceTenantAsync_discovers_runtime_tree_enrolments_and_waits_for_verified_bootstrap()
    {
        const string treeId = "t/acme/orders";
        var registry = new FakeTenantRegistry();
        registry.Seed(TenantRecordForBackfill());
        var treeRegistry = Substitute.For<ILatticeRegistry>();
        treeRegistry.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Returns(Task.FromResult<IReadOnlyList<string>>([]));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(treeRegistry);

        var replication = Substitute.For<ILatticeReplicationConfigAuthority>();
        replication.GetAllTreeStatusesAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyDictionary<string, LatticeReplicationTreeStatus>>(
                new Dictionary<string, LatticeReplicationTreeStatus>(StringComparer.Ordinal)
                {
                    [treeId] = new(treeId, Enabled: true, LatticeMergeMode.LwwRegister, Ambiguous: false),
                }));
        var replicationContext = Substitute.For<ILatticeReplicationContext>();
        replicationContext.ResolveMergeMode(treeId).Returns(LatticeMergeMode.LwwRegister);

        var bootstrap = Substitute.For<ILatticeBootstrapCoordinator>();
        var bootstrapStatus = new BootstrapCoordinatorStatus(LatticeBootstrapState.ApplyingSnapshot, "east")
        {
            ReadFenced = true,
            EntriesApplied = 12,
        };
        bootstrap.GetStatusAsync(treeId, Arg.Any<CancellationToken>()).Returns(_ => bootstrapStatus);

        var deadLetters = Substitute.For<ILatticeReplicationDeadLetters>();
        deadLetters.ListAsync(treeId, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<DeadLetterEntry>>([]));
        var driver = new TenantRegionLifecycleDriver(
            registry,
            Options.Create(new ClusterOptions { ClusterId = "west" }));
        var service = new TenantRegionBackfillService(
            registry,
            driver,
            grainFactory,
            Options.Create(new ClusterOptions { ClusterId = "west" }),
            Substitute.For<ILogger<TenantRegionBackfillService>>(),
            bootstrap,
            deadLetters,
            replicationContext: replicationContext,
            replicationConfigAuthority: replication);

        await service.AdvanceTenantAsync(Acme, "east", CancellationToken.None);

        Assert.That(registry.Peek(Acme.Value)!.GetRegionStatus("west"), Is.EqualTo(TenantRegionStatus.Backfilling),
            "a tree known through the replicated runtime config must not be mistaken for an empty local registry");

        bootstrapStatus = new BootstrapCoordinatorStatus(LatticeBootstrapState.LiveIncremental, "east")
        {
            ReadFenced = false,
            EntriesApplied = 20,
        };
        await service.AdvanceTenantAsync(Acme, "east", CancellationToken.None);

        Assert.That(registry.Peek(Acme.Value)!.GetRegionStatus("west"), Is.EqualTo(TenantRegionStatus.Online));
    }

    [Test]
    public async Task AdvanceTenantAsync_discovers_static_tree_enrolments_before_the_local_tree_exists()
    {
        const string treeId = "t/acme/orders";
        var registry = new FakeTenantRegistry();
        registry.Seed(TenantRecordForBackfill());
        var treeRegistry = Substitute.For<ILatticeRegistry>();
        treeRegistry.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Returns(Task.FromResult<IReadOnlyList<string>>([]));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(treeRegistry);
        var replicationOptions = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        replicationOptions.CurrentValue.Returns(new LatticeReplicationOptions
        {
            ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                [treeId] = LatticeMergeMode.LwwRegister,
            },
        });
        var bootstrap = Substitute.For<ILatticeBootstrapCoordinator>();
        bootstrap.GetStatusAsync(treeId, Arg.Any<CancellationToken>())
            .Returns(new BootstrapCoordinatorStatus(LatticeBootstrapState.Idle, null));
        var replicationContext = Substitute.For<ILatticeReplicationContext>();
        replicationContext.ResolveMergeMode(treeId).Returns(LatticeMergeMode.LwwRegister);
        var deadLetters = Substitute.For<ILatticeReplicationDeadLetters>();
        deadLetters.ListAsync(treeId, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<DeadLetterEntry>>([]));
        var driver = new TenantRegionLifecycleDriver(
            registry,
            Options.Create(new ClusterOptions { ClusterId = "west" }));
        var service = new TenantRegionBackfillService(
            registry,
            driver,
            grainFactory,
            Options.Create(new ClusterOptions { ClusterId = "west" }),
            Substitute.For<ILogger<TenantRegionBackfillService>>(),
            bootstrap,
            deadLetters,
            replicationOptions,
            replicationContext);

        await service.AdvanceTenantAsync(Acme, "east", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(registry.Peek(Acme.Value)!.GetRegionStatus("west"), Is.EqualTo(TenantRegionStatus.Backfilling),
                "a statically enrolled remote tree must not be mistaken for an empty tenant");
            Assert.That(bootstrap.ReceivedCalls(), Is.Not.Empty,
                "the discovered remote tree must enter the receiver bootstrap path");
        });
    }

    [Test]
    public async Task AdvanceTenantAsync_reseeds_from_a_new_online_source_before_promotion()
    {
        const string treeId = "t/acme/orders";
        var record = TenantRecordForBackfill(TenantRegionStatus.Backfilling);
        record.AuthorizeRegion("north", new() { WallClockTicks = 6 }, "seed");
        record.SetRegionStatus("north", TenantRegionStatus.Online, new() { WallClockTicks = 7 }, "seed");
        record.SetRegionStatus("east", TenantRegionStatus.Offline, new() { WallClockTicks = 8 }, "seed");
        var registry = new FakeTenantRegistry();
        registry.Seed(record);
        var treeRegistry = Substitute.For<ILatticeRegistry>();
        treeRegistry.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Returns(Task.FromResult<IReadOnlyList<string>>([treeId]));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(treeRegistry);
        var replicationContext = Substitute.For<ILatticeReplicationContext>();
        replicationContext.ResolveMergeMode(treeId).Returns(LatticeMergeMode.LwwRegister);
        var bootstrap = Substitute.For<ILatticeBootstrapCoordinator>();
        var bootstrapStatus = new BootstrapCoordinatorStatus(LatticeBootstrapState.LiveIncremental, null)
        {
            CompletedSourceClusterId = "east",
        };
        bootstrap.GetStatusAsync(treeId, Arg.Any<CancellationToken>())
            .Returns(_ => bootstrapStatus);
        bootstrap.BootstrapAsync(treeId, "north", Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                bootstrapStatus = new BootstrapCoordinatorStatus(LatticeBootstrapState.RequestingSnapshot, "north");
                return Task.CompletedTask;
            });
        var deadLetters = Substitute.For<ILatticeReplicationDeadLetters>();
        deadLetters.ListAsync(treeId, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<DeadLetterEntry>>([]));
        var clusterOptions = Options.Create(new ClusterOptions { ClusterId = "west" });
        var service = new TenantRegionBackfillService(
            registry,
            new TenantRegionLifecycleDriver(registry, clusterOptions),
            grainFactory,
            clusterOptions,
            Substitute.For<ILogger<TenantRegionBackfillService>>(),
            bootstrap,
            deadLetters,
            replicationContext: replicationContext);

        var progress = await service.GetProgressAsync(Acme, "west");
        Assert.Multiple(() =>
        {
            Assert.That(progress.Phase, Is.EqualTo("Blocked"));
            Assert.That(progress.StallReason, Does.Contain("east").And.Contain("no longer Online"));
        });

        await service.AdvanceTenantAsync(Acme, "north", CancellationToken.None);
        Assert.That(registry.Peek(Acme.Value)!.GetRegionStatus("west"), Is.EqualTo(TenantRegionStatus.Backfilling));
        await bootstrap.Received(1).BootstrapAsync(treeId, "north", Arg.Any<CancellationToken>());

        bootstrapStatus = new BootstrapCoordinatorStatus(LatticeBootstrapState.LiveIncremental, null)
        {
            CompletedSourceClusterId = "north",
        };
        await service.AdvanceTenantAsync(Acme, "north", CancellationToken.None);

        Assert.That(registry.Peek(Acme.Value)!.GetRegionStatus("west"), Is.EqualTo(TenantRegionStatus.Online));
    }

    [Test]
    public async Task AdvanceTenantAsync_does_not_promote_when_parked_tenant_entries_are_deferred()
    {
        const string treeId = "t/acme/orders";
        var registry = new FakeTenantRegistry();
        registry.Seed(TenantRecordForBackfill(TenantRegionStatus.Backfilling));
        var treeRegistry = Substitute.For<ILatticeRegistry>();
        treeRegistry.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Returns(Task.FromResult<IReadOnlyList<string>>([treeId]));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(treeRegistry);
        var replicationContext = Substitute.For<ILatticeReplicationContext>();
        replicationContext.ResolveMergeMode(treeId).Returns(LatticeMergeMode.LwwRegister);
        var bootstrap = Substitute.For<ILatticeBootstrapCoordinator>();
        bootstrap.GetStatusAsync(treeId, Arg.Any<CancellationToken>())
            .Returns(new BootstrapCoordinatorStatus(LatticeBootstrapState.LiveIncremental, "east"));
        var deadLetters = Substitute.For<ILatticeReplicationDeadLetters>();
        var parked = new DeadLetterEntry { EntryId = 1, ReasonTag = LatticeReplicationMetrics.ReasonTenantOffline };
        deadLetters.ListAsync(treeId, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<DeadLetterEntry>>([parked]));
        deadLetters.ReplayAsync(treeId, parked.EntryId, Arg.Any<CancellationToken>())
            .Returns(new ApplyResult { Deferred = true });
        var driver = new TenantRegionLifecycleDriver(
            registry,
            Options.Create(new ClusterOptions { ClusterId = "west" }));
        var service = new TenantRegionBackfillService(
            registry,
            driver,
            grainFactory,
            Options.Create(new ClusterOptions { ClusterId = "west" }),
            Substitute.For<ILogger<TenantRegionBackfillService>>(),
            bootstrap,
            deadLetters,
            replicationContext: replicationContext);

        await service.AdvanceTenantAsync(Acme, "east", CancellationToken.None);

        Assert.That(registry.Peek(Acme.Value)!.GetRegionStatus("west"), Is.EqualTo(TenantRegionStatus.Backfilling));
    }

    private static TenantRecord TenantRecordForBackfill(TenantRegionStatus targetStatus = TenantRegionStatus.Provisioning)
    {
        static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

        var record = TenantRecord.Create(
            Acme,
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Stamp(1),
            "seed");
        record.AuthorizeRegion("east", Stamp(2), "seed");
        record.SetRegionStatus("east", TenantRegionStatus.Online, Stamp(3), "seed");
        record.AuthorizeRegion("west", Stamp(4), "seed");
        record.SetRegionStatus("west", targetStatus, Stamp(5), "seed");
        return record;
    }
}
