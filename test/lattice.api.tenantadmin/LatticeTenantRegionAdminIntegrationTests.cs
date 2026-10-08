using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Tenancy;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// End-to-end integration coverage for the T20 per-tenant region-residency control
/// facade (<see cref="ILatticeTenantRegionAdmin"/>) and its system-driven promotion
/// driver, composed over a real single-silo cluster with the auth and tenancy
/// add-ons. It drives the full lifecycle through the public facade: an operator
/// authorizes a tenant's allowed regions, a tenant admin sets residency (a region
/// begins <see cref="TenantRegionLifecycleStatus.Provisioning"/>), the driver
/// advances it through <see cref="TenantRegionLifecycleStatus.Backfilling"/> to
/// <see cref="TenantRegionLifecycleStatus.Online"/> (add path), then residency is
/// narrowed and the dropped region drains through
/// <see cref="TenantRegionLifecycleStatus.Offline"/> to
/// <see cref="TenantRegionLifecycleStatus.Removed"/> (remove path). It also pins
/// the last-resident-region guard, the exception types the transport bindings map
/// to specific statuses (so a typed catch arm can never go dead and surface an
/// opaque fault, as in #1697), and, load-bearing for security, proves both
/// authorization tiers are fail-closed end-to-end through the real gate under
/// <c>DefaultEffect = Allow</c>: an unauthenticated caller is denied the
/// operator-only allowed-set operation and the operator-or-tenant-admin residency
/// and status operations alike, even though the data plane defaults to allow. The
/// trusted co-host (system-origin) path stands in for authenticated infrastructure
/// so the lifecycle runs without a wire identity. Only the automatic drain
/// completion (issue #3897) is asynchronous, and its test polls the committed
/// record against a deadline rather than sleeping for a fixed interval.
/// </summary>
/// <remarks>
/// Owned by the epic coordinator's integration run; not exercised in the T20
/// unit-only pass.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTenantRegionAdminIntegrationTests
{
    private const string Operator = "root";
    private const string TenantAdmin = "alice";
    private const string RegionA = "us-east";
    private const string RegionB = "us-west";
    private const string BackfillTree = "t/backfill-data/orders";

    private readonly FacadeClusterFixture _fixture = new();

    private ILatticeTenantRegionAdmin Facade =>
        _fixture.SiloServices.GetRequiredService<ILatticeTenantRegionAdmin>();

    private TenantRegionLifecycleDriver Driver =>
        _fixture.SiloServices.GetRequiredService<TenantRegionLifecycleDriver>();

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task Add_path_auto_promotes_an_empty_tenant_without_an_operator_step()
    {
        var tenant = TenantId.Parse("add-path");
        await SeedTenantAsync(tenant);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA });

            var change = await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA });
            Assert.That(change.AddedRegions, Does.Contain(RegionA), "the newly-resident region begins adding");

        }

        var online = await WaitForStatusAsync(tenant, RegionA, TenantRegionLifecycleStatus.Online);
        Assert.That(online, Is.EqualTo(TenantRegionLifecycleStatus.Online),
            "the empty-tenant fast path must complete automatically, without an operator promotion call");
    }

    [Test]
    public async Task Existing_tenant_data_is_backfilled_before_the_new_region_becomes_online()
    {
        var tenant = TenantId.Parse("backfill-data");
        await SeedTenantAsync(tenant, RegionB);
        using (LatticeSystemOrigin.Enter())
        using (LatticeActiveTenantContext.With(tenant))
        {
            var targetTree = _fixture.SiloServices.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(BackfillTree);
            await targetTree.SetAsync("existing-1", Bytes("stale-one"));
            await targetTree.SetAsync("existing-2", Bytes("stale-two"));
        }

        FacadeClusterFixture.FixtureBootstrapSnapshotSource.Arm(BackfillTree);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA, RegionB });
            var change = await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA, RegionB });
            Assert.That(change.AddedRegions, Does.Contain(RegionA));
        }

        try
        {
            await FacadeClusterFixture.FixtureBootstrapSnapshotSource.FirstRowAppliedAsync(TimeSpan.FromSeconds(30));

            using (LatticeSystemOrigin.Enter())
            {
                var report = await Facade.GetTenantRegionStatusAsync(tenant.Value);
                var backfilling = report.Regions.Single(r => r.RegionId == RegionA);
                Assert.That(backfilling.Status, Is.EqualTo(TenantRegionLifecycleStatus.Backfilling),
                    "the region must not become Online while the snapshot is only partly applied");
            }

            var gate = _fixture.SiloServices.GetRequiredService<ILatticeAccessGate>();
            using (LatticeActiveTenantContext.With(tenant))
            {
                var decision = await gate.AuthorizeAsync(new LatticeAccessRequest(
                    BackfillTree,
                    LatticeOperation.Read,
                    new LatticeSubject(TenantAdmin),
                    "existing-1"));
                Assert.That(decision.Allowed, Is.False,
                    "tenant client reads remain refused while the region is Backfilling");
            }
        }
        finally
        {
            FacadeClusterFixture.FixtureBootstrapSnapshotSource.Release();
        }

        var online = await WaitForStatusAsync(tenant, RegionA, TenantRegionLifecycleStatus.Online);
        using (LatticeSystemOrigin.Enter())
        {
            var report = await Facade.GetTenantRegionStatusAsync(tenant.Value);
            var progress = report.Regions.Single(r => r.RegionId == RegionA).BackfillProgress;
            Assert.That(online, Is.EqualTo(TenantRegionLifecycleStatus.Online),
                $"Backfill progress: phase={progress?.Phase}; stall={progress?.StallReason}; "
                + $"trees={string.Join(", ", progress?.Trees.Select(t => $"{t.TreeId}:{t.Phase}:{t.SourceClusterId}:{t.ReadFenced}") ?? [])}");
        }

        using (LatticeSystemOrigin.Enter())
        {
            var tree = _fixture.SiloServices.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(BackfillTree);
            var values = await tree.GetManyAsync(["existing-1", "existing-2"]);
            Assert.Multiple(() =>
            {
                Assert.That(values["existing-1"], Is.EqualTo(Bytes("replicated-one")));
                Assert.That(values["existing-2"], Is.EqualTo(Bytes("replicated-two")));
            });
        }
    }

    [Test]
    public async Task Remove_path_drains_then_completes_to_removed_through_the_facade_and_driver()
    {
        var tenant = TenantId.Parse("remove-path");
        await SeedTenantAsync(tenant);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA, RegionB });
            await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA, RegionB });

            // Narrow residency to RegionA: RegionB begins draining.
            var change = await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA });
            Assert.That(change.RemovedRegions, Does.Contain(RegionB), "the dropped region begins draining");

            // Drain completion: Draining -> Offline -> Removed.
            Assert.That(await Driver.CompleteDrainStepAsync(tenant, RegionB), Is.EqualTo(TenantRegionStatus.Offline));
            Assert.That(await Driver.CompleteDrainStepAsync(tenant, RegionB), Is.EqualTo(TenantRegionStatus.Removed));

            var report = await Facade.GetTenantRegionStatusAsync(tenant.Value);
            var row = report.Regions.Single(r => r.RegionId == RegionB);
            Assert.That(row.Status, Is.EqualTo(TenantRegionLifecycleStatus.Removed), "the region is removed after drain");
        }
    }

    [Test]
    public async Task Draining_the_local_region_completes_to_removed_with_no_explicit_driver_call()
    {
        // Issue #3897: nothing drove the lifecycle, so a dropped region sat at
        // Draining for good. The silo now completes the drain of its own serving
        // region automatically, one legal step at a time, off the residency
        // snapshot's change notifications.
        var tenant = TenantId.Parse("auto-drain");
        await SeedTenantAsync(tenant);
        var localRegion = _fixture.LocalRegionId;

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { localRegion, RegionB });
            await Facade.SetResidencyAsync(tenant.Value, new[] { localRegion, RegionB });

            var change = await Facade.SetResidencyAsync(tenant.Value, new[] { RegionB });
            Assert.That(change.RemovedRegions, Does.Contain(localRegion), "the local region begins draining");

            var local = await WaitForStatusAsync(tenant, localRegion, TenantRegionLifecycleStatus.Removed);
            var remote = (await Facade.GetTenantRegionStatusAsync(tenant.Value)).Regions.Single(r => r.RegionId == RegionB);

            Assert.Multiple(() =>
            {
                Assert.That(local, Is.EqualTo(TenantRegionLifecycleStatus.Removed), "the local drain completes on its own");
                Assert.That(
                    remote.Status,
                    Is.EqualTo(TenantRegionLifecycleStatus.Provisioning),
                    "this single-silo fixture has no lifecycle driver running in the remote region");
            });
        }
    }

    [Test]
    public async Task Set_residency_to_empty_is_refused_by_the_last_resident_region_guard()
    {
        var tenant = TenantId.Parse("last-region");
        await SeedTenantAsync(tenant);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA });
            await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA });

            Assert.That(
                async () => await Facade.SetResidencyAsync(tenant.Value, Array.Empty<string>()),
                Throws.TypeOf<TenantLastRegionException>(),
                "the last resident region can never be removed");
        }
    }

    [Test]
    public async Task Operator_authorization_is_denied_for_an_unauthenticated_caller_under_default_allow()
    {
        var tenant = TenantId.Parse("guarded");
        await SeedTenantAsync(tenant);

        // No system-origin bypass and no authenticated caller: the operator tier must
        // fail closed even though the data plane defaults to allow.
        Assert.That(
            async () => await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA }),
            Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "authorizing a tenant's allowed region set requires platform-operator authority");
    }

    [Test]
    public async Task Residency_and_status_are_denied_for_an_unauthenticated_caller_under_default_allow()
    {
        var tenant = TenantId.Parse("guarded-tenant-tier");
        await SeedTenantAsync(tenant);

        // The tenant-admin tier is operator-or-tenant-admin; an anonymous caller is
        // neither, so both operations fail closed under DefaultEffect = Allow. The
        // transport bindings inherit exactly this gate - they must not widen it.
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA }),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await Facade.GetTenantRegionStatusAsync(tenant.Value),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }

    [Test]
    public void Region_operations_on_an_unknown_tenant_raise_the_status_mapped_not_found_exception()
    {
        using (LatticeSystemOrigin.Enter())
        {
            // Pins the exception type the transport bindings map to NotFound. If the
            // facade ever raised a different type the binding's typed arm would go
            // dead and the fault would surface as an opaque Internal (issue #1697).
            Assert.Multiple(() =>
            {
                Assert.That(
                    async () => await Facade.AuthorizeAllowedRegionsAsync("no-such-tenant", new[] { RegionA }),
                    Throws.TypeOf<TenantNotFoundException>());
                Assert.That(
                    async () => await Facade.GetTenantRegionStatusAsync("no-such-tenant"),
                    Throws.TypeOf<TenantNotFoundException>());
            });
        }
    }

    [Test]
    public async Task Residency_outside_the_allowed_set_raises_the_status_mapped_not_allowed_exception()
    {
        var tenant = TenantId.Parse("not-allowed");
        await SeedTenantAsync(tenant);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA });

            // Pins the exception type the transport bindings map to FailedPrecondition.
            Assert.That(
                async () => await Facade.SetResidencyAsync(tenant.Value, new[] { RegionB }),
                Throws.TypeOf<TenantRegionNotAllowedException>(),
                "residency is always a subset of the operator-authored allowed set");
        }
    }

    [Test]
    public async Task Region_status_reports_an_allowed_but_not_yet_resident_region()
    {
        var tenant = TenantId.Parse("allowed-not-resident");
        await SeedTenantAsync(tenant);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA, RegionB });
            await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA });

            var report = await Facade.GetTenantRegionStatusAsync(tenant.Value);
            var row = report.Regions.Single(r => r.RegionId == RegionB);
            Assert.Multiple(() =>
            {
                Assert.That(row.IsAllowed, Is.True, "the operator authorized it");
                Assert.That(row.Status, Is.EqualTo(TenantRegionLifecycleStatus.None),
                    "but the tenant has not moved into it - the actionable-but-not-resident case the "
                    + "region catalog advertises");
            });
        }
    }

    private async Task<TenantRegionLifecycleStatus> WaitForStatusAsync(
        TenantId tenant, string regionId, TenantRegionLifecycleStatus expected)
    {
        // The drain is driven off the residency snapshot's background rebuilds, so
        // poll the committed record against a generous deadline rather than a delay.
        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
        var status = TenantRegionLifecycleStatus.None;
        while (Environment.TickCount64 < deadline)
        {
            using var systemOrigin = LatticeSystemOrigin.Enter();
            var report = await Facade.GetTenantRegionStatusAsync(tenant.Value);
            status = report.Regions.Single(r => r.RegionId == regionId).Status;
            if (status == expected)
            {
                return status;
            }

            await Task.Delay(50);
        }

        return status;
    }

    private async Task SeedTenantAsync(TenantId tenant, string? onlineRegion = null)
    {
        var record = TenantRecord.Create(
            tenant,
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 1000 },
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed");
        if (onlineRegion is not null)
        {
            record.AuthorizeRegion(onlineRegion, new HybridLogicalClock { WallClockTicks = 2 }, "seed");
            record.SetRegionStatus(onlineRegion, TenantRegionStatus.Online, new HybridLogicalClock { WallClockTicks = 3 }, "seed");
            record.AddAdminSubject(TenantAdmin, new HybridLogicalClock { WallClockTicks = 4 }, "seed");
        }

        await _fixture.Registry.PutAsync(record);
    }

    private static byte[] Bytes(string value) => System.Text.Encoding.UTF8.GetBytes(value);

    /// <summary>
    /// A single-silo cluster composing the tenancy engine, the auth add-on
    /// (default-allow with a bootstrap operator), and the tenant-admin control API,
    /// so the region-residency facade and its driver run against a real registry.
    /// </summary>
    private sealed class FacadeClusterFixture
    {
        public TestCluster Cluster { get; private set; } = null!;

        public IServiceProvider SiloServices =>
            Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

        public ITenantRegistry Registry => SiloServices.GetRequiredService<ITenantRegistry>();

        public string LocalRegionId =>
            SiloServices.GetRequiredService<Microsoft.Extensions.Options.IOptions<Orleans.Configuration.ClusterOptions>>().Value.ClusterId;

        public async Task InitializeAsync()
        {
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            builder.Options.ClusterId = RegionA;
            builder.AddSiloBuilderConfigurator<SiloConfigurator>();
            Cluster = builder.Build();
            await Cluster.DeployAsync();
        }

        public async Task DisposeAsync()
        {
            if (Cluster is not null)
            {
                await Cluster.StopAllSilosAsync();
                await Cluster.DisposeAsync();
            }
        }

        private sealed class SiloConfigurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
                siloBuilder.UseInMemoryReminderService();
                siloBuilder.Services.AddSingleton<IBootstrapSnapshotSource, FixtureBootstrapSnapshotSource>();
                siloBuilder.AddLatticeReplication(options =>
                {
                    options.ClusterId = RegionA;
                    options.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
                    {
                        [BackfillTree] = LatticeMergeMode.LwwRegister,
                    };
                });
                siloBuilder.AddLatticeMembership();
                siloBuilder.AddLatticeAuth(options =>
                {
                    options.DefaultEffect = LatticeEffect.Allow;
                    options.BootstrapAdministrators.Add(Operator);
                });
                siloBuilder.AddLatticeTenancy();
                siloBuilder.AddLatticeTenantAdminApi();
            }
        }

        internal sealed class FixtureBootstrapSnapshotSource : IBootstrapSnapshotSource
        {
            private static readonly object Sync = new();
            private static string? _tree;
            private static TaskCompletionSource _firstRowApplied = NewSignal();
            private static TaskCompletionSource _released = NewSignal();

            private static TaskCompletionSource NewSignal() =>
                new(TaskCreationOptions.RunContinuationsAsynchronously);

            public static void Arm(string tree)
            {
                lock (Sync)
                {
                    _tree = tree;
                    _firstRowApplied = NewSignal();
                    _released = NewSignal();
                }
            }

            public static void Release()
            {
                lock (Sync)
                {
                    _released.TrySetResult();
                    _tree = null;
                }
            }

            public static async Task FirstRowAppliedAsync(TimeSpan timeout)
            {
                Task firstRow;
                lock (Sync)
                {
                    firstRow = _firstRowApplied.Task;
                }

                var reached = await Task.WhenAny(firstRow, Task.Delay(timeout));
                Assert.That(reached, Is.SameAs(firstRow),
                    "PRECONDITION: the receiver must apply a snapshot row and pause before promotion");
            }

            public Task<SnapshotStream> ExportAsync(
                string treeName,
                HybridLogicalClock asOfHlc,
                CancellationToken cancellationToken = default) =>
                Task.FromResult(new SnapshotStream(
                    treeName,
                    HybridLogicalClock.Zero,
                    new VersionVector(),
                    RowsAsync(treeName)));

            private static async IAsyncEnumerable<SnapshotEntry> RowsAsync(string treeName)
            {
                string? gatedTree;
                TaskCompletionSource firstRow;
                TaskCompletionSource released;
                lock (Sync)
                {
                    gatedTree = _tree;
                    firstRow = _firstRowApplied;
                    released = _released;
                }

                yield return new SnapshotEntry
                {
                    Key = "existing-1",
                    Value = Bytes("replicated-one"),
                    Timestamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.AddHours(1).Ticks },
                };

                if (string.Equals(treeName, gatedTree, StringComparison.Ordinal))
                {
                    firstRow.TrySetResult();
                    await released.Task;
                }

                yield return new SnapshotEntry
                {
                    Key = "existing-2",
                    Value = Bytes("replicated-two"),
                    Timestamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.AddHours(1).Ticks },
                };
            }
        }
    }
}
