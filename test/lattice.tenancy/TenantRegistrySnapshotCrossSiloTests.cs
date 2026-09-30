using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Replication;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Regression tests for issues #4051 (residency) and #4052 (placement): a
/// tenant-registry write is observed by the change feed only on the silo that
/// commits it, so every other silo's residency and placement snapshots must be
/// told - or lose their authority - before the write completes, and every consumer
/// must fall back while they are not authoritative. Silos are simulated by
/// maintainers joined through <see cref="TenantPolicyEpochTestCluster"/> (the epoch
/// grain's real ledger on a fake clock). The mutation is delivered only to silo A,
/// exactly as the core does; silo A's compiled-policy maintainer publishes the
/// advance, as in production. Nothing sleeps or polls.
/// </summary>
[TestFixture]
public sealed class TenantRegistrySnapshotCrossSiloTests
{
    private const string Region = "eu";
    private const string OwnedTree = "t/acme/orders";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly LatticeMutation RegistryWrite = new() { TreeId = TenantTreeNames.RegistryTree };

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    // ---- #4051 residency ---------------------------------------------------

    [Test]
    public async Task Residency_peer_silo_never_notified_of_a_region_drain_must_not_report_online()
    {
        var world = await ResidencyWorld.CreateAsync();
        Assert.That(world.ResolverB.IsOnlineInServingRegion(Acme), Is.True, "precondition: online on silo B");
        world.SiloBRegistry.HoldScans();

        world.Acme.SetRegionStatus(Region, TenantRegionStatus.Offline, Clock(20), "test");
        await world.CommitOnSiloAAsync();

        Assert.Multiple(() =>
        {
            Assert.That(world.ResidencyB.IsSnapshotAuthoritative, Is.False, "silo B was told its residency view is behind");
            Assert.That(world.ResidencyB.Current.IsOnlineLocally(Acme), Is.True, "precondition: the stale view still says online");
            Assert.That(world.ResolverB.TryResolveOnline(Acme, out _), Is.False, "a stale view is not answered from");
            Assert.That(world.ResolverB.IsOnlineInServingRegion(Acme), Is.False,
                "silo B must not keep reporting a tenant online after it was taken offline through silo A");
        });
        Assert.That(await world.ResolverB.ConfirmOnlineAsync(Acme), Is.False, "the registry record says offline");

        world.SiloBRegistry.ReleaseScans();
        await world.ResidencyB.BackgroundRebuild;
        Assert.Multiple(() =>
        {
            Assert.That(world.ResidencyB.IsSnapshotAuthoritative, Is.True);
            Assert.That(world.ResolverB.IsOnlineInServingRegion(Acme), Is.False, "the drain holds once silo B has rebuilt");
        });
    }

    [Test]
    public async Task Residency_tenant_gate_on_a_peer_silo_refuses_a_tenant_taken_offline_on_another_silo()
    {
        var world = await ResidencyWorld.CreateAsync();
        world.AssertGateOnB(allowed: true, "precondition: the online tenant is admitted on silo B");
        world.SiloBRegistry.HoldScans();

        world.Acme.SetRegionStatus(Region, TenantRegionStatus.Draining, Clock(20), "test");
        await world.CommitOnSiloAAsync();

        var decision = await world.ReadOnSiloBAsync();

        Assert.That(decision.Allowed, Is.False, "silo B's tenant gate must not admit a tenant drained through silo A");
        Assert.That(decision.Reason, Does.Contain("not online"));
    }

    [Test]
    public async Task Residency_tenant_gate_confirms_an_online_tenant_while_the_view_is_stale()
    {
        var world = await ResidencyWorld.CreateAsync(TenantRegionStatus.Backfilling);
        world.AssertGateOnB(allowed: false, "precondition: a backfilling tenant is refused on silo B");
        world.SiloBRegistry.HoldScans();

        world.Acme.SetRegionStatus(Region, TenantRegionStatus.Online, Clock(20), "test");
        await world.CommitOnSiloAAsync();
        var reads = world.SiloBRegistry.PointReads;

        var decision = await world.ReadOnSiloBAsync();

        Assert.That(decision.Allowed, Is.True, $"a tenant brought online through silo A is admitted on silo B at once; denied with: {decision.Reason}");
        Assert.That(world.SiloBRegistry.PointReads, Is.GreaterThan(reads), "the stale view was confirmed against the registry");
    }

    [Test]
    public async Task Residency_synchronous_enforce_while_the_view_is_stale_denies()
    {
        var world = await ResidencyWorld.CreateAsync();
        world.SiloBRegistry.HoldScans();
        await world.CommitOnSiloAAsync();
        LatticeActiveTenantContext.Current = Acme;

        var request = ResidencyWorld.Request();
        var decision = world.EnforcerB.Enforce(in request);

        Assert.That(decision.Allowed, Is.False, "the synchronous form cannot confirm residency, so it denies");
        Assert.That(decision.Reason, Does.Contain("could not be confirmed"));
    }

    [Test]
    public async Task Residency_registry_failure_during_confirmation_denies()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Acme, Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("registry down"));
        var enforcer = OwnedTreeEnforcer(new TenantResidencyResolver(TenantPolicyEpochTestCluster.UnleasedResidency(registry, Region), registry));
        LatticeActiveTenantContext.Current = Acme;

        var request = ResidencyWorld.Request();
        var decision = await enforcer.EnforceAsync(in request);

        Assert.That(decision.Allowed, Is.False, "unconfirmable residency is denied, never admitted");
        Assert.That(decision.Reason, Does.Contain("could not be confirmed"));
    }

    [Test]
    public void Residency_caller_cancellation_during_confirmation_propagates()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Acme, Arg.Any<CancellationToken>()).ThrowsAsync(new OperationCanceledException(cts.Token));
        var enforcer = OwnedTreeEnforcer(new TenantResidencyResolver(TenantPolicyEpochTestCluster.UnleasedResidency(registry, Region), registry));
        LatticeActiveTenantContext.Current = Acme;

        Assert.That(
            async () =>
            {
                var request = ResidencyWorld.Request();
                await enforcer.EnforceAsync(in request, cts.Token);
            },
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Residency_crossing_confirmed_from_the_registry_is_also_gated_on_confirmed_residency()
    {
        // Both snapshots stale: the crossing is confirmed against the owner's record,
        // then the grantee's residency against the grantee's record.
        var registry = new HoldableTenantRegistry();
        registry.Records.Add(Record("acme", grants: [CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, OwnedTree, TenantGrantOperations.Read)]));
        var beta = Record("beta", admins: ["bob"]);
        beta.SetRegionStatus(Region, TenantRegionStatus.Offline, Clock(50), "test");
        registry.Records.Add(beta);
        var policy = TenantPolicyEpochTestCluster.Unleased(registry);
        await policy.RebuildNowAsync();
        var enforcer = new TenantGateEnforcer(
            new LatticeTenantPolicyEngine(policy),
            new TenantResidencyResolver(TenantPolicyEpochTestCluster.UnleasedResidency(registry, Region), registry),
            policy,
            registry,
            NullLogger<TenantGateEnforcer>.Instance);
        LatticeActiveTenantContext.Current = TenantId.Parse("beta");

        var request = new LatticeAccessRequest(OwnedTree, LatticeOperation.Read, new LatticeSubject("bob"), "k");
        var decision = await enforcer.EnforceAsync(in request);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("not online"));
    }

    [Test]
    public async Task Residency_replication_gate_on_a_peer_silo_rejects_a_tenant_taken_offline_on_another_silo()
    {
        var world = await ResidencyWorld.CreateAsync();
        var gateB = world.ReplicationGateB();
        Assert.That(await gateB.EvaluateAsync(OwnedTree), Is.EqualTo(ReplicationTenantIsolationDecision.Admit), "precondition");
        world.SiloBRegistry.HoldScans();

        world.Acme.SetRegionStatus(Region, TenantRegionStatus.Offline, Clock(20), "test");
        await world.CommitOnSiloAAsync();

        Assert.That(
            await gateB.EvaluateAsync(OwnedTree),
            Is.EqualTo(ReplicationTenantIsolationDecision.RejectOutOfRegion),
            "silo B must not keep admitting inbound replication for a tenant taken offline through silo A");
    }

    [Test]
    public async Task Residency_replication_gate_answers_from_the_snapshots_in_the_steady_state()
    {
        var world = await ResidencyWorld.CreateAsync();
        var gateB = world.ReplicationGateB();
        var reads = world.SiloBRegistry.PointReads;

        var pending = gateB.EvaluateAsync(OwnedTree);

        Assert.That(pending.IsCompletedSuccessfully, Is.True);
        Assert.That(pending.Result, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        Assert.That(world.SiloBRegistry.PointReads, Is.EqualTo(reads), "the steady state never reads the registry");
    }

    [Test]
    public async Task Residency_peer_silo_whose_lease_lapses_confirms_against_the_registry()
    {
        var world = await ResidencyWorld.CreateAsync();

        world.Cluster.Time.Advance(TenantPolicyEpochTestCluster.LeaseDuration);

        Assert.That(world.ResidencyB.IsSnapshotAuthoritative, Is.False, "an unrenewed lease lapses");
        world.AssertGateOnB(allowed: true, "an unchanged online tenant is still admitted, confirmed against the registry");
    }

    [Test]
    public async Task Residency_restarted_epoch_grain_invalidates_a_silo_that_renews_against_it()
    {
        var world = await ResidencyWorld.CreateAsync();
        world.Cluster.RestartEpochGrain();
        world.SiloBRegistry.HoldScans();

        world.Cluster.Renew(world.ResidencyB);

        Assert.That(world.ResidencyB.IsSnapshotAuthoritative, Is.False);
        world.SiloBRegistry.ReleaseScans();
        await world.ResidencyB.BackgroundRebuild;
        Assert.That(world.ResidencyB.IsSnapshotAuthoritative, Is.True);
    }

    [Test]
    public async Task Residency_local_registry_write_revokes_authority_until_the_local_rebuild_lands()
    {
        var registry = new HoldableTenantRegistry();
        registry.Records.Add(Record("acme"));
        var residency = await TenantPolicyEpochTestCluster.LeasedResidencyAsync(registry, Region);
        registry.HoldScans();

        await residency.OnMutationAsync(RegistryWrite, CancellationToken.None);

        Assert.That(residency.IsSnapshotAuthoritative, Is.False, "a failed or pending local rebuild never stays authoritative");
        registry.ReleaseScans();
        await residency.BackgroundRebuild;
        Assert.That(residency.IsSnapshotAuthoritative, Is.True);
    }

    [Test]
    public async Task Residency_non_registry_write_does_not_revoke_authority()
    {
        var residency = await TenantPolicyEpochTestCluster.LeasedResidencyAsync(new FakeTenantRegistry(), Region);

        await residency.OnMutationAsync(new LatticeMutation { TreeId = "some-app-tree" }, CancellationToken.None);

        Assert.That(residency.IsSnapshotAuthoritative, Is.True);
    }

    // ---- #4052 placement ---------------------------------------------------

    [Test]
    public async Task Placement_peer_silo_never_notified_of_a_placement_change_must_not_keep_the_old_placement()
    {
        var world = await PlacementWorld.CreateAsync();
        world.SiloBRegistry.HoldScans();

        world.Acme.SetPlacement(new TenantPlacement { DedicatedWal = true, WalProviderName = "wal-acme" }, Clock(20), "test");
        await world.CommitOnSiloAAsync();

        Assert.Multiple(() =>
        {
            Assert.That(world.PlacementB.IsSnapshotAuthoritative, Is.False, "silo B was told its placement view is behind");
            Assert.That(world.PlacementB.Current.TryGetPlacement(Acme, out var stale) && !stale.DedicatedWal, Is.True,
                "precondition: the stale view still holds the shared placement");
            Assert.That(world.ResolverB.TryResolveForRegistration(PlacementWorld.Tree, out _), Is.False,
                "silo B must not resolve a tenant tree's placement from the stale view");
        });

        var resolving = world.ResolverB.ResolveForRegistrationAsync(PlacementWorld.Tree).AsTask();
        Assert.That(resolving.IsCompleted, Is.False, "registration waits for the view to become authoritative");

        world.SiloBRegistry.ReleaseScans();
        var placement = await resolving;

        Assert.Multiple(() =>
        {
            Assert.That(world.PlacementB.Current.TryGetPlacement(Acme, out var fresh) && fresh.DedicatedWal, Is.True,
                "silo B must not keep resolving the old placement after it was changed through silo A");
            Assert.That(placement.WalProviderKey, Is.EqualTo("wal-acme"), "the tree is pinned to the new dedicated WAL");
        });
    }

    [Test]
    public async Task Placement_registration_is_refused_when_the_view_does_not_become_authoritative_in_time()
    {
        var world = await PlacementWorld.CreateAsync();
        world.SiloBRegistry.HoldScans();
        await world.CommitOnSiloAAsync();

        var resolving = world.ResolverB.ResolveForRegistrationAsync(PlacementWorld.Tree).AsTask();
        world.Cluster.Time.Advance(TenantPolicyEpochTestCluster.LeaseDuration / 5);

        var ex = Assert.ThrowsAsync<TimeoutException>(async () => await resolving);
        Assert.That(ex!.Message, Does.Contain(PlacementWorld.Tree).And.Contain("Retry"));
        world.SiloBRegistry.ReleaseScans();
    }

    [Test]
    public async Task Placement_registration_on_a_silo_whose_lease_lapsed_is_refused()
    {
        var world = await PlacementWorld.CreateAsync();
        world.Cluster.Time.Advance(TenantPolicyEpochTestCluster.LeaseDuration);
        Assert.That(world.PlacementB.IsSnapshotAuthoritative, Is.False, "precondition: the lease lapsed");

        var resolving = world.ResolverB.ResolveForRegistrationAsync(PlacementWorld.Tree).AsTask();
        world.Cluster.Time.Advance(TenantPolicyEpochTestCluster.LeaseDuration / 5);

        Assert.ThrowsAsync<TimeoutException>(async () => await resolving);
    }

    [Test]
    public async Task Placement_registration_waiting_on_a_lapsed_lease_resumes_once_it_is_renewed()
    {
        var world = await PlacementWorld.CreateAsync();
        world.Cluster.Time.Advance(TenantPolicyEpochTestCluster.LeaseDuration);

        var resolving = world.ResolverB.ResolveForRegistrationAsync(PlacementWorld.Tree).AsTask();
        world.Cluster.Renew(world.PlacementB);

        Assert.That(await resolving, Is.EqualTo(TreePhysicalPlacement.Default), "a renewed lease restores authority");
    }

    [Test]
    public async Task Placement_non_tenant_tree_resolves_synchronously_even_while_the_view_is_stale()
    {
        var world = await PlacementWorld.CreateAsync();
        world.SiloBRegistry.HoldScans();
        await world.CommitOnSiloAAsync();

        var resolved = world.ResolverB.TryResolveForRegistration("legacy-tree", out var placement);

        Assert.That(resolved, Is.True);
        Assert.That(placement, Is.EqualTo(TreePhysicalPlacement.Default));
        world.SiloBRegistry.ReleaseScans();
    }

    [Test]
    public async Task Placement_WaitUntilAuthoritativeAsync_returns_at_once_when_authoritative()
    {
        var world = await PlacementWorld.CreateAsync();

        var waiting = world.PlacementB.WaitUntilAuthoritativeAsync(TimeSpan.FromSeconds(1));

        Assert.That(waiting.IsCompletedSuccessfully, Is.True);
        Assert.That(await waiting, Is.True);
    }

    [Test]
    public async Task Placement_WaitUntilAuthoritativeAsync_caller_cancellation_propagates()
    {
        var world = await PlacementWorld.CreateAsync();
        world.SiloBRegistry.HoldScans();
        await world.CommitOnSiloAAsync();
        using var cts = new CancellationTokenSource();

        var waiting = world.PlacementB.WaitUntilAuthoritativeAsync(TimeSpan.FromSeconds(1), cts.Token);
        cts.Cancel();

        Assert.That(async () => await waiting, Throws.InstanceOf<OperationCanceledException>());
        world.SiloBRegistry.ReleaseScans();
    }

    [Test]
    public async Task Placement_local_registry_write_revokes_authority_until_the_local_rebuild_lands()
    {
        var registry = new HoldableTenantRegistry();
        registry.Records.Add(Record("acme"));
        var placement = await TenantPolicyEpochTestCluster.LeasedPlacementAsync(registry);
        registry.HoldScans();

        await placement.OnMutationAsync(RegistryWrite, CancellationToken.None);

        Assert.That(placement.IsSnapshotAuthoritative, Is.False);
        registry.ReleaseScans();
        await placement.BackgroundRebuild;
        Assert.That(placement.IsSnapshotAuthoritative, Is.True);
    }

    private static TenantGateEnforcer OwnedTreeEnforcer(ITenantResidencyResolver residency)
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());
        var policy = TenantPolicyEpochTestCluster.Unleased(new FakeTenantRegistry());
        policy.RebuildNowAsync().GetAwaiter().GetResult();
        return new TenantGateEnforcer(engine, residency, policy, Substitute.For<ITenantRegistry>(), NullLogger<TenantGateEnforcer>.Instance);
    }

    /// <summary>
    /// Tenant <c>acme</c> (admin <c>alice</c>) resident in region <c>eu</c>. Silo A is a
    /// compiled-policy maintainer (which publishes advances) plus a residency
    /// maintainer; silo B is a compiled-policy maintainer and a residency maintainer
    /// whose registry scans can be held - so the residency view alone is stale, and
    /// every fallback exercised is the residency one. All are leased and built.
    /// </summary>
    private sealed class ResidencyWorld
    {
        private ResidencyWorld(
            TenantPolicyEpochTestCluster cluster,
            FakeTenantRegistry registry,
            TenantRecord acme,
            CompiledTenantPolicySnapshotMaintainer policyA,
            TenantResidencySnapshotMaintainer residencyA,
            CompiledTenantPolicySnapshotMaintainer policyB,
            TenantResidencySnapshotMaintainer residencyB,
            HoldableTenantRegistry siloBRegistry)
        {
            Cluster = cluster;
            Registry = registry;
            Acme = acme;
            PolicyA = policyA;
            ResidencyA = residencyA;
            PolicyB = policyB;
            ResidencyB = residencyB;
            SiloBRegistry = siloBRegistry;
            ResolverB = new TenantResidencyResolver(residencyB, siloBRegistry);
            EnforcerB = new TenantGateEnforcer(
                new LatticeTenantPolicyEngine(policyB),
                ResolverB,
                policyB,
                siloBRegistry,
                NullLogger<TenantGateEnforcer>.Instance);
        }

        public TenantPolicyEpochTestCluster Cluster { get; }

        public FakeTenantRegistry Registry { get; }

        public TenantRecord Acme { get; }

        public CompiledTenantPolicySnapshotMaintainer PolicyA { get; }

        public TenantResidencySnapshotMaintainer ResidencyA { get; }

        public CompiledTenantPolicySnapshotMaintainer PolicyB { get; }

        public TenantResidencySnapshotMaintainer ResidencyB { get; }

        public HoldableTenantRegistry SiloBRegistry { get; }

        public TenantResidencyResolver ResolverB { get; }

        public TenantGateEnforcer EnforcerB { get; }

        public static async Task<ResidencyWorld> CreateAsync(TenantRegionStatus initial = TenantRegionStatus.Online)
        {
            var registry = new FakeTenantRegistry();
            var acme = Record("acme", admins: ["alice"]);
            acme.SetRegionStatus(Region, initial, Clock(10), "test");
            registry.Records.Add(acme);

            var cluster = new TenantPolicyEpochTestCluster();
            var policyA = await cluster.AddLeasedSiloAsync(registry);
            var residencyA = await cluster.AddLeasedResidencySiloAsync(registry, Region);
            var siloBRegistry = new HoldableTenantRegistry(registry);
            var policyB = await cluster.AddLeasedSiloAsync(registry);
            var residencyB = await cluster.AddLeasedResidencySiloAsync(siloBRegistry, Region);
            return new ResidencyWorld(cluster, registry, acme, policyA, residencyA, policyB, residencyB, siloBRegistry);
        }

        public static LatticeAccessRequest Request() =>
            new(OwnedTree, LatticeOperation.Read, new LatticeSubject("alice"), "k");

        public ReplicationTenantIsolationGate ReplicationGateB() =>
            new(SiloBRegistry, ResolverB, PolicyB);

        /// <summary>
        /// Delivers the registry write's change-feed event to silo A's observers only,
        /// in production registration order, and waits for the write to complete.
        /// </summary>
        public async Task CommitOnSiloAAsync()
        {
            await PolicyA.OnMutationAsync(RegistryWrite, CancellationToken.None);
            await ResidencyA.OnMutationAsync(RegistryWrite, CancellationToken.None);
            await PolicyA.BackgroundRebuild;
            await ResidencyA.BackgroundRebuild;
            await PolicyB.BackgroundRebuild;
            Assert.That(PolicyB.IsSnapshotAuthoritative, Is.True, "precondition: only silo B's residency view is stale");
        }

        public async Task<LatticeAccessDecision> ReadOnSiloBAsync()
        {
            LatticeActiveTenantContext.Current = Acme.Id;
            var request = Request();
            return await EnforcerB.EnforceAsync(in request);
        }

        public void AssertGateOnB(bool allowed, string because)
        {
            var decision = ReadOnSiloBAsync().GetAwaiter().GetResult();
            Assert.That(decision.Allowed, Is.EqualTo(allowed), $"{because}; reason: {decision.Reason}");
        }
    }

    /// <summary>
    /// Tenant <c>acme</c> on the shared placement. Silo A is a compiled-policy
    /// maintainer plus a placement maintainer; silo B is a placement maintainer whose
    /// registry scans can be held, with its WAL placement resolver.
    /// </summary>
    private sealed class PlacementWorld
    {
        public static readonly string Tree = LatticeTenantTrees.Compose(TenantId.Parse("acme"), "orders");

        private PlacementWorld(
            TenantPolicyEpochTestCluster cluster,
            TenantRecord acme,
            CompiledTenantPolicySnapshotMaintainer policyA,
            TenantPlacementSnapshotMaintainer placementA,
            TenantPlacementSnapshotMaintainer placementB,
            HoldableTenantRegistry siloBRegistry)
        {
            Cluster = cluster;
            Acme = acme;
            PolicyA = policyA;
            PlacementA = placementA;
            PlacementB = placementB;
            SiloBRegistry = siloBRegistry;
            ResolverB = new TenantWalPlacementResolver(
                placementB,
                Options.Create(new LatticeTenancyOptions { PolicySnapshotLeaseDuration = TenantPolicyEpochTestCluster.LeaseDuration }));
        }

        public TenantPolicyEpochTestCluster Cluster { get; }

        public TenantRecord Acme { get; }

        public CompiledTenantPolicySnapshotMaintainer PolicyA { get; }

        public TenantPlacementSnapshotMaintainer PlacementA { get; }

        public TenantPlacementSnapshotMaintainer PlacementB { get; }

        public HoldableTenantRegistry SiloBRegistry { get; }

        public TenantWalPlacementResolver ResolverB { get; }

        public static async Task<PlacementWorld> CreateAsync()
        {
            var registry = new FakeTenantRegistry();
            var acme = Record("acme");
            registry.Records.Add(acme);

            var cluster = new TenantPolicyEpochTestCluster();
            var policyA = await cluster.AddLeasedSiloAsync(registry);
            var placementA = await cluster.AddLeasedPlacementSiloAsync(registry);
            var siloBRegistry = new HoldableTenantRegistry(registry);
            var placementB = await cluster.AddLeasedPlacementSiloAsync(siloBRegistry);
            return new PlacementWorld(cluster, acme, policyA, placementA, placementB, siloBRegistry);
        }

        public async Task CommitOnSiloAAsync()
        {
            await PolicyA.OnMutationAsync(RegistryWrite, CancellationToken.None);
            await PlacementA.OnMutationAsync(RegistryWrite, CancellationToken.None);
            await PolicyA.BackgroundRebuild;
            await PlacementA.BackgroundRebuild;
        }
    }
}
