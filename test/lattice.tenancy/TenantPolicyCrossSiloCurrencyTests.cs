using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Replication;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Regression tests for issue #4030: a tenant-registry write is observed by the
/// change feed only on the silo that committed it, so every other silo's compiled
/// tenant-policy snapshot must be told - or must lose its authority - before the
/// write completes. Two or more <see cref="CompiledTenantPolicySnapshotMaintainer"/>s
/// over one registry stand in for silos, joined by the in-process
/// <see cref="TenantPolicyEpochTestCluster"/> (the epoch grain's real ledger on a
/// fake clock). The registry mutation is delivered only to silo A, exactly as the
/// core does. Nothing sleeps or polls: leases lapse only when the test advances
/// the fake clock.
/// </summary>
[TestFixture]
public sealed class TenantPolicyCrossSiloCurrencyTests
{
    private const string SharedTree = "t/acme/orders";

    private static readonly TenantId Beta = TenantId.Parse("beta");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    // ---- the issue's reproduction ----------------------------------------

    [Test]
    public async Task Peer_silo_never_notified_of_a_revocation_must_not_admit()
    {
        var world = await World.CreateAsync();
        world.AssertPeerAdmits("precondition: the active grant admits on silo B");
        world.SiloBRegistry.HoldScans();

        world.RevokeGrant();
        await world.CommitOnSiloAAsync();

        Assert.Multiple(() =>
        {
            Assert.That(world.SiloB.IsSnapshotAuthoritative, Is.False, "silo B was told its snapshot is behind");
            world.AssertSiloBSnapshotStillAdmits();
        });
        Assert.That(
            (await world.ReadOnSiloBAsync()).Allowed,
            Is.False,
            "silo B must not keep admitting a grant revoked on silo A");

        world.SiloBRegistry.ReleaseScans();
        await world.SiloB.BackgroundRebuild;
        Assert.That((await world.ReadOnSiloBAsync()).Allowed, Is.False, "the revocation holds once silo B has rebuilt");
    }

    [Test]
    public async Task Peer_silo_converges_and_regains_authority_once_its_rebuild_lands()
    {
        var world = await World.CreateAsync();

        world.RevokeGrant();
        await world.CommitOnSiloAAsync();
        await world.SiloB.BackgroundRebuild;

        Assert.Multiple(() =>
        {
            Assert.That(world.SiloB.IsSnapshotAuthoritative, Is.True, "the pushed rebuild restores authority");
            Assert.That(
                new LatticeTenantPolicyEngine(world.SiloB)
                    .ResolveCrossTenantGrant(Beta, TenantId.Parse("acme"), SharedTree, TenantGrantOperations.Read)
                    .Allowed,
                Is.False,
                "the rebuilt snapshot no longer admits the revoked grant");
        });
        Assert.That((await world.ReadOnSiloBAsync()).Allowed, Is.False);
    }

    [Test]
    public async Task Peer_silo_approval_on_silo_A_is_read_your_writes_on_silo_B()
    {
        var world = await World.CreateAsync(TenantGrantState.Pending);
        Assert.That((await world.ReadOnSiloBAsync()).Allowed, Is.False, "precondition: a pending grant does not admit");

        world.TransitionGrant(TenantGrantState.Active);
        await world.CommitOnSiloAAsync();

        var decision = await world.ReadOnSiloBAsync();

        Assert.That(decision.Allowed, Is.True, $"an approval on silo A is honoured on silo B at once; denied with: {decision.Reason}");
    }

    [Test]
    public async Task Every_peer_silo_is_told_before_the_write_completes()
    {
        var world = await World.CreateAsync();
        var siloCRegistry = new HoldableTenantRegistry(world.Registry);
        var siloC = await world.Cluster.AddLeasedSiloAsync(siloCRegistry);
        world.SiloBRegistry.HoldScans();
        siloCRegistry.HoldScans();

        world.RevokeGrant();
        await world.CommitOnSiloAAsync();

        Assert.Multiple(() =>
        {
            Assert.That(world.SiloB.IsSnapshotAuthoritative, Is.False);
            Assert.That(siloC.IsSnapshotAuthoritative, Is.False);
        });
        world.SiloBRegistry.ReleaseScans();
        siloCRegistry.ReleaseScans();
        await world.SiloB.BackgroundRebuild;
        await siloC.BackgroundRebuild;
    }

    // ---- a peer the push cannot reach -------------------------------------

    [Test]
    public async Task Unreachable_peer_holds_the_write_open_until_its_lease_lapses_and_then_denies()
    {
        var world = await World.CreateAsync();
        world.Cluster.MakeUnreachable(world.SiloB);

        world.RevokeGrant();
        var write = world.SiloA.OnMutationAsync(World.RegistryMutation, CancellationToken.None);

        Assert.That(write.IsCompleted, Is.False, "the write waits out the lease of a silo that did not acknowledge");

        world.Cluster.Time.Advance(world.Cluster.LeaseWaitOut);
        await write;

        Assert.That(world.SiloB.IsSnapshotAuthoritative, Is.False, "silo B's lease has lapsed");
        Assert.That((await world.ReadOnSiloBAsync()).Allowed, Is.False, "an unreachable silo falls back to the registry");
    }

    [Test]
    public async Task Unreachable_peer_that_renews_during_the_wait_learns_the_new_epoch()
    {
        var world = await World.CreateAsync();
        world.Cluster.MakeUnreachable(world.SiloB);

        world.RevokeGrant();
        var write = world.SiloA.OnMutationAsync(World.RegistryMutation, CancellationToken.None);
        world.SiloBRegistry.HoldScans();
        world.Cluster.Renew(world.SiloB);

        Assert.That(world.SiloB.IsSnapshotAuthoritative, Is.False, "a renewal after the advance carries the new epoch");
        Assert.That((await world.ReadOnSiloBAsync()).Allowed, Is.False);
        world.SiloBRegistry.ReleaseScans();

        world.Cluster.Time.Advance(world.Cluster.LeaseWaitOut);
        await write;
    }

    [Test]
    public async Task Peer_silo_whose_lease_lapses_is_not_authoritative()
    {
        var world = await World.CreateAsync();

        world.Cluster.Time.Advance(TenantPolicyEpochTestCluster.LeaseDuration);

        Assert.That(world.SiloB.IsSnapshotAuthoritative, Is.False, "an unrenewed lease lapses");
        world.AssertPeerAdmits("an unchanged grant is still admitted, confirmed against the registry");
    }

    // ---- a restarted epoch grain ------------------------------------------

    [Test]
    public async Task Restarted_epoch_grain_holds_the_write_open_until_every_old_lease_has_lapsed()
    {
        var world = await World.CreateAsync();
        world.Cluster.RestartEpochGrain();

        world.RevokeGrant();
        var write = world.SiloA.OnMutationAsync(World.RegistryMutation, CancellationToken.None);

        Assert.That(
            write.IsCompleted,
            Is.False,
            "a fresh incarnation has pushed to nobody, so it must wait out every lease the old one granted");

        world.Cluster.Time.Advance(world.Cluster.LeaseWaitOut);
        await write;

        Assert.That(world.SiloB.IsSnapshotAuthoritative, Is.False, "silo B's lease from the old incarnation has lapsed");
        Assert.That(
            (await world.ReadOnSiloBAsync()).Allowed,
            Is.False,
            "silo B must not admit a grant revoked after the epoch grain restarted");
    }

    [Test]
    public async Task Restarted_epoch_grain_invalidates_a_silo_that_renews_against_it()
    {
        var world = await World.CreateAsync();
        world.Cluster.RestartEpochGrain();
        world.SiloBRegistry.HoldScans();

        world.Cluster.Renew(world.SiloB);

        Assert.That(world.SiloB.IsSnapshotAuthoritative, Is.False, "a new incarnation is treated as a registry change");
        world.SiloBRegistry.ReleaseScans();
        await world.SiloB.BackgroundRebuild;
        Assert.That(world.SiloB.IsSnapshotAuthoritative, Is.True, "and the rebuild restores authority");
    }

    // ---- the committing silo itself ---------------------------------------

    [Test]
    public async Task Committing_silo_is_not_authoritative_while_its_advance_is_in_flight()
    {
        var world = await World.CreateAsync();
        world.Cluster.MakeUnreachable(world.SiloB);

        world.RevokeGrant();
        var write = world.SiloA.OnMutationAsync(World.RegistryMutation, CancellationToken.None);
        await world.SiloA.BackgroundRebuild;

        Assert.That(world.SiloA.IsSnapshotAuthoritative, Is.False, "silo A has not finished telling the cluster");

        world.Cluster.Time.Advance(world.Cluster.LeaseWaitOut);
        await write;
        world.Cluster.Renew(world.SiloA);
        await world.SiloA.BackgroundRebuild;
        Assert.That(world.SiloA.IsSnapshotAuthoritative, Is.True, "once published and re-leased, silo A is authoritative again");
    }

    // ---- the replication isolation gate trusts the same signal ------------

    [Test]
    public async Task Replication_gate_on_a_peer_silo_rejects_a_tenant_suspended_on_another_silo()
    {
        var world = await World.CreateAsync();
        var gateB = new ReplicationTenantIsolationGate(world.SiloBRegistry, new NullTenantResidencyResolver(), world.SiloB);
        Assert.That(
            await gateB.EvaluateAsync(SharedTree),
            Is.EqualTo(ReplicationTenantIsolationDecision.Admit),
            "precondition: an active tenant is admitted on silo B");

        world.SiloBRegistry.HoldScans();
        world.Owner.SetStatus(TenantStatus.Suspended, Clock(9_000), "test");
        await world.CommitOnSiloAAsync();
        Assert.That(world.SiloB.Current.TryGetTenant("acme", out var stale) && stale!.Status == TenantStatus.Active, Is.True,
            "precondition: silo B's snapshot still holds the tenant as active");

        Assert.That(
            await gateB.EvaluateAsync(SharedTree),
            Is.EqualTo(ReplicationTenantIsolationDecision.RejectSuspendedTenant),
            "silo B must not keep admitting inbound replication for a tenant suspended on silo A");
    }

    [Test]
    public async Task Replication_gate_on_a_peer_silo_rejects_a_tenant_deleted_on_another_silo()
    {
        var world = await World.CreateAsync();
        var gateB = new ReplicationTenantIsolationGate(world.SiloBRegistry, new NullTenantResidencyResolver(), world.SiloB);

        world.SiloBRegistry.HoldScans();
        world.Registry.Records.Remove(world.Owner);
        await world.CommitOnSiloAAsync();
        Assert.That(world.SiloB.Current.TryGetTenant("acme", out _), Is.True,
            "precondition: silo B's snapshot still holds the deleted tenant");

        Assert.That(
            await gateB.EvaluateAsync(SharedTree),
            Is.EqualTo(ReplicationTenantIsolationDecision.RejectUnknownTenant),
            "silo B must not keep admitting inbound replication for a tenant deleted on silo A");
    }

    // ---- a silo that has never built its snapshot --------------------------

    [Test]
    public async Task Cold_silo_builds_its_snapshot_on_first_use_instead_of_reporting_tenants_unregistered()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("beta", admins: ["bob"]));
        var cold = TenantPolicyEpochTestCluster.Unleased(registry);
        var enforcer = new TenantGateEnforcer(
            new LatticeTenantPolicyEngine(cold),
            new NullTenantResidencyResolver(),
            cold,
            registry,
            NullLogger<TenantGateEnforcer>.Instance);
        LatticeActiveTenantContext.Current = Beta;
        var request = new LatticeAccessRequest("t/beta/own", LatticeOperation.Read, new LatticeSubject("bob"), "k");

        var decision = await enforcer.EnforceAsync(in request);

        Assert.That(decision.Allowed, Is.True, $"a registered tenant is not reported unregistered on a cold silo; denied with: {decision.Reason}");
        Assert.That(cold.CurrentEpoch, Is.GreaterThan(0), "the first decision built the snapshot");
    }

    [Test]
    public async Task Cold_silo_whose_warm_up_fails_denies_tenant_owned_access()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Throwing());
        var cold = TenantPolicyEpochTestCluster.Unleased(registry);
        var enforcer = new TenantGateEnforcer(
            new LatticeTenantPolicyEngine(cold),
            new NullTenantResidencyResolver(),
            cold,
            registry,
            NullLogger<TenantGateEnforcer>.Instance);
        LatticeActiveTenantContext.Current = Beta;
        var request = new LatticeAccessRequest("t/beta/own", LatticeOperation.Read, new LatticeSubject("bob"), "k");

        var decision = await enforcer.EnforceAsync(in request);

        Assert.That(decision.Allowed, Is.False, "an unbuildable snapshot admits no tenant-owned access");
    }

    [Test]
    public void Cold_silo_caller_cancellation_during_warm_up_propagates()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var registry = new FakeTenantRegistry();
        var cold = TenantPolicyEpochTestCluster.Unleased(registry);
        var enforcer = new TenantGateEnforcer(
            new LatticeTenantPolicyEngine(cold),
            new NullTenantResidencyResolver(),
            cold,
            registry,
            NullLogger<TenantGateEnforcer>.Instance);
        LatticeActiveTenantContext.Current = Beta;

        Assert.That(
            async () =>
            {
                var request = new LatticeAccessRequest("t/beta/own", LatticeOperation.Read, new LatticeSubject("bob"), "k");
                await enforcer.EnforceAsync(in request, cts.Token);
            },
            Throws.InstanceOf<OperationCanceledException>());
    }

#pragma warning disable CS1998 // the throw is the point; no await is reachable
    private static async IAsyncEnumerable<TenantRecord> Throwing()
    {
        throw new InvalidOperationException("registry scan failed");
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
    }
#pragma warning restore CS1998

    /// <summary>
    /// Tenant <c>acme</c> sharing <c>t/acme/orders</c> with tenant <c>beta</c>, whose
    /// admin <c>bob</c> reads it; silos A and B are leased and built over one registry.
    /// </summary>
    private sealed class World
    {
        public static readonly LatticeMutation RegistryMutation = new() { TreeId = TenantTreeNames.RegistryTree };

        private long _tick = 1_000;

        private World(
            TenantPolicyEpochTestCluster cluster,
            FakeTenantRegistry registry,
            TenantRecord owner,
            CompiledTenantPolicySnapshotMaintainer siloA,
            CompiledTenantPolicySnapshotMaintainer siloB,
            HoldableTenantRegistry siloBRegistry)
        {
            Cluster = cluster;
            Registry = registry;
            SiloBRegistry = siloBRegistry;
            Owner = owner;
            SiloA = siloA;
            SiloB = siloB;
            EnforcerB = new TenantGateEnforcer(
                new LatticeTenantPolicyEngine(siloB),
                new NullTenantResidencyResolver(),
                siloB,
                siloBRegistry,
                NullLogger<TenantGateEnforcer>.Instance);
        }

        public TenantPolicyEpochTestCluster Cluster { get; }

        public FakeTenantRegistry Registry { get; }

        /// <summary>Silo B's view of the registry, whose scans can be held.</summary>
        public HoldableTenantRegistry SiloBRegistry { get; }

        public TenantRecord Owner { get; }

        public CompiledTenantPolicySnapshotMaintainer SiloA { get; }

        public CompiledTenantPolicySnapshotMaintainer SiloB { get; }

        public TenantGateEnforcer EnforcerB { get; }

        public static async Task<World> CreateAsync(TenantGrantState initialState = TenantGrantState.Active)
        {
            var registry = new FakeTenantRegistry();
            var owner = Record(
                "acme",
                grants: [CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, SharedTree, TenantGrantOperations.Read, initialState)]);
            registry.Records.Add(owner);
            registry.Records.Add(Record("beta", admins: ["bob"]));

            var cluster = new TenantPolicyEpochTestCluster();
            var siloA = await cluster.AddLeasedSiloAsync(registry);
            var siloBRegistry = new HoldableTenantRegistry(registry);
            var siloB = await cluster.AddLeasedSiloAsync(siloBRegistry);
            return new World(cluster, registry, owner, siloA, siloB, siloBRegistry);
        }

        public void RevokeGrant() => TransitionGrant(TenantGrantState.Revoked);

        public void TransitionGrant(TenantGrantState state) =>
            Owner.TransitionGrant(Owner.Grants.Single().GrantId, state, Clock(++_tick), "test");

        /// <summary>
        /// Delivers the registry write's change-feed event to silo A only, as the
        /// core does, and waits for the write to complete and silo A's rebuild.
        /// </summary>
        public async Task CommitOnSiloAAsync()
        {
            await SiloA.OnMutationAsync(RegistryMutation, CancellationToken.None);
            await SiloA.BackgroundRebuild;
        }

        public async Task<LatticeAccessDecision> ReadOnSiloBAsync()
        {
            LatticeActiveTenantContext.Current = Beta;
            var request = new LatticeAccessRequest(SharedTree, LatticeOperation.Read, new LatticeSubject("bob"), "k");
            return await EnforcerB.EnforceAsync(in request);
        }

        public void AssertSiloBSnapshotStillAdmits() =>
            Assert.That(
                new LatticeTenantPolicyEngine(SiloB)
                    .ResolveCrossTenantGrant(Beta, TenantId.Parse("acme"), SharedTree, TenantGrantOperations.Read)
                    .Allowed,
                Is.True,
                "precondition: silo B's snapshot still holds the active grant, so trusting it would admit");

        public void AssertPeerAdmits(string because)
        {
            var decision = ReadOnSiloBAsync().GetAwaiter().GetResult();
            Assert.That(decision.Allowed, Is.True, $"{because}; denied with: {decision.Reason}");
        }
    }
}
