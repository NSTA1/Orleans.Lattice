using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Auth;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Regression tests for issue #4001: a cross-tenant grant decision made while the
/// compiled tenant-policy snapshot is not authoritative. A tenant-registry write
/// (approve, revoke) only schedules a background rebuild, so for one rebuild the
/// snapshot still holds the pre-write grant state. These tests drive the real
/// <see cref="CompiledTenantPolicySnapshotMaintainer"/> and
/// <see cref="LatticeTenantPolicyEngine"/> over a registry whose full scan is held
/// on a gate, so the rebuild is deterministically outstanding - no timing, no
/// polling - while a point read of the registry answers with the committed state.
/// </summary>
[TestFixture]
public sealed class TenantGateEnforcerSnapshotLagTests
{
    private const string Subject = "bob";
    private const string SharedTree = "t/acme/orders";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    // ---- the two directions the issue names -----------------------------

    [Test]
    public async Task EnforceAsync_read_immediately_after_revoking_the_grant_is_refused()
    {
        await using var world = await World.CreateAsync(TenantGrantState.Active);
        world.AssertAdmitted(await world.ReadAsync(), "precondition: the active grant admits the read");

        world.TransitionGrant(TenantGrantState.Revoked);
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillAdmits();

        var decision = await world.ReadAsync();

        Assert.That(decision.Allowed, Is.False, "a revoked grant must not outlive its revocation by a rebuild");
        Assert.That(decision.Reason, Does.Contain("no active grant"));

        await world.ReleaseRebuildAsync();
        Assert.That((await world.ReadAsync()).Allowed, Is.False, "the revocation holds once the rebuild lands");
    }

    [Test]
    public async Task EnforceAsync_read_immediately_after_removing_the_grant_is_refused()
    {
        await using var world = await World.CreateAsync(TenantGrantState.Active);

        world.RemoveGrant();
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertSnapshotStillAdmits();

        var decision = await world.ReadAsync();

        Assert.That(decision.Allowed, Is.False, "a removed grant must not outlive its removal by a rebuild");
    }

    [Test]
    public async Task EnforceAsync_read_immediately_after_approving_the_grant_is_admitted()
    {
        await using var world = await World.CreateAsync(TenantGrantState.Pending);
        Assert.That((await world.ReadAsync()).Allowed, Is.False, "precondition: a pending grant does not admit");

        world.TransitionGrant(TenantGrantState.Active);
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        Assert.That(
            world.Engine.ResolveCrossTenantGrant(Beta, Acme, SharedTree, TenantGrantOperations.Read).Allowed,
            Is.False,
            "precondition: the snapshot still holds the pending grant");

        var decision = await world.ReadAsync();

        world.AssertAdmitted(decision, "an approved grant is read-your-writes");
    }

    // ---- fail closed ----------------------------------------------------

    [Test]
    public async Task Enforce_synchronous_crossing_while_the_snapshot_is_not_authoritative_denies()
    {
        await using var world = await World.CreateAsync(TenantGrantState.Pending);
        world.TransitionGrant(TenantGrantState.Active);
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        LatticeActiveTenantContext.Current = Beta;

        var request = World.Request(LatticeOperation.Read);
        var decision = world.Enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.False, "the synchronous form cannot confirm the grant, so it denies");
        Assert.That(decision.Reason, Does.Contain("could not be confirmed"));
    }

    [Test]
    public async Task EnforceAsync_registry_failure_while_the_snapshot_is_not_authoritative_denies()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Acme, Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("registry down"));
        var enforcer = CrossingEnforcer(registry);
        LatticeActiveTenantContext.Current = Beta;

        var request = World.Request(LatticeOperation.Read);

        var decision = await enforcer.EnforceAsync(in request);

        Assert.That(decision.Allowed, Is.False, "an unconfirmable grant is denied, never admitted");
        Assert.That(decision.Reason, Does.Contain("could not be confirmed"));
    }

    [Test]
    public async Task EnforceAsync_unregistered_owner_while_the_snapshot_is_not_authoritative_denies()
    {
        var registry = new FakeTenantRegistry();
        var enforcer = CrossingEnforcer(registry);
        LatticeActiveTenantContext.Current = Beta;

        var request = World.Request(LatticeOperation.Read);

        var decision = await enforcer.EnforceAsync(in request);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("not registered"));
    }

    [Test]
    public void EnforceAsync_caller_cancellation_during_confirmation_propagates()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Acme, Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException(cts.Token));
        var enforcer = CrossingEnforcer(registry);
        LatticeActiveTenantContext.Current = Beta;

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            var request = World.Request(LatticeOperation.Read);
            await enforcer.EnforceAsync(in request, cts.Token);
        });
    }

    // ---- the confirmed crossing applies the same rules ------------------

    [Test]
    public async Task EnforceAsync_confirmed_read_grant_does_not_admit_a_write()
    {
        await using var world = await World.CreateAsync(TenantGrantState.Pending);
        world.TransitionGrant(TenantGrantState.Active);
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        world.AssertAdmitted(await world.ReadAsync(), "precondition: the confirmed grant admits a read");

        var decision = await world.ReadAsync(LatticeOperation.Write);

        Assert.That(decision.Allowed, Is.False, "a read-only grant confirmed from the registry never admits a write");
    }

    [Test]
    public async Task EnforceAsync_confirmed_crossing_is_gated_on_residency()
    {
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(true);
        residency.IsOnlineInServingRegion(Beta).Returns(false);
        await using var world = await World.CreateAsync(TenantGrantState.Pending, residency);
        world.TransitionGrant(TenantGrantState.Active);
        await world.PublishRegistryWriteWithRebuildHeldAsync();

        var decision = await world.ReadAsync();

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Does.Contain("not online"));
    }

    // ---- the steady state is untouched ----------------------------------

    [Test]
    public async Task EnforceAsync_authoritative_snapshot_completes_synchronously_without_reading_the_registry()
    {
        await using var world = await World.CreateAsync(TenantGrantState.Active);
        var reads = world.Registry.PointReads;
        LatticeActiveTenantContext.Current = Beta;
        var request = World.Request(LatticeOperation.Read);

        var pending = world.Enforcer.EnforceAsync(in request);

        Assert.That(pending.IsCompletedSuccessfully, Is.True, "the steady state never pays an async continuation");
        Assert.That(pending.Result.Allowed, Is.True);
        Assert.That(world.Registry.PointReads, Is.EqualTo(reads), "the steady state never reads the registry");
    }

    [Test]
    public async Task EnforceAsync_owned_tree_while_the_snapshot_is_not_authoritative_does_not_read_the_registry()
    {
        await using var world = await World.CreateAsync(TenantGrantState.Active);
        world.TransitionGrant(TenantGrantState.Revoked);
        await world.PublishRegistryWriteWithRebuildHeldAsync();
        var reads = world.Registry.PointReads;
        LatticeActiveTenantContext.Current = Beta;
        var request = new LatticeAccessRequest("t/beta/own", LatticeOperation.Read, new LatticeSubject(Subject), "k");

        var pending = world.Enforcer.EnforceAsync(in request);

        Assert.That(pending.IsCompletedSuccessfully, Is.True, "only a cross-tenant crossing is confirmed");
        Assert.That(pending.Result.Allowed, Is.True);
        Assert.That(world.Registry.PointReads, Is.EqualTo(reads));
    }

    // ---- helpers --------------------------------------------------------

    /// <summary>
    /// A maintainer that has built an (empty) snapshot but holds no lease, so it is
    /// warm yet not authoritative.
    /// </summary>
    private static CompiledTenantPolicySnapshotMaintainer NonAuthoritativePolicy()
    {
        var policy = TenantPolicyEpochTestCluster.Unleased(new FakeTenantRegistry());
        policy.RebuildNowAsync().GetAwaiter().GetResult();
        Assert.That(policy.IsSnapshotAuthoritative, Is.False, "precondition: a cold maintainer is not authoritative");
        return policy;
    }

    /// <summary>
    /// An enforcer over a cold (non-authoritative) maintainer and an engine that
    /// validates <c>bob</c> acting as <c>beta</c>, so a read of <c>acme</c>'s tree
    /// reaches the registry confirmation.
    /// </summary>
    private static TenantGateEnforcer CrossingEnforcer(ITenantRegistry registry)
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant(Subject, Beta).Returns(TenantAccessDecision.Allow());
        return new TenantGateEnforcer(
            engine,
            new NullTenantResidencyResolver(),
            NonAuthoritativePolicy(),
            registry,
            NullLogger<TenantGateEnforcer>.Instance);
    }

    /// <summary>
    /// The owner tenant <c>acme</c> sharing <c>t/acme/orders</c> with the grantee
    /// tenant <c>beta</c>, whose admin <c>bob</c> reads it with <c>beta</c> active,
    /// wired through the real maintainer, engine, and enforcer.
    /// </summary>
    private sealed class World : IAsyncDisposable
    {
        private readonly TenantRecord _owner;
        private long _tick = 1_000;

        private World(GatedTenantRegistry registry, TenantRecord owner, ITenantResidencyResolver? residency)
        {
            Registry = registry;
            _owner = owner;
            Cluster = new TenantPolicyEpochTestCluster();
            Maintainer = Cluster.AddSilo(registry);
            Engine = new LatticeTenantPolicyEngine(Maintainer);
            Enforcer = new TenantGateEnforcer(
                Engine,
                residency ?? new NullTenantResidencyResolver(),
                Maintainer,
                registry,
                NullLogger<TenantGateEnforcer>.Instance);
        }

        public GatedTenantRegistry Registry { get; }

        public TenantPolicyEpochTestCluster Cluster { get; }

        public CompiledTenantPolicySnapshotMaintainer Maintainer { get; }

        public LatticeTenantPolicyEngine Engine { get; }

        public TenantGateEnforcer Enforcer { get; }

        private CrossTenantGrant Grant => _owner.Grants.Single();

        public static async Task<World> CreateAsync(
            TenantGrantState initialState,
            ITenantResidencyResolver? residency = null)
        {
            var registry = new GatedTenantRegistry();
            var owner = Record(
                "acme",
                grants: [CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, SharedTree, TenantGrantOperations.Read, initialState)]);
            registry.Records.Add(owner);
            registry.Records.Add(Record("beta", admins: [Subject]));

            var world = new World(registry, owner, residency);
            world.Cluster.Renew(world.Maintainer);
            await world.Maintainer.BackgroundRebuild;
            await world.Maintainer.RebuildNowAsync();
            Assert.That(world.Maintainer.IsSnapshotAuthoritative, Is.True, "precondition: the warm snapshot is authoritative");
            return world;
        }

        public static LatticeAccessRequest Request(LatticeOperation operation) =>
            new(SharedTree, operation, new LatticeSubject(Subject), "k");

        public void TransitionGrant(TenantGrantState state) =>
            _owner.TransitionGrant(Grant.GrantId, state, Clock(++_tick), "test");

        public void RemoveGrant() => _owner.RemoveGrant(Grant.GrantId, Clock(++_tick), "test");

        /// <summary>
        /// Publishes the registry write to the maintainer exactly as the core
        /// write path does, with the rebuild's registry scan held on the gate, and
        /// asserts the resulting non-authoritative window.
        /// </summary>
        public async Task PublishRegistryWriteWithRebuildHeldAsync()
        {
            Registry.HoldScans();
            await Maintainer.OnMutationAsync(
                new LatticeMutation { TreeId = TenantTreeNames.RegistryTree },
                CancellationToken.None);
            Assert.That(Maintainer.IsSnapshotAuthoritative, Is.False, "precondition: the rebuild is outstanding");
        }

        /// <summary>Asserts the stale snapshot on its own would still admit the read.</summary>
        public void AssertSnapshotStillAdmits() =>
            Assert.That(
                Engine.ResolveCrossTenantGrant(Beta, Acme, SharedTree, TenantGrantOperations.Read).Allowed,
                Is.True,
                "precondition: the snapshot still holds the active grant, so trusting it would admit");

        public void AssertAdmitted(LatticeAccessDecision decision, string because) =>
            Assert.That(decision.Allowed, Is.True, $"{because}; denied with: {decision.Reason}");

        public async Task<LatticeAccessDecision> ReadAsync(LatticeOperation operation = LatticeOperation.Read)
        {
            LatticeActiveTenantContext.Current = Beta;
            var request = Request(operation);
            return await Enforcer.EnforceAsync(in request);
        }

        public async Task ReleaseRebuildAsync()
        {
            Registry.ReleaseScans();
            await Maintainer.BackgroundRebuild;
            Assert.That(Maintainer.IsSnapshotAuthoritative, Is.True, "the rebuild landed");
        }

        public async ValueTask DisposeAsync()
        {
            Registry.ReleaseScans();
            await Maintainer.BackgroundRebuild;
        }
    }

    /// <summary>
    /// A <see cref="FakeTenantRegistry"/> whose full scan (the rebuild's only
    /// registry call) can be held on a gate, while a point read answers at once
    /// with the committed record and is counted.
    /// </summary>
    private sealed class GatedTenantRegistry : ITenantRegistry
    {
        private readonly FakeTenantRegistry _inner = new();
        private TaskCompletionSource _gate = CompletedGate();

        public List<TenantRecord> Records => _inner.Records;

        public int PointReads { get; private set; }

        public void HoldScans() => _gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void ReleaseScans() => _gate.TrySetResult();

        public Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)
        {
            PointReads++;
            return _inner.GetAsync(tenant, cancellationToken);
        }

        public Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            _inner.ExistsAsync(tenant, cancellationToken);

        public async IAsyncEnumerable<TenantRecord> ListAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            await foreach (var record in _inner.ListAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return record;
            }
        }

        public Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default) =>
            _inner.PutAsync(record, cancellationToken);

        public Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            _inner.DeleteAsync(tenant, cancellationToken);

        private static TaskCompletionSource CompletedGate()
        {
            var gate = new TaskCompletionSource();
            gate.SetResult();
            return gate;
        }
    }
}
