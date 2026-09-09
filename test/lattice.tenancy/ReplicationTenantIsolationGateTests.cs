using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="ReplicationTenantIsolationGate"/>: the active
/// <see cref="IReplicationTenantIsolationGate"/> the tenancy add-on wires into the
/// inbound replication apply path. The tenant registry and the residency resolver
/// are substituted and the tree ownership is derived from the tree id by
/// <see cref="LatticeTenantTrees.GetOwner"/>, so every decision is exact and
/// timing-independent. Ids are chosen to exercise each ownership shape (platform,
/// system-internal, bare legacy, tenant-scoped).
/// </summary>
[TestFixture]
public sealed class ReplicationTenantIsolationGateTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private const string AcmeTree = "t/acme/orders";
    private const string LegacyTree = "app";
    private const string PlatformTree = "sys-foo";
    private const string SystemInternalTree = "_lattice_meta";
    private const string TenantRegistryTree = "sys-tenant-registry";

    private static ReplicationTenantIsolationGate CreateGate(
        ITenantRegistry registry,
        ITenantResidencyResolver? residency = null,
        CompiledTenantPolicySnapshotMaintainer? policy = null) =>
        new(registry,
            residency ?? new NullTenantResidencyResolver(),
            policy ?? EmptyPolicy());

    /// <summary>
    /// A maintainer whose snapshot is the empty compile, so every tenant misses the
    /// in-memory fast path and falls through to the authoritative registry. This is
    /// the shape the pre-existing cases below assume.
    /// </summary>
    private static CompiledTenantPolicySnapshotMaintainer EmptyPolicy() =>
        new(Substitute.For<ITenantRegistry>(),
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);

    /// <summary>
    /// A maintainer compiled over a registry containing <paramref name="tenants"/>,
    /// so those tenants are answered from memory with no registry round-trip.
    /// </summary>
    private static async Task<CompiledTenantPolicySnapshotMaintainer> CompiledPolicyAsync(
        params TenantId[] tenants)
    {
        var source = Substitute.For<ITenantRegistry>();
        source.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => ToAsync(tenants));

        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            source, NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        await maintainer.RebuildNowAsync();
        return maintainer;
    }

    private static async IAsyncEnumerable<TenantRecord> ToAsync(TenantId[] tenants)
    {
        foreach (var tenant in tenants)
        {
            yield return TenantRecord.Create(
                tenant,
                TenantStatus.Active,
                TenantQuotas.Unbounded,
                TenantPlacement.Shared,
                HybridLogicalClock.Zero,
                "test");
        }

        await Task.CompletedTask;
    }

    /// <summary>A registry record for <see cref="Acme"/> in the given status.</summary>
    private static TenantRecord Record(TenantStatus status) => TenantRecord.Create(
        Acme, status, TenantQuotas.Unbounded, TenantPlacement.Shared, HybridLogicalClock.Zero, "test");

    /// <summary>Stubs the registry so <see cref="Acme"/> exists and is active.</summary>
    private static void KnowsActive(ITenantRegistry registry) =>
        registry.GetAsync(Acme, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TenantRecord?>(Record(TenantStatus.Active)));

    /// <summary>Stubs the registry so <see cref="Acme"/> exists but is suspended.</summary>
    private static void KnowsSuspended(ITenantRegistry registry) =>
        registry.GetAsync(Acme, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TenantRecord?>(Record(TenantStatus.Suspended)));

    /// <summary>Stubs the registry so <see cref="Acme"/> does not exist.</summary>
    private static void KnowsNothing(ITenantRegistry registry) =>
        registry.GetAsync(Acme, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TenantRecord?>(null));

    /// <summary>
    /// A maintainer compiled over a registry containing one tenant in the given
    /// status, so that tenant is answered from the in-memory fast path.
    /// </summary>
    private static async Task<CompiledTenantPolicySnapshotMaintainer> CompiledPolicyWithStatusAsync(
        TenantId tenant,
        TenantStatus status)
    {
        var source = Substitute.For<ITenantRegistry>();
        source.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => ToAsyncWithStatus(tenant, status));

        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            source, NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        await maintainer.RebuildNowAsync();
        return maintainer;
    }

    private static async IAsyncEnumerable<TenantRecord> ToAsyncWithStatus(TenantId tenant, TenantStatus status)
    {
        yield return TenantRecord.Create(
            tenant, status, TenantQuotas.Unbounded, TenantPlacement.Shared, HybridLogicalClock.Zero, "test");
        await Task.CompletedTask;
    }

    [Test]
    public void IsActive_is_true_for_the_active_gate()
    {
        var gate = CreateGate(Substitute.For<ITenantRegistry>());

        Assert.That(gate.IsActive, Is.True);
    }

    [Test]
    public void EvaluateAsync_null_treeId_throws()
    {
        var gate = CreateGate(Substitute.For<ITenantRegistry>());

        Assert.That(
            async () => await gate.EvaluateAsync(null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    // ---- Platform / definition trees converge everywhere ----------------

    [Test]
    public async Task EvaluateAsync_platform_tree_admits_without_consulting_the_registry()
    {
        var registry = Substitute.For<ITenantRegistry>();
        var gate = CreateGate(registry);

        var decision = await gate.EvaluateAsync(PlatformTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        Assert.That(registry.ReceivedCalls(), Is.Empty, "a platform tree is not tenant data");
    }

    [Test]
    public async Task EvaluateAsync_system_internal_tree_admits_without_consulting_the_registry()
    {
        var registry = Substitute.For<ITenantRegistry>();
        var gate = CreateGate(registry);

        var decision = await gate.EvaluateAsync(SystemInternalTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        Assert.That(registry.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task EvaluateAsync_tenant_registry_definition_tree_admits_so_tenant_creates_converge()
    {
        // The tenant registry itself is a platform (sys-) tree, so replicated tenant
        // definitions converge everywhere independently of the data-isolation gate -
        // otherwise a tenant could never come to exist on a receiver from replication.
        var registry = Substitute.For<ITenantRegistry>();
        var gate = CreateGate(registry);

        var decision = await gate.EvaluateAsync(TenantRegistryTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        Assert.That(registry.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task EvaluateAsync_bare_legacy_tree_admits_without_consulting_the_registry()
    {
        // A bare, unsegmented legacy id is adopted by the reserved default tenant:
        // pre-tenancy global state, admitted unconditionally so existing trees keep
        // replicating exactly as before tenancy.
        var registry = Substitute.For<ITenantRegistry>();
        var gate = CreateGate(registry);

        var decision = await gate.EvaluateAsync(LegacyTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        Assert.That(registry.ReceivedCalls(), Is.Empty);
    }

    // ---- Real tenant trees: existence + residency -----------------------

    [Test]
    public async Task EvaluateAsync_existing_resident_tenant_admits()
    {
        var registry = Substitute.For<ITenantRegistry>();
        KnowsActive(registry);
        // Null residency default (IsActive false) => all regions allowed.
        var gate = CreateGate(registry);

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
    }

    [Test]
    public async Task EvaluateAsync_nonexistent_tenant_rejects_unknown_and_never_auto_creates()
    {
        var registry = Substitute.For<ITenantRegistry>();
        KnowsNothing(registry);
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(true);
        residency.IsOnlineInServingRegion(Acme).Returns(true);
        var gate = CreateGate(registry, residency);

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.RejectUnknownTenant));
        // Fail closed on existence before residency; never create a tenant, never
        // consult residency for a tenant that does not exist.
        await registry.DidNotReceiveWithAnyArgs().PutAsync(default!, default);
        residency.DidNotReceiveWithAnyArgs().IsOnlineInServingRegion(default);
    }

    [Test]
    public async Task EvaluateAsync_existing_tenant_offline_in_region_rejects_out_of_region()
    {
        var registry = Substitute.For<ITenantRegistry>();
        KnowsActive(registry);
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(true);
        residency.IsOnlineInServingRegion(Acme).Returns(false);
        var gate = CreateGate(registry, residency);

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.RejectOutOfRegion));
    }

    [Test]
    public async Task EvaluateAsync_existing_tenant_online_in_region_admits()
    {
        var registry = Substitute.For<ITenantRegistry>();
        KnowsActive(registry);
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(true);
        residency.IsOnlineInServingRegion(Acme).Returns(true);
        var gate = CreateGate(registry, residency);

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
    }

    [Test]
    public async Task EvaluateAsync_existing_tenant_with_null_residency_admits_all_regions()
    {
        // The null residency default (IsActive false) means residency is not yet
        // wired (until T20), so an existing tenant is admitted in every region.
        var registry = Substitute.For<ITenantRegistry>();
        KnowsActive(registry);
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(false);
        var gate = CreateGate(registry, residency);

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        // Residency is skipped entirely on the single IsActive bool read.
        residency.DidNotReceiveWithAnyArgs().IsOnlineInServingRegion(default);
    }

    // ---- Steady-state apply path is answered from memory ----------------

    [Test]
    public async Task EvaluateAsync_compiled_tenant_admits_without_a_registry_round_trip()
    {
        // The load-bearing case. Every inbound replicated entry for a tenant tree
        // used to make an uncached registry grain call here, so a single tenant's
        // replication stream could saturate the shared registry grain and slow
        // inbound convergence for every other tenant - on a path that is
        // deliberately not rate-limited and so has no other brake.
        var registry = Substitute.For<ITenantRegistry>();
        var gate = CreateGate(registry, policy: await CompiledPolicyAsync(Acme));

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        Assert.That(registry.ReceivedCalls(), Is.Empty,
            "a tenant present in the compiled snapshot must not cost a registry grain call");
    }

    [Test]
    public async Task EvaluateAsync_compiled_tenant_offline_in_region_still_rejects_out_of_region()
    {
        // The memory fast path must apply exactly the same residency rule as the
        // registry path; short-circuiting existence must not short-circuit isolation.
        var registry = Substitute.For<ITenantRegistry>();
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(true);
        residency.IsOnlineInServingRegion(Acme).Returns(false);
        var gate = CreateGate(registry, residency, await CompiledPolicyAsync(Acme));

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.RejectOutOfRegion));
        Assert.That(registry.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task EvaluateAsync_tenant_absent_from_snapshot_falls_back_to_the_registry()
    {
        // Fail-closed: a snapshot miss is "not yet compiled", never "does not
        // exist". A tenant created moments ago must still be admitted, so the
        // authoritative registry is consulted rather than the write being refused.
        var registry = Substitute.For<ITenantRegistry>();
        KnowsActive(registry);
        var gate = CreateGate(registry, policy: await CompiledPolicyAsync(TenantId.Parse("other")));

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        await registry.Received(1).GetAsync(Acme, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EvaluateAsync_unknown_tenant_absent_from_snapshot_still_rejects()
    {
        var registry = Substitute.For<ITenantRegistry>();
        KnowsNothing(registry);
        var gate = CreateGate(registry, policy: await CompiledPolicyAsync(TenantId.Parse("other")));

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.RejectUnknownTenant));
    }
    // ---- Snapshot staleness (security review F3) -------------------------
    //
    // The compiled-snapshot fast path replaced a registry read on every inbound
    // apply. Rebuilds are background and coalesced, and the rebuild loop keeps the
    // previous snapshot when one fails - so a tenant deleted from the registry went
    // on being admitted for the whole rebuild lag, and indefinitely if rebuilds
    // kept failing. That is a deny-to-allow regression: a peer region shipping for
    // a revoked tenant would be applied.
    //
    // Age is the wrong staleness signal here, because rebuilds are mutation-driven
    // rather than periodic: on a quiet estate an hours-old snapshot is exactly
    // correct, and an age bound would reintroduce the per-apply registry call the
    // fast path exists to remove. The signal used instead is an outstanding or
    // failing rebuild, since a tenant deletion always schedules one.

    /// <summary>
    /// The regression proper: a tenant present in a compiled snapshot but since
    /// deleted from the registry is refused once the snapshot stops being
    /// authoritative, rather than being admitted from the stale compile.
    /// </summary>
    [Test]
    public async Task Deleted_tenant_is_refused_when_the_snapshot_is_not_authoritative()
    {
        var policy = await FailingPolicyAsync(Acme);
        Assert.That(policy.IsSnapshotAuthoritative, Is.False, "guard: the snapshot must be non-authoritative");
        Assert.That(policy.Current.TryGetTenant(Acme.Value!, out _), Is.True, "guard: the stale compile still holds acme");

        // The authoritative registry no longer knows the tenant.
        var registry = Substitute.For<ITenantRegistry>();
        KnowsNothing(registry);
        var gate = CreateGate(registry, policy: policy);

        var decision = await gate.EvaluateAsync(AcmeTree, CancellationToken.None);

        Assert.That(
            decision,
            Is.EqualTo(ReplicationTenantIsolationDecision.RejectUnknownTenant),
            "a stale snapshot must not keep a deleted tenant admitted");
        await registry.Received(1).GetAsync(Acme, Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// The converse: while the snapshot is authoritative the fast path still
    /// answers from memory, so the fallback costs nothing on the warm path.
    /// </summary>
    [Test]
    public async Task Authoritative_snapshot_answers_without_touching_the_registry()
    {
        var policy = await CompiledPolicyAsync(Acme);
        Assert.That(policy.IsSnapshotAuthoritative, Is.True);

        var registry = Substitute.For<ITenantRegistry>();
        var gate = CreateGate(registry, policy: policy);

        var decision = await gate.EvaluateAsync(AcmeTree, CancellationToken.None);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        await registry.DidNotReceive().GetAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// A non-authoritative snapshot still fails closed for a tenant the registry
    /// also does not know: the fallback is a registry consult, not a default-allow.
    /// </summary>
    [Test]
    public async Task Non_authoritative_snapshot_still_admits_a_tenant_the_registry_knows()
    {
        var policy = await FailingPolicyAsync(Acme);
        var registry = Substitute.For<ITenantRegistry>();
        KnowsActive(registry);
        var gate = CreateGate(registry, policy: policy);

        var decision = await gate.EvaluateAsync(AcmeTree, CancellationToken.None);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
    }

    // ---- Tenant lifecycle status binds the apply path too -----------------
    //
    // The gate asked only whether a tenant EXISTS, never whether it is admissible.
    // The authoring path has always refused a non-Active tenant
    // (LatticeTenantPolicyEngine.ValidateActiveTenant), so suspension was a
    // one-sided control: an operator suspends a tenant, every local write is
    // refused, and the tenant's data goes on changing anyway from any peer region
    // still shipping for it. Both halves of the decision - the compiled fast path
    // and the authoritative registry fallback - must apply the same rule.

    /// <summary>
    /// The regression proper, on the fallback half: a tenant the registry knows but
    /// has suspended is refused rather than admitted on existence alone.
    /// </summary>
    [Test]
    public async Task Suspended_tenant_is_refused_by_the_registry_fallback()
    {
        var registry = Substitute.For<ITenantRegistry>();
        KnowsSuspended(registry);
        var gate = CreateGate(registry, policy: await CompiledPolicyAsync(TenantId.Parse("other")));

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(
            decision,
            Is.EqualTo(ReplicationTenantIsolationDecision.RejectSuspendedTenant),
            "a suspended tenant's inbound shipping must not be applied");
    }

    /// <summary>
    /// The same rule on the compiled fast path, which answers the overwhelming
    /// majority of applies: short-circuiting the registry read must not
    /// short-circuit the status check.
    /// </summary>
    [Test]
    public async Task Suspended_tenant_is_refused_on_the_compiled_fast_path()
    {
        var registry = Substitute.For<ITenantRegistry>();
        var policy = await CompiledPolicyWithStatusAsync(Acme, TenantStatus.Suspended);
        Assert.That(policy.IsSnapshotAuthoritative, Is.True, "guard: the fast path must be the one under test");
        var gate = CreateGate(registry, policy: policy);

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.RejectSuspendedTenant));
        Assert.That(registry.ReceivedCalls(), Is.Empty, "the fast path must stay allocation- and round-trip-free");
    }

    /// <summary>
    /// A suspended tenant is refused for its status, not silently reclassified as
    /// unknown or out-of-region: the three refusals carry different reason tags and
    /// different operator remedies, so they must stay distinguishable.
    /// </summary>
    [Test]
    public async Task Suspended_tenant_is_not_reported_as_unknown_or_out_of_region()
    {
        var registry = Substitute.For<ITenantRegistry>();
        KnowsSuspended(registry);
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(true);
        residency.IsOnlineInServingRegion(Acme).Returns(true);
        var gate = CreateGate(registry, residency);

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.Multiple(() =>
        {
            Assert.That(decision, Is.Not.EqualTo(ReplicationTenantIsolationDecision.Admit));
            Assert.That(decision, Is.Not.EqualTo(ReplicationTenantIsolationDecision.RejectUnknownTenant));
            Assert.That(decision, Is.Not.EqualTo(ReplicationTenantIsolationDecision.RejectOutOfRegion));
        });
    }

    /// <summary>
    /// The converse control: an active tenant is still admitted, so the status
    /// check narrows nothing it should not.
    /// </summary>
    [Test]
    public async Task Active_tenant_is_still_admitted_on_both_halves()
    {
        var fallbackRegistry = Substitute.For<ITenantRegistry>();
        KnowsActive(fallbackRegistry);
        var fallbackGate = CreateGate(
            fallbackRegistry, policy: await CompiledPolicyAsync(TenantId.Parse("other")));

        var fastGate = CreateGate(
            Substitute.For<ITenantRegistry>(),
            policy: await CompiledPolicyWithStatusAsync(Acme, TenantStatus.Active));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                await fallbackGate.EvaluateAsync(AcmeTree),
                Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
            Assert.That(
                await fastGate.EvaluateAsync(AcmeTree),
                Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        });
    }

    /// <summary>
    /// Platform and legacy trees sit outside every tenant namespace, so the status
    /// check must not reach them: a suspended tenant cannot stop system definitions
    /// or pre-tenancy trees from converging.
    /// </summary>
    [Test]
    public async Task Status_check_does_not_reach_platform_or_legacy_trees()
    {
        var registry = Substitute.For<ITenantRegistry>();
        KnowsSuspended(registry);
        var gate = CreateGate(registry);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                await gate.EvaluateAsync(PlatformTree),
                Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
            Assert.That(
                await gate.EvaluateAsync(LegacyTree),
                Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        });
    }

    [Test]
    public void Constructor_null_policy_throws() =>
        Assert.That(
            () => new ReplicationTenantIsolationGate(
                Substitute.For<ITenantRegistry>(), new NullTenantResidencyResolver(), null!),
            Throws.ArgumentNullException);
    /// <summary>
    /// Builds a maintainer holding a good compile of <paramref name="tenants"/>
    /// whose subsequent rebuild has failed, so the snapshot is populated but no
    /// longer authoritative. Deterministic: the scheduled rebuild is awaited.
    /// </summary>
    private static async Task<CompiledTenantPolicySnapshotMaintainer> FailingPolicyAsync(
        params TenantId[] tenants)
    {
        var scans = 0;
        var source = Substitute.For<ITenantRegistry>();
        source.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            Interlocked.Increment(ref scans) == 1 ? ToAsync(tenants) : Throwing());

        var maintainer = new CompiledTenantPolicySnapshotMaintainer(
            source, NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);

        // First scan succeeds: the snapshot is populated and authoritative.
        await maintainer.RebuildNowAsync();

        // A registry mutation schedules a rebuild; the second scan throws, so the
        // maintainer keeps the previous snapshot and records the failure.
        await maintainer.OnMutationAsync(
            new LatticeMutation { TreeId = TenantTreeNames.RegistryTree }, CancellationToken.None);
        await maintainer.BackgroundRebuild;

        return maintainer;
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
}