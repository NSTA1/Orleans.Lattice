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
        ITenantResidencyResolver? residency = null) =>
        new(registry, residency ?? new NullTenantResidencyResolver());

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
        registry.ExistsAsync(Acme, Arg.Any<CancellationToken>()).Returns(true);
        // Null residency default (IsActive false) => all regions allowed.
        var gate = CreateGate(registry);

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
    }

    [Test]
    public async Task EvaluateAsync_nonexistent_tenant_rejects_unknown_and_never_auto_creates()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ExistsAsync(Acme, Arg.Any<CancellationToken>()).Returns(false);
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
        registry.ExistsAsync(Acme, Arg.Any<CancellationToken>()).Returns(true);
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
        registry.ExistsAsync(Acme, Arg.Any<CancellationToken>()).Returns(true);
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
        registry.ExistsAsync(Acme, Arg.Any<CancellationToken>()).Returns(true);
        var residency = Substitute.For<ITenantResidencyResolver>();
        residency.IsActive.Returns(false);
        var gate = CreateGate(registry, residency);

        var decision = await gate.EvaluateAsync(AcmeTree);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
        // Residency is skipped entirely on the single IsActive bool read.
        residency.DidNotReceiveWithAnyArgs().IsOnlineInServingRegion(default);
    }
}
