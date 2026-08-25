using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantAdmin"/>, the single narrowest seam at
/// which every tenant lifecycle operation (create, suspend, resume, delete with
/// cascade) is authorized fail-closed and applied to the tenant registry. Driven
/// purely with an in-memory registry, hand-written access gates, a strictly
/// increasing clock, and a stub cascade - no cluster, no timing, no ordering
/// assumptions.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminTests
{
    private const string Tenant = "acme";

    private static TenantId Parse(string id)
    {
        Assert.That(TenantId.TryParse(id, out var tenant), Is.True, $"'{id}' should parse.");
        return tenant;
    }

    private static LatticeTenantAdmin Create(
        FakeTenantRegistry registry,
        bool allow = true,
        ITenantAdminClock? clock = null,
        ITenantTreeCascade? cascade = null)
        => new(
            registry,
            new TenantAdminAccessAuthorizer(new FixedGate(allow)),
            clock ?? new IncrementingClock(),
            cascade ?? new StubCascade(0),
            Options.Create(new ClusterOptions()));

    private static TenantRecord ActiveRecord(string id) => TenantRecord.Create(
        Parse(id), TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared,
        HybridLogicalClock.Tick(HybridLogicalClock.Zero), "seed");

    private static TenantRecord SuspendedRecord(string id)
    {
        var record = ActiveRecord(id);
        record.SetStatus(TenantStatus.Suspended, HybridLogicalClock.Tick(HybridLogicalClock.Tick(HybridLogicalClock.Zero)), "seed");
        return record;
    }

    // ----- Create -----

    [Test]
    public async Task CreateTenantAsync_registers_a_new_active_tenant()
    {
        var registry = new FakeTenantRegistry();
        var facade = Create(registry);

        var result = await facade.CreateTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo(Tenant));
            Assert.That(result.Status, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(registry.Contains(Tenant), Is.True);
            Assert.That(registry.Peek(Tenant)!.Status, Is.EqualTo(TenantStatus.Active));
            Assert.That(registry.Puts, Is.EqualTo(1));
        });
    }

    [Test]
    public void CreateTenantAsync_of_a_duplicate_fails_closed()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(Tenant));
        var facade = Create(registry);

        Assert.That(async () => await facade.CreateTenantAsync(Tenant),
            Throws.TypeOf<TenantAlreadyExistsException>());
        Assert.That(registry.Puts, Is.EqualTo(0), "A rejected create must not write.");
    }

    [Test]
    public void CreateTenantAsync_when_unauthorized_is_denied_before_touching_the_registry()
    {
        var registry = new FakeTenantRegistry();
        var facade = Create(registry, allow: false);

        Assert.That(async () => await facade.CreateTenantAsync(Tenant),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(registry.Puts, Is.EqualTo(0));
    }

    [Test]
    public void CreateTenantAsync_rejects_a_null_or_empty_tenant_id()
    {
        var facade = Create(new FakeTenantRegistry());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.CreateTenantAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await facade.CreateTenantAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    // ----- Suspend -----

    [Test]
    public async Task SuspendTenantAsync_transitions_an_active_tenant_to_suspended()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(Tenant));
        var facade = Create(registry);

        var result = await facade.SuspendTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(result.PreviousStatus, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(result.NewStatus, Is.EqualTo(TenantLifecycleStatus.Suspended));
            Assert.That(result.Changed, Is.True);
            Assert.That(registry.Peek(Tenant)!.Status, Is.EqualTo(TenantStatus.Suspended));
        });
    }

    [Test]
    public async Task SuspendTenantAsync_of_an_already_suspended_tenant_is_an_idempotent_no_op()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SuspendedRecord(Tenant));
        var facade = Create(registry);

        var result = await facade.SuspendTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.False);
            Assert.That(result.NewStatus, Is.EqualTo(TenantLifecycleStatus.Suspended));
            Assert.That(registry.Puts, Is.EqualTo(0), "An idempotent no-op must not write.");
        });
    }

    [Test]
    public void SuspendTenantAsync_of_an_unknown_tenant_fails_closed()
    {
        var facade = Create(new FakeTenantRegistry());

        Assert.That(async () => await facade.SuspendTenantAsync(Tenant),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [Test]
    public void SuspendTenantAsync_of_the_reserved_default_tenant_is_rejected()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(TenantId.DefaultId));
        var facade = Create(registry);

        Assert.That(async () => await facade.SuspendTenantAsync(TenantId.DefaultId),
            Throws.TypeOf<ReservedTenantOperationException>());
        Assert.That(registry.Puts, Is.EqualTo(0));
    }

    [Test]
    public void SuspendTenantAsync_when_unauthorized_is_denied()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(Tenant));
        var facade = Create(registry, allow: false);

        Assert.That(async () => await facade.SuspendTenantAsync(Tenant),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(registry.Puts, Is.EqualTo(0));
    }

    // ----- Resume -----

    [Test]
    public async Task ResumeTenantAsync_transitions_a_suspended_tenant_to_active()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SuspendedRecord(Tenant));
        var facade = Create(registry);

        var result = await facade.ResumeTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(result.PreviousStatus, Is.EqualTo(TenantLifecycleStatus.Suspended));
            Assert.That(result.NewStatus, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(result.Changed, Is.True);
            Assert.That(registry.Peek(Tenant)!.Status, Is.EqualTo(TenantStatus.Active));
        });
    }

    [Test]
    public async Task ResumeTenantAsync_of_an_already_active_tenant_is_an_idempotent_no_op()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(Tenant));
        var facade = Create(registry);

        var result = await facade.ResumeTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.False);
            Assert.That(result.NewStatus, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(registry.Puts, Is.EqualTo(0));
        });
    }

    [Test]
    public void ResumeTenantAsync_of_an_unknown_tenant_fails_closed()
    {
        var facade = Create(new FakeTenantRegistry());

        Assert.That(async () => await facade.ResumeTenantAsync(Tenant),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [Test]
    public async Task ResumeTenantAsync_of_the_reserved_default_tenant_is_allowed_as_an_active_no_op()
    {
        // Resume is meaningless on a tenant that can never be suspended, but it is
        // not itself a forbidden operation: the default tenant is always active, so
        // a resume is an idempotent no-op rather than a ReservedTenantOperation.
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(TenantId.DefaultId));
        var facade = Create(registry);

        var result = await facade.ResumeTenantAsync(TenantId.DefaultId);

        Assert.That(result.Changed, Is.False);
    }

    // ----- Delete (with cascade) -----

    [Test]
    public async Task DeleteTenantAsync_cascades_the_trees_then_removes_the_registry_record()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(Tenant));
        var cascade = new StubCascade(count: 3);
        var facade = Create(registry, cascade: cascade);

        var result = await facade.DeleteTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo(Tenant));
            Assert.That(result.CascadedTreeCount, Is.EqualTo(3));
            Assert.That(cascade.Calls, Is.EqualTo(1));
            Assert.That(cascade.LastTenant!.Value.Value, Is.EqualTo(Tenant));
            Assert.That(registry.Contains(Tenant), Is.False);
            Assert.That(registry.Deletes, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task DeleteTenantAsync_of_a_tenant_with_no_trees_reports_zero_cascaded()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(Tenant));
        var facade = Create(registry, cascade: new StubCascade(count: 0));

        var result = await facade.DeleteTenantAsync(Tenant);

        Assert.That(result.CascadedTreeCount, Is.EqualTo(0));
        Assert.That(registry.Contains(Tenant), Is.False);
    }

    [Test]
    public async Task DeleteTenantAsync_suspends_the_tenant_before_cascading_its_trees()
    {
        // Fail-closed ordering: the tenant must be blocked from new admissions
        // (suspended) before any tree is enumerated or deleted, so a create racing
        // the delete cannot slip a fresh tree in under a definition about to be
        // removed. The observing cascade snapshots the registry status at the
        // instant it runs.
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(Tenant));
        var cascade = new StatusObservingCascade(registry, count: 2);
        var facade = Create(registry, cascade: cascade);

        var result = await facade.DeleteTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(cascade.ObservedStatusAtCascade, Is.EqualTo(TenantStatus.Suspended),
                "The tenant must already be suspended when the cascade runs.");
            Assert.That(result.CascadedTreeCount, Is.EqualTo(2));
            Assert.That(registry.Contains(Tenant), Is.False, "The registry record is removed last.");
            Assert.That(registry.Deletes, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task DeleteTenantAsync_of_an_already_suspended_tenant_still_removes_it()
    {
        // A re-run of an interrupted delete (tenant left suspended) must complete
        // idempotently: still cascade and remove the registry record.
        var registry = new FakeTenantRegistry();
        registry.Seed(SuspendedRecord(Tenant));
        var cascade = new StatusObservingCascade(registry, count: 1);
        var facade = Create(registry, cascade: cascade);

        var result = await facade.DeleteTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(cascade.ObservedStatusAtCascade, Is.EqualTo(TenantStatus.Suspended));
            Assert.That(result.CascadedTreeCount, Is.EqualTo(1));
            Assert.That(registry.Contains(Tenant), Is.False);
        });
    }

    [Test]
    public void DeleteTenantAsync_of_an_unknown_tenant_fails_closed_without_cascading()
    {
        var registry = new FakeTenantRegistry();
        var cascade = new StubCascade(count: 5);
        var facade = Create(registry, cascade: cascade);

        Assert.That(async () => await facade.DeleteTenantAsync(Tenant),
            Throws.TypeOf<TenantNotFoundException>());
        Assert.Multiple(() =>
        {
            Assert.That(cascade.Calls, Is.EqualTo(0), "An unknown tenant must not cascade.");
            Assert.That(registry.Deletes, Is.EqualTo(0));
        });
    }

    [Test]
    public void DeleteTenantAsync_of_the_reserved_default_tenant_is_rejected_without_cascading()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(TenantId.DefaultId));
        var cascade = new StubCascade(count: 5);
        var facade = Create(registry, cascade: cascade);

        Assert.That(async () => await facade.DeleteTenantAsync(TenantId.DefaultId),
            Throws.TypeOf<ReservedTenantOperationException>());
        Assert.Multiple(() =>
        {
            Assert.That(cascade.Calls, Is.EqualTo(0));
            Assert.That(registry.Deletes, Is.EqualTo(0));
            Assert.That(registry.Contains(TenantId.DefaultId), Is.True);
        });
    }

    [Test]
    public void DeleteTenantAsync_when_unauthorized_is_denied_without_cascading()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(ActiveRecord(Tenant));
        var cascade = new StubCascade(count: 5);
        var facade = Create(registry, allow: false, cascade: cascade);

        Assert.That(async () => await facade.DeleteTenantAsync(Tenant),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.Multiple(() =>
        {
            Assert.That(cascade.Calls, Is.EqualTo(0));
            Assert.That(registry.Deletes, Is.EqualTo(0));
        });
    }

    // ----- Constructor guards -----

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantAdminAccessAuthorizer(new FixedGate(true));
        var clock = new IncrementingClock();
        var cascade = new StubCascade(0);
        var options = Options.Create(new ClusterOptions());

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeTenantAdmin(null!, authorizer, clock, cascade, options), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdmin(registry, null!, clock, cascade, options), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdmin(registry, authorizer, null!, cascade, options), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdmin(registry, authorizer, clock, null!, options), Throws.ArgumentNullException);
            Assert.That(() => new LatticeTenantAdmin(registry, authorizer, clock, cascade, null!), Throws.ArgumentNullException);
        });
    }
}
