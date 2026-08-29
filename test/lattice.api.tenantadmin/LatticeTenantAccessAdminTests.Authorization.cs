using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// The full authorization matrix for <see cref="LatticeTenantAccessAdmin"/>.
/// Admin-subject management is a <b>tenant-tier</b> action: authorized for a
/// platform operator <em>or</em> a live admin subject of that specific tenant, and
/// denied to everyone else - including an admin subject of a <em>different</em>
/// tenant and an anonymous caller. Every case is driven through
/// <see cref="DefaultEffectAllowGate"/>, which faithfully models the real core gate
/// under <c>DefaultEffect = Allow</c> (control-plane isolation on the reserved
/// policy tree, fail-open only on data-plane scopes), so these tests prove the tier
/// holds even on the most permissive realistic cluster.
/// </summary>
/// <remarks>
/// The last test pins the boundary this whole issue depends on: the same tenant
/// admin that may manage its own tenant's membership must <b>not</b> be able to
/// drive an operator-tier lifecycle mutation such as
/// <see cref="ILatticeTenantAdmin.SetTenantQuotasAsync"/>. Wiring the wrong
/// identically-named <c>AuthorizeTenantAdminAsync</c> would break exactly one of
/// these two directions, so both are asserted.
/// </remarks>
public sealed partial class LatticeTenantAccessAdminTests
{
    private const string Operator = "platform-operator";
    private const string AcmeAdmin = "alice@acme.example";
    private const string OtherTenant = "globex";
    private const string OtherTenantAdmin = "mallory@globex.example";

    /// <summary>
    /// A registry seeded with two independent tenants, each with its own admin
    /// subject, so a cross-tenant escalation attempt has a real second tenant to
    /// be an admin of.
    /// </summary>
    private static FakeTenantRegistry TwoTenantRegistry()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, AcmeAdmin, "bob@acme.example"));
        registry.Seed(SeededRecord(OtherTenant, OtherTenantAdmin));
        return registry;
    }

    /// <summary>
    /// Builds the facade as seen by <paramref name="callerSubjectId"/>, with the
    /// control-plane-faithful gate that grants operator authority only to
    /// <see cref="Operator"/>. Passing <see langword="null"/> registers no
    /// membership context, so the caller resolves to anonymous.
    /// </summary>
    private static LatticeTenantAccessAdmin AdminAs(FakeTenantRegistry registry, string? callerSubjectId) =>
        new(
            registry,
            new TenantRegionResidencyAuthorizer(
                new DefaultEffectAllowGate(policyTreeAdminSubjectId: Operator),
                registry,
                callerSubjectId is null ? null : new FixedMembershipContext(new LatticeSubject(callerSubjectId))),
            new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }));

    // ---- tier 1: the platform operator -----------------------------------

    [Test]
    public async Task An_operator_may_list_add_and_remove_on_any_tenant()
    {
        var registry = TwoTenantRegistry();
        var admin = AdminAs(registry, Operator);

        var listed = await admin.ListAdminSubjectsAsync(Tenant);
        var added = await admin.AddAdminSubjectAsync(Tenant, "carol@acme.example");
        var removed = await admin.RemoveAdminSubjectAsync(Tenant, "bob@acme.example");

        Assert.Multiple(() =>
        {
            Assert.That(listed.Subjects, Does.Contain(AcmeAdmin));
            Assert.That(added.Changed, Is.True);
            Assert.That(removed.Changed, Is.True);
            Assert.That(
                registry.Peek(Tenant)!.AdminSubjects,
                Is.EqualTo(new[] { AcmeAdmin, "carol@acme.example" }));
        });
    }

    [Test]
    public void An_operator_naming_an_unregistered_tenant_is_told_not_found()
    {
        // The operator is trusted to learn whether a tenant exists.
        var admin = AdminAs(TwoTenantRegistry(), Operator);

        Assert.That(
            async () => await admin.ListAdminSubjectsAsync("ghost"),
            Throws.TypeOf<TenantNotFoundException>());
    }

    // ---- tier 2: a live admin subject of that same tenant -----------------

    [Test]
    public async Task A_tenant_admin_may_list_its_own_tenants_subjects()
    {
        var admin = AdminAs(TwoTenantRegistry(), AcmeAdmin);

        var report = await admin.ListAdminSubjectsAsync(Tenant);

        Assert.That(report.Subjects, Is.EqualTo(new[] { AcmeAdmin, "bob@acme.example" }));
    }

    [Test]
    public async Task A_tenant_admin_may_add_a_subject_to_its_own_tenant()
    {
        var registry = TwoTenantRegistry();
        var admin = AdminAs(registry, AcmeAdmin);

        var result = await admin.AddAdminSubjectAsync(Tenant, "carol@acme.example");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject("carol@acme.example"), Is.True);
        });
    }

    [Test]
    public async Task A_tenant_admin_may_remove_a_subject_from_its_own_tenant()
    {
        var registry = TwoTenantRegistry();
        var admin = AdminAs(registry, AcmeAdmin);

        var result = await admin.RemoveAdminSubjectAsync(Tenant, "bob@acme.example");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject("bob@acme.example"), Is.False);
        });
    }

    [Test]
    public async Task A_tenant_admin_may_remove_itself_while_another_admin_remains()
    {
        var registry = TwoTenantRegistry();
        var admin = AdminAs(registry, AcmeAdmin);

        var result = await admin.RemoveAdminSubjectAsync(Tenant, AcmeAdmin);

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Subjects, Is.EqualTo(new[] { "bob@acme.example" }));
        });
    }

    // ---- tier 2 negative: an admin subject of a DIFFERENT tenant ----------

    [Test]
    public void A_tenant_admin_of_another_tenant_may_not_list_this_tenants_subjects()
    {
        var admin = AdminAs(TwoTenantRegistry(), OtherTenantAdmin);

        Assert.That(
            async () => await admin.ListAdminSubjectsAsync(Tenant),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void A_tenant_admin_of_another_tenant_may_not_add_a_subject_here()
    {
        var registry = TwoTenantRegistry();
        var admin = AdminAs(registry, OtherTenantAdmin);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.AddAdminSubjectAsync(Tenant, OtherTenantAdmin),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject(OtherTenantAdmin), Is.False,
                "A cross-tenant caller must not be able to grant itself authority here.");
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public void A_tenant_admin_of_another_tenant_may_not_remove_a_subject_here()
    {
        var registry = TwoTenantRegistry();
        var admin = AdminAs(registry, OtherTenantAdmin);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.RemoveAdminSubjectAsync(Tenant, AcmeAdmin),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject(AcmeAdmin), Is.True);
        });
    }

    [Test]
    public void A_tenant_admin_of_another_tenant_is_told_denied_not_not_found_for_a_missing_tenant()
    {
        // Fail-closed posture: a non-operator must never learn whether a tenant
        // exists, so a missing tenant and an unauthorized one are indistinguishable.
        var admin = AdminAs(TwoTenantRegistry(), OtherTenantAdmin);

        Assert.That(
            async () => await admin.ListAdminSubjectsAsync("ghost"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "Tenant existence must not be probeable through this surface.");
    }

    [Test]
    public void A_removed_admin_subject_immediately_loses_authority()
    {
        // Authority is re-derived from the record on every call, never cached or
        // taken from the wire, so a revoked subject is denied on its next attempt.
        var registry = TwoTenantRegistry();
        registry.Peek(Tenant)!.RemoveAdminSubject(AcmeAdmin, new HybridLogicalClock { WallClockTicks = 900 }, "seed");
        var admin = AdminAs(registry, AcmeAdmin);

        Assert.That(
            async () => await admin.ListAdminSubjectsAsync(Tenant),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ---- tier 3 negative: anonymous ---------------------------------------

    [Test]
    public void An_anonymous_caller_may_not_list_add_or_remove()
    {
        var registry = TwoTenantRegistry();
        var admin = AdminAs(registry, callerSubjectId: null);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.ListAdminSubjectsAsync(Tenant),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await admin.AddAdminSubjectAsync(Tenant, "carol@acme.example"),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await admin.RemoveAdminSubjectAsync(Tenant, AcmeAdmin),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public void A_caller_that_is_no_tenants_admin_may_not_list()
    {
        var admin = AdminAs(TwoTenantRegistry(), "stranger@example.com");

        Assert.That(
            async () => await admin.ListAdminSubjectsAsync(Tenant),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ---- the tenant-tier / operator-tier boundary -------------------------

    [Test]
    public void A_tenant_admin_that_may_manage_membership_may_not_set_quotas()
    {
        // The boundary this issue turns on. The same caller is authorized for the
        // tenant-tier membership surface and denied on the operator-tier lifecycle
        // surface, proving admin-subject management is wired to the two-tier
        // TenantRegionResidencyAuthorizer and not to the operator-only
        // TenantAdminAccessAuthorizer (whose identically-named method means the
        // opposite thing).
        var registry = TwoTenantRegistry();
        var gate = new DefaultEffectAllowGate(policyTreeAdminSubjectId: Operator);
        var membership = new FixedMembershipContext(new LatticeSubject(AcmeAdmin));

        var accessAdmin = new LatticeTenantAccessAdmin(
            registry,
            new TenantRegionResidencyAuthorizer(gate, registry, membership),
            new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }));

        var lifecycleAdmin = new LatticeTenantAdmin(
            registry,
            new TenantAdminAccessAuthorizer(gate, membership),
            new IncrementingClock(),
            new StubCascade(0),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }),
            membership);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await accessAdmin.ListAdminSubjectsAsync(Tenant),
                Throws.Nothing,
                "A live admin subject holds the tenant tier.");
            Assert.That(
                async () => await accessAdmin.AddAdminSubjectAsync(Tenant, "carol@acme.example"),
                Throws.Nothing,
                "A live admin subject holds the tenant tier.");
            Assert.That(
                async () => await lifecycleAdmin.SetTenantQuotasAsync(Tenant, TenantQuotasDescriptor.Unbounded),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "The tenant tier must never reach an operator-tier lifecycle mutation.");
            Assert.That(
                async () => await lifecycleAdmin.SuspendTenantAsync(Tenant),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "The tenant tier must never reach an operator-tier lifecycle mutation.");
            Assert.That(
                async () => await lifecycleAdmin.DeleteTenantAsync(Tenant),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "The tenant tier must never reach an operator-tier lifecycle mutation.");
        });
    }

    [Test]
    public async Task An_operator_holds_both_tiers()
    {
        // The inverse direction: the operator tier subsumes the tenant tier, so a
        // platform operator drives membership without being an admin subject of the
        // tenant. Proves the tenant-tier wiring did not accidentally exclude the
        // operator.
        var registry = TwoTenantRegistry();
        var gate = new DefaultEffectAllowGate(policyTreeAdminSubjectId: Operator);
        var membership = new FixedMembershipContext(new LatticeSubject(Operator));

        var accessAdmin = new LatticeTenantAccessAdmin(
            registry,
            new TenantRegionResidencyAuthorizer(gate, registry, membership),
            new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }));

        var lifecycleAdmin = new LatticeTenantAdmin(
            registry,
            new TenantAdminAccessAuthorizer(gate, membership),
            new IncrementingClock(),
            new StubCascade(0),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }),
            membership);

        var added = await accessAdmin.AddAdminSubjectAsync(Tenant, "carol@acme.example");
        var quotas = await lifecycleAdmin.SetTenantQuotasAsync(Tenant, TenantQuotasDescriptor.Unbounded);

        Assert.Multiple(() =>
        {
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject(Operator), Is.False,
                "The operator is not an admin subject of the tenant it just administered.");
            Assert.That(added.Changed, Is.True);
            Assert.That(quotas.TenantId, Is.EqualTo(Tenant));
        });
    }

    [Test]
    public void The_denial_message_names_the_admin_subject_surface()
    {
        // The reused authorizer must report the surface the caller was actually
        // refused on, not the region-residency surface it was first written for.
        var admin = AdminAs(TwoTenantRegistry(), OtherTenantAdmin);

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await admin.ListAdminSubjectsAsync(Tenant));

        Assert.That(ex!.Message, Does.Contain("admin subjects"));
    }
}
