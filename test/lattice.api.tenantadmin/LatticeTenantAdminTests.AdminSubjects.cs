using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for the admin-subject seeding the tenant-create path performs.
/// Tenant visibility on the read-only self-service surface resolves from
/// admin-subject membership, so a tenant created with no subjects would be
/// mutable-but-invisible; these tests pin the explicit-set, caller-seeding, and
/// unresolvable-caller behaviours, plus the fail-closed validation of the
/// supplied set. Driven purely with an in-memory registry, a hand-written gate,
/// and a fixed membership context - no cluster, no timing.
/// </summary>
public sealed partial class LatticeTenantAdminTests
{
    private const string CallerSubject = "platform-operator";

    private static ILatticeMembershipContext Caller(string subjectId)
        => new FixedMembershipContext(new LatticeSubject(subjectId));

    [Test]
    public async Task CreateTenantAsync_seeds_the_calling_subject_when_no_admin_subjects_are_supplied()
    {
        var registry = new FakeTenantRegistry();
        var facade = Create(registry, membership: Caller(CallerSubject));

        var result = await facade.CreateTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(result.AdminSubjects, Is.EqualTo(new[] { CallerSubject }));
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject(CallerSubject), Is.True,
                "The creating subject must be able to see the tenant it just created.");
            Assert.That(registry.Puts, Is.EqualTo(1), "The seeded record must land in a single write.");
        });
    }

    [Test]
    public async Task CreateTenantAsync_with_explicit_subjects_seeds_exactly_those_and_not_the_caller()
    {
        var registry = new FakeTenantRegistry();
        var facade = Create(registry, membership: Caller(CallerSubject));

        var result = await facade.CreateTenantAsync(Tenant, ["ops@example.com", "sre@example.com"]);

        var record = registry.Peek(Tenant)!;
        Assert.Multiple(() =>
        {
            Assert.That(result.AdminSubjects, Is.EquivalentTo(new[] { "ops@example.com", "sre@example.com" }));
            Assert.That(record.HasAdminSubject("ops@example.com"), Is.True);
            Assert.That(record.HasAdminSubject("sre@example.com"), Is.True);
            Assert.That(record.HasAdminSubject(CallerSubject), Is.False,
                "An explicit set overrides the caller-seeding default outright.");
        });
    }

    [Test]
    public async Task CreateTenantAsync_with_an_empty_subject_set_falls_back_to_seeding_the_caller()
    {
        var registry = new FakeTenantRegistry();
        var facade = Create(registry, membership: Caller(CallerSubject));

        var result = await facade.CreateTenantAsync(Tenant, []);

        Assert.That(result.AdminSubjects, Is.EqualTo(new[] { CallerSubject }));
    }

    [Test]
    public async Task CreateTenantAsync_collapses_duplicate_admin_subjects()
    {
        var registry = new FakeTenantRegistry();
        var facade = Create(registry, membership: Caller(CallerSubject));

        var result = await facade.CreateTenantAsync(Tenant, ["ops@example.com", "ops@example.com"]);

        Assert.That(result.AdminSubjects, Is.EqualTo(new[] { "ops@example.com" }));
    }

    [Test]
    public async Task CreateTenantAsync_seeds_nothing_when_the_caller_cannot_be_resolved()
    {
        var registry = new FakeTenantRegistry();

        // No membership context at all, and an anonymous caller: neither can be
        // promoted to an admin subject, so the tenant is deliberately left
        // subject-less rather than having one invented for it.
        var noMembership = await Create(registry).CreateTenantAsync(Tenant);
        var anonymous = await Create(
                new FakeTenantRegistry(),
                membership: new FixedMembershipContext(LatticeSubject.Anonymous))
            .CreateTenantAsync("globex");

        Assert.Multiple(() =>
        {
            Assert.That(noMembership.AdminSubjects, Is.Empty);
            Assert.That(anonymous.AdminSubjects, Is.Empty);
        });
    }

    [Test]
    public void CreateTenantAsync_rejects_a_blank_admin_subject_fail_closed()
    {
        var registry = new FakeTenantRegistry();
        var facade = Create(registry, membership: Caller(CallerSubject));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.CreateTenantAsync(Tenant, ["ops@example.com", ""]),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await facade.CreateTenantAsync(Tenant, ["   "]),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await facade.CreateTenantAsync(Tenant, [null!]),
                Throws.InstanceOf<ArgumentException>());
        });

        Assert.That(registry.Puts, Is.EqualTo(0), "A rejected create must not write.");
    }

    [Test]
    public void CreateTenantAsync_authorizes_before_it_validates_the_subject_set()
    {
        var registry = new FakeTenantRegistry();
        var facade = Create(registry, allow: false, membership: Caller(CallerSubject));

        // Authorize, then validate, then write - the order the security
        // instructions fix for every administrative create path. An unauthorized
        // caller must be denied identically whether or not its arguments are
        // well-formed, so a malformed subject list cannot be used as an oracle to
        // distinguish "denied" from "denied and also malformed".
        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.CreateTenantAsync(Tenant, [""]),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a denied caller is refused before its arguments are inspected");
            Assert.That(async () => await facade.CreateTenantAsync(Tenant, ["ops@example.com"]),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });

        Assert.That(registry.Puts, Is.EqualTo(0));
    }

    [Test]
    public void CreateTenantAsync_rejects_a_tenant_id_in_a_reserved_namespace()
    {
        var registry = new FakeTenantRegistry();
        var facade = Create(registry, membership: Caller(CallerSubject));

        // A tenant id travels into tree ids (t/{tenant}/{name}), metric labels and
        // log lines beside real tree ids, so one shadowing a reserved namespace is
        // an avoidable confusion trap even though it is structurally namespaced.
        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.CreateTenantAsync("sys-auth-policy"),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await facade.CreateTenantAsync("sys-tenant-registry"),
                Throws.InstanceOf<ArgumentException>());
        });

        Assert.That(registry.Puts, Is.EqualTo(0), "a reserved tenant id must not be registered.");
    }

    [Test]
    public async Task CreateTenantAsync_seeded_subject_makes_the_new_tenant_visible_to_its_creator()
    {
        var registry = new FakeTenantRegistry();
        var facade = Create(registry, membership: Caller(CallerSubject));

        await facade.CreateTenantAsync(Tenant);

        // The self-service read path scopes visibility to the subjects a tenant
        // record names, so asserting membership on the persisted record is the
        // structural equivalent of "list/get now returns it" for this caller.
        var record = registry.Peek(Tenant)!;
        Assert.Multiple(() =>
        {
            Assert.That(record.AdminSubjects, Is.EqualTo(new[] { CallerSubject }));
            Assert.That(record.Status, Is.EqualTo(TenantStatus.Active));
            Assert.That(record.Quotas.IsUnbounded, Is.True, "Create must still register the tenant unbounded.");
        });
    }
}
