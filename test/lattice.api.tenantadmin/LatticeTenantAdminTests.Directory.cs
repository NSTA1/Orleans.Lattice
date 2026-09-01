using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Directory-validation, caller-seeding, id-grammar, and status-mapping tests for
/// <see cref="LatticeTenantAdmin"/>. They pin the fail-closed contract that an
/// explicitly supplied admin-subject id is rejected before any write when the
/// configured identity directory cannot resolve it, that the caller-seeding path
/// resolves an uncached subject under a gate-bypassing system-origin scope, that a
/// malformed tenant id is refused, and that an out-of-range persisted status maps
/// to a safe lifecycle default rather than throwing.
/// </summary>
public sealed partial class LatticeTenantAdminTests
{
    private static readonly HybridLogicalClock Seed = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

    private static LatticeTenantAdmin CreateWithDirectory(
        FakeTenantRegistry registry,
        ILatticeIdentityDirectory directory,
        bool validationRequired,
        ILatticeMembershipContext? membership = null)
        => new(
            registry,
            new TenantAdminAccessAuthorizer(new FixedGate(allow: true), membership),
            new IncrementingClock(),
            new StubCascade(0),
            Options.Create(new ClusterOptions()),
            membership,
            directory,
            new FixedOptionsMonitor<LatticeIdentityDirectoryOptions>(
                new LatticeIdentityDirectoryOptions { ValidationRequired = validationRequired }));

    // ----- directory validation of an explicit admin-subject set -----

    [Test]
    public void CreateTenantAsync_with_an_unresolvable_admin_subject_is_denied_before_any_write()
    {
        var registry = new FakeTenantRegistry();
        var admin = CreateWithDirectory(
            registry, new FakeIdentityDirectory(principal: null), validationRequired: true);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.CreateTenantAsync(Tenant, new[] { "ghost" }),
                Throws.TypeOf<LatticeDirectoryValidationException>());
            Assert.That(registry.Puts, Is.Zero, "A directory-rejected create must never write.");
            Assert.That(registry.Contains(Tenant), Is.False);
        });
    }

    [Test]
    public async Task CreateTenantAsync_with_a_resolvable_admin_subject_creates_the_tenant()
    {
        var registry = new FakeTenantRegistry();
        var principal = new DirectoryPrincipal("real", "Real User", DirectoryPrincipalKind.User);
        var directory = new FakeIdentityDirectory(principal);
        var admin = CreateWithDirectory(registry, directory, validationRequired: true);

        var result = await admin.CreateTenantAsync(Tenant, new[] { "real" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AdminSubjects, Does.Contain("real"));
            Assert.That(directory.Resolved, Is.EqualTo(new[] { "real" }));
            Assert.That(registry.Contains(Tenant), Is.True);
        });
    }

    [Test]
    public async Task CreateTenantAsync_with_the_null_directory_accepts_ids_without_validation()
    {
        // The default no-op directory is "available but not real": even with
        // validation required, ids are accepted unvalidated (the guard short-circuits
        // on DirectoryAvailable being false), so no resolve is attempted.
        var registry = new FakeTenantRegistry();
        var admin = CreateWithDirectory(registry, new NullIdentityDirectory(), validationRequired: true);

        var result = await admin.CreateTenantAsync(Tenant, new[] { "ghost" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AdminSubjects, Does.Contain("ghost"));
            Assert.That(registry.Contains(Tenant), Is.True);
        });
    }

    [Test]
    public async Task CreateTenantAsync_with_validation_disabled_skips_the_directory()
    {
        var registry = new FakeTenantRegistry();
        var directory = new FakeIdentityDirectory(principal: null);
        var admin = CreateWithDirectory(registry, directory, validationRequired: false);

        var result = await admin.CreateTenantAsync(Tenant, new[] { "unchecked" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AdminSubjects, Does.Contain("unchecked"));
            Assert.That(directory.Resolved, Is.Empty, "validation-disabled must not consult the directory.");
        });
    }

    [Test]
    public async Task CreateTenantAsync_with_a_directory_but_no_options_monitor_skips_validation()
    {
        // A real directory configured without an options monitor cannot report that
        // validation is required, so the guard fails safe by skipping validation
        // rather than throwing on the missing options - the directory is never
        // consulted and the create proceeds.
        var registry = new FakeTenantRegistry();
        var directory = new FakeIdentityDirectory(principal: null);
        var admin = new LatticeTenantAdmin(
            registry,
            new TenantAdminAccessAuthorizer(new FixedGate(allow: true)),
            new IncrementingClock(),
            new StubCascade(0),
            Options.Create(new ClusterOptions()),
            membership: null,
            directory,
            identityDirectoryOptions: null);

        var result = await admin.CreateTenantAsync(Tenant, new[] { "unchecked" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AdminSubjects, Does.Contain("unchecked"));
            Assert.That(directory.Resolved, Is.Empty, "a missing options monitor must not consult the directory.");
        });
    }

    // ----- caller-seeding via the uncached membership path -----

    [Test]
    public async Task CreateTenantAsync_with_no_subjects_seeds_the_uncached_caller_under_system_origin()
    {
        var registry = new FakeTenantRegistry();
        var membership = new CacheMissMembershipContext(new LatticeSubject("creator"));
        var admin = Create(registry, membership: membership);

        var result = await admin.CreateTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(result.AdminSubjects, Does.Contain("creator"));
            Assert.That(membership.ResolveCurrentCalled, Is.True);
            Assert.That(
                membership.ResolvedUnderSystemOrigin,
                Is.True,
                "the uncached caller resolution must run under a gate-bypassing system-origin scope.");
        });
    }

    // ----- id grammar -----

    [Test]
    public void CreateTenantAsync_with_a_malformed_tenant_id_throws_argument()
    {
        var admin = Create(new FakeTenantRegistry());

        Assert.That(
            async () => await admin.CreateTenantAsync("BAD_ID"),
            Throws.InstanceOf<ArgumentException>());
    }

    // ----- status mapping -----

    [Test]
    public async Task SuspendTenantAsync_maps_an_out_of_range_previous_status_to_the_safe_default()
    {
        var registry = new FakeTenantRegistry();
        var record = TenantRecord.Create(
            Parse(Tenant), (TenantStatus)99, TenantQuotas.Unbounded, TenantPlacement.Shared, Seed, "seed");
        registry.Seed(record);
        var admin = Create(registry);

        var result = await admin.SuspendTenantAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(result.PreviousStatus, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(result.NewStatus, Is.EqualTo(TenantLifecycleStatus.Suspended));
            Assert.That(result.Changed, Is.True);
        });
    }
}
