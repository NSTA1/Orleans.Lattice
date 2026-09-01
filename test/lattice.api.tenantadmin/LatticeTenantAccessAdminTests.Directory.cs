using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Directory-validation tests for <see cref="LatticeTenantAccessAdmin.AddAdminSubjectAsync"/>.
/// Membership of the admin-subject set <em>is</em> the tenant-admin capability, so
/// adding a subject is an administrative membership-reference create: it must
/// reject an id the configured identity directory cannot resolve before any write,
/// yet accept a resolvable one, and it must never consult the directory when
/// validation is off or the provider is the default no-op.
/// </summary>
public sealed partial class LatticeTenantAccessAdminTests
{
    private static LatticeTenantAccessAdmin AdminWithDirectory(
        FakeTenantRegistry registry,
        ILatticeIdentityDirectory directory,
        bool validationRequired)
        => new(
            registry,
            new TenantRegionResidencyAuthorizer(
                new FixedGate(allow: true), registry, new FixedMembershipContext(new LatticeSubject("op"))),
            new IncrementingClock(),
            Options.Create(new ClusterOptions()),
            directory,
            new FixedOptionsMonitor<LatticeIdentityDirectoryOptions>(
                new LatticeIdentityDirectoryOptions { ValidationRequired = validationRequired }));

    [Test]
    public void AddAdminSubjectAsync_with_an_unresolvable_id_is_denied_before_any_write()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant));
        var admin = AdminWithDirectory(registry, new FakeIdentityDirectory(principal: null), validationRequired: true);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.AddAdminSubjectAsync(Tenant, "ghost"),
                Throws.TypeOf<LatticeDirectoryValidationException>());
            Assert.That(registry.Puts, Is.Zero, "A directory-rejected add must never write.");
        });
    }

    [Test]
    public async Task AddAdminSubjectAsync_with_a_resolvable_id_records_the_grant()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant));
        var principal = new DirectoryPrincipal("real", "Real User", DirectoryPrincipalKind.User);
        var directory = new FakeIdentityDirectory(principal);
        var admin = AdminWithDirectory(registry, directory, validationRequired: true);

        var result = await admin.AddAdminSubjectAsync(Tenant, "real");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Subjects, Does.Contain("real"));
            Assert.That(directory.Resolved, Is.EqualTo(new[] { "real" }));
        });
    }

    [Test]
    public async Task AddAdminSubjectAsync_with_validation_disabled_skips_the_directory()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant));
        var directory = new FakeIdentityDirectory(principal: null);
        var admin = AdminWithDirectory(registry, directory, validationRequired: false);

        var result = await admin.AddAdminSubjectAsync(Tenant, "unchecked");

        Assert.Multiple(() =>
        {
            Assert.That(result.Subjects, Does.Contain("unchecked"));
            Assert.That(directory.Resolved, Is.Empty, "validation-disabled must not consult the directory.");
        });
    }

    [Test]
    public async Task AddAdminSubjectAsync_with_the_null_provider_skips_validation()
    {
        // The default no-op provider is not a real directory, so an add is accepted
        // unvalidated even with validation required - the guard treats the null
        // provider as "no directory" and never resolves.
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant));
        var admin = AdminWithDirectory(registry, new NullIdentityDirectory(), validationRequired: true);

        var result = await admin.AddAdminSubjectAsync(Tenant, "unchecked");

        Assert.That(result.Subjects, Does.Contain("unchecked"));
    }

    [Test]
    public async Task AddAdminSubjectAsync_with_a_directory_but_no_options_monitor_skips_validation()
    {
        // A real directory with no options monitor cannot report that validation is
        // required, so the guard fails safe by skipping validation rather than
        // throwing on the missing options; the directory is never consulted.
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant));
        var directory = new FakeIdentityDirectory(principal: null);
        var admin = new LatticeTenantAccessAdmin(
            registry,
            new TenantRegionResidencyAuthorizer(
                new FixedGate(allow: true), registry, new FixedMembershipContext(new LatticeSubject("op"))),
            new IncrementingClock(),
            Options.Create(new ClusterOptions()),
            directory,
            identityDirectoryOptions: null);

        var result = await admin.AddAdminSubjectAsync(Tenant, "unchecked");

        Assert.Multiple(() =>
        {
            Assert.That(result.Subjects, Does.Contain("unchecked"));
            Assert.That(directory.Resolved, Is.Empty, "a missing options monitor must not consult the directory.");
        });
    }
}
