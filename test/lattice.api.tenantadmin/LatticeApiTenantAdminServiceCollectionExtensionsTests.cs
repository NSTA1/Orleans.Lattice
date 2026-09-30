using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiTenantAdminServiceCollectionExtensions"/>
/// that do not require a live silo: the ordering guard (the tenant-administration
/// control API must follow the tenancy add-on whose registry it operates on), the
/// null-argument guard, idempotent re-registration of the control singleton
/// and its seams, and that the registered create path is wired to the identity
/// directory so a seeded admin subject is validated (issue #4003).
/// </summary>
[TestFixture]
public sealed class LatticeApiTenantAdminServiceCollectionExtensionsTests
{
    private const string Tenant = "acme";

    [Test]
    public void AddLatticeTenantAdminApi_before_tenancy_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeTenantAdminApi(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeTenantAdminApi_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddLatticeTenantAdminApi(), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeTenantAdminApi_after_tenancy_wires_the_control_and_its_seams_once()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ITenantRegistry>(new FakeTenantRegistry());
        builder.Services.AddSingleton(new TenantAdminAccessAuthorizer(new FixedGate(true)));

        builder.AddLatticeTenantAdminApi();
        builder.AddLatticeTenantAdminApi();

        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantAdmin)), Is.EqualTo(1));
            Assert.That(builder.Services.Any(d => d.ServiceType == typeof(ITenantAdminClock)), Is.True);
            Assert.That(builder.Services.Any(d => d.ServiceType == typeof(ITenantTreeCascade)), Is.True);
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantSelfService)),
                Is.EqualTo(1),
                "The read-only tenant self-awareness facade is the single tenancy-enabled signal the MCP binding keys off.");
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantAccessAdmin)),
                Is.EqualTo(1),
                "The tenant access-administration facade is wired exactly once alongside the lifecycle facade.");
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantGrantAdmin)),
                Is.EqualTo(1),
                "The cross-tenant grant facade is wired exactly once alongside the lifecycle facade.");
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(TenantRegionResidencyAuthorizer)),
                Is.EqualTo(1),
                "Both tenant-tier facades share the one two-tier authorizer.");
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(ITenantRegionStatusChangeListener)
                    && d.ImplementationType == typeof(TenantRegionDrainCompletionListener)),
                Is.EqualTo(1),
                "The listener that completes the local region's drain is wired exactly once (issue #3897).");
        });
    }

    [Test]
    public void AddLatticeTenantAdminApi_returns_the_same_builder_for_chaining()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ITenantRegistry>(new FakeTenantRegistry());

        Assert.That(builder.AddLatticeTenantAdminApi(), Is.SameAs(builder));
    }

    [Test]
    public void AddLatticeTenantAdminApi_with_a_configure_delegate_runs_it_when_options_resolve()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ITenantRegistry>(new FakeTenantRegistry());
        var configured = false;

        builder.AddLatticeTenantAdminApi(_ => configured = true);

        using var provider = builder.Services.BuildServiceProvider();
        _ = provider.GetRequiredService<IOptions<LatticeApiTenantAdminOptions>>().Value;

        Assert.That(configured, Is.True, "a supplied configure delegate must be bound and run when the options resolve.");
    }

    // ----- identity-directory wiring of the registered create path (issue #4003) -----

    [Test]
    public void Registered_CreateTenantAsync_with_validation_required_rejects_a_subject_the_directory_does_not_know()
    {
        var registry = new FakeTenantRegistry();
        var directory = new FakeIdentityDirectory(principal: null);
        using var provider = BuildRegisteredProvider(registry, directory, validationRequired: true);
        var admin = provider.GetRequiredService<ILatticeTenantAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.CreateTenantAsync(Tenant, new[] { "ghost" }),
                Throws.TypeOf<LatticeDirectoryValidationException>(),
                "The registered facade must validate a seeded admin subject exactly as AddAdminSubjectAsync does.");
            Assert.That(directory.Resolved, Is.EqualTo(new[] { "ghost" }), "The registered directory must be consulted.");
            Assert.That(registry.Puts, Is.Zero, "A directory-rejected create must never write.");
            Assert.That(registry.Contains(Tenant), Is.False);
        });
    }

    [Test]
    public async Task Registered_CreateTenantAsync_with_validation_required_accepts_a_subject_the_directory_resolves()
    {
        var registry = new FakeTenantRegistry();
        var directory = new FakeIdentityDirectory(
            new DirectoryPrincipal("real", "Real User", DirectoryPrincipalKind.User));
        using var provider = BuildRegisteredProvider(registry, directory, validationRequired: true);
        var admin = provider.GetRequiredService<ILatticeTenantAdmin>();

        var result = await admin.CreateTenantAsync(Tenant, new[] { "real" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AdminSubjects, Does.Contain("real"));
            Assert.That(directory.Resolved, Is.EqualTo(new[] { "real" }));
            Assert.That(registry.Contains(Tenant), Is.True);
        });
    }

    [Test]
    public async Task Registered_CreateTenantAsync_with_validation_not_required_does_not_consult_the_directory()
    {
        var registry = new FakeTenantRegistry();
        var directory = new FakeIdentityDirectory(principal: null);
        using var provider = BuildRegisteredProvider(registry, directory, validationRequired: false);
        var admin = provider.GetRequiredService<ILatticeTenantAdmin>();

        var result = await admin.CreateTenantAsync(Tenant, new[] { "unchecked" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AdminSubjects, Does.Contain("unchecked"));
            Assert.That(directory.Resolved, Is.Empty, "Validation not required must not consult the directory.");
            Assert.That(registry.Contains(Tenant), Is.True);
        });
    }

    [Test]
    public async Task Registered_CreateTenantAsync_with_the_null_directory_accepts_ids_even_when_validation_is_required()
    {
        var registry = new FakeTenantRegistry();
        using var provider = BuildRegisteredProvider(registry, new NullIdentityDirectory(), validationRequired: true);
        var admin = provider.GetRequiredService<ILatticeTenantAdmin>();

        var result = await admin.CreateTenantAsync(Tenant, new[] { "ghost" });

        Assert.Multiple(() =>
        {
            Assert.That(result.AdminSubjects, Does.Contain("ghost"));
            Assert.That(registry.Contains(Tenant), Is.True);
        });
    }

    [Test]
    public async Task Registered_CreateTenantAsync_with_no_directory_registered_accepts_ids()
    {
        var registry = new FakeTenantRegistry();
        using var provider = BuildRegisteredProvider(registry, directory: null, validationRequired: true);
        var admin = provider.GetRequiredService<ILatticeTenantAdmin>();

        var result = await admin.CreateTenantAsync(Tenant, new[] { "ghost" });

        Assert.That(result.AdminSubjects, Does.Contain("ghost"));
    }

    /// <summary>
    /// Builds a provider through the real <see cref="LatticeApiTenantAdminServiceCollectionExtensions.AddLatticeTenantAdminApi"/>
    /// registration, supplying only the upstream seams it resolves (the tenancy
    /// registry, an allow-all access gate, a stub tree cascade standing in for the
    /// grain-backed one, and optionally an identity directory with its options).
    /// </summary>
    private static ServiceProvider BuildRegisteredProvider(
        FakeTenantRegistry registry,
        ILatticeIdentityDirectory? directory,
        bool validationRequired)
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ITenantRegistry>(registry);
        builder.Services.AddSingleton<ILatticeAccessGate>(new FixedGate(allow: true));
        builder.Services.AddSingleton<ITenantTreeCascade>(new StubCascade(0));
        if (directory is not null)
        {
            builder.Services.AddSingleton(directory);
        }

        builder.Services.Configure<LatticeIdentityDirectoryOptions>(o => o.ValidationRequired = validationRequired);

        builder.AddLatticeTenantAdminApi();

        return builder.Services.BuildServiceProvider();
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
