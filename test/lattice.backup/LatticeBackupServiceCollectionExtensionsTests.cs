using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeBackupServiceCollectionExtensions"/> that do
/// not require a live silo: the ordering guard (backup must follow the core
/// registration), the null-argument guards, and idempotent re-registration.
/// Happy-path wiring is covered by the sink and catalog integration tests.
/// </summary>
[TestFixture]
public sealed class LatticeBackupServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeBackup_before_AddLattice_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeBackup(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeBackup_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddLatticeBackup(), Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeBackup_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).ConfigureLatticeBackup(_ => { }), Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeBackup_with_null_configure_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.ConfigureLatticeBackup(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeBackup_after_core_wires_the_backup_services_once()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<IValidateOptions<LatticeOptions>>(
            new PassthroughLatticeOptionsValidator());

        builder.AddLatticeBackup();
        builder.AddLatticeBackup();

        var sinkRegistrations = builder.Services.Count(d => d.ServiceType == typeof(ILatticeBackupSink));
        var catalogRegistrations = builder.Services.Count(d => d.ServiceType == typeof(ILatticeBackupCatalogStore));
        Assert.That(sinkRegistrations, Is.EqualTo(1));
        Assert.That(catalogRegistrations, Is.EqualTo(1));
    }

    [Test]
    public void ConfigureLatticeBackup_layers_the_options_delegate()
    {
        var builder = new FakeSiloBuilder();
        builder.ConfigureLatticeBackup(o => o.EnableDurableHistoryView = false);

        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeBackupOptions>>()
            .Value;

        Assert.That(options.EnableDurableHistoryView, Is.False);
    }

    [Test]
    public void AddLatticeBackup_registers_the_inert_tenant_scope_by_default()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<IValidateOptions<LatticeOptions>>(
            new PassthroughLatticeOptionsValidator());

        builder.AddLatticeBackup();

        var scope = builder.Services.BuildServiceProvider().GetRequiredService<ILatticeBackupTenantScope>();
        Assert.Multiple(() =>
        {
            Assert.That(scope, Is.InstanceOf<NullLatticeBackupTenantScope>(),
                "with no tenancy add-on the inert null scope is registered");
            Assert.That(scope.IsActive, Is.False);
        });
    }

    [Test]
    public void ConfigureLatticeBackupSchedule_with_scopeKey_and_null_builder_throws()
    {
        // Line 244: null-builder guard on the overload that takes a scopeKey.
        Assert.That(
            () => ((ISiloBuilder)null!).ConfigureLatticeBackupSchedule("my-scope", _ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeBackupSchedule_with_null_or_empty_scopeKey_throws()
    {
        // Line 245: null/empty scopeKey guard.
        var builder = new FakeSiloBuilder();
        Assert.Multiple(() =>
        {
            Assert.That(
                () => builder.ConfigureLatticeBackupSchedule(null!, _ => { }),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => builder.ConfigureLatticeBackupSchedule("", _ => { }),
                Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void ConfigureLatticeBackupSchedule_with_scopeKey_and_null_configure_throws()
    {
        // Line 246: null-configure guard on the scoped overload.
        var builder = new FakeSiloBuilder();
        Assert.That(
            () => builder.ConfigureLatticeBackupSchedule("my-scope", null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeBackupSchedule_with_scopeKey_layers_options_under_that_name()
    {
        // Line 247: the Configure call with a named options section.
        var builder = new FakeSiloBuilder();

        builder.ConfigureLatticeBackupSchedule("my-scope", o => o.RetentionEnabled = true);

        var opts = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptionsMonitor<LatticeBackupScheduleOptions>>()
            .Get("my-scope");

        Assert.That(opts.RetentionEnabled, Is.True);
    }

    [Test]
    public void ConfigureLatticeBackupHealth_with_null_builder_throws()
    {
        // Line 267: null-builder guard on ConfigureLatticeBackupHealth.
        Assert.That(
            () => ((ISiloBuilder)null!).ConfigureLatticeBackupHealth(_ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeBackupHealth_with_null_configure_throws()
    {
        // Line 268: null-configure guard.
        var builder = new FakeSiloBuilder();
        Assert.That(
            () => builder.ConfigureLatticeBackupHealth(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeBackupHealth_layers_the_options_delegate()
    {
        // Lines 269-270: the successful path - options delegate is applied.
        var builder = new FakeSiloBuilder();
        builder.ConfigureLatticeBackupHealth(o => o.Enabled = false);

        var opts = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeBackupHealthOptions>>()
            .Value;

        Assert.That(opts.Enabled, Is.False);
    }

    private sealed class PassthroughLatticeOptionsValidator : IValidateOptions<LatticeOptions>
    {
        public ValidateOptionsResult Validate(string? name, LatticeOptions options) =>
            ValidateOptionsResult.Success;
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
