using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaEnforcementServiceCollectionExtensions"/>
/// that do not require a live silo: the null-argument and ordering guards,
/// idempotent structural wiring, options-delegate layering, and the opt-in
/// merge-observer branch driven by <see cref="LatticeSchemaEnforcementOptions.ValidateCrdtMergeResults"/>.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaEnforcementServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeSchemaEnforcement_null_builder_throws()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddLatticeSchemaEnforcement(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeSchemaEnforcement_null_builder_throws()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).ConfigureLatticeSchemaEnforcement(_ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeSchemaEnforcement_null_configure_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(
            () => builder.ConfigureLatticeSchemaEnforcement(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeSchemaEnforcement_before_AddLattice_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeSchemaEnforcement(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeSchemaEnforcement_wires_the_control_plane()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        builder.AddLatticeSchemaEnforcement();

        Assert.That(Registered<ILatticeSchemaPolicyStore>(builder), Is.True);
        Assert.That(Registered<ILatticeSchemaDeadLetterStore>(builder), Is.True);
        Assert.That(Registered<ILatticeSchemaPolicyProvider>(builder), Is.True);
        Assert.That(Registered<ILatticeWriteInterceptor>(builder), Is.True);
        Assert.That(Registered<ILatticeSchemaAdmin>(builder), Is.True);
        Assert.That(Registered<ILatticeSchemaRemediationAdmin>(builder), Is.True);
        Assert.That(Registered<ILatticeSchemaComplianceAdmin>(builder), Is.True);
    }

    [Test]
    public void AddLatticeSchemaEnforcement_is_idempotent_for_structural_wiring()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        builder.AddLatticeSchemaEnforcement();
        builder.AddLatticeSchemaEnforcement();

        var markerCount = builder.Services.Count(
            d => d.ServiceType == typeof(SchemaEnforcementRegistrationMarker));
        Assert.That(markerCount, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeSchemaEnforcement_does_not_register_merge_observer_by_default()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        builder.AddLatticeSchemaEnforcement();

        Assert.That(Registered<ILatticeMergeObserver>(builder), Is.False);
    }

    [Test]
    public void AddLatticeSchemaEnforcement_registers_merge_observer_when_opted_in()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        builder.AddLatticeSchemaEnforcement(o => o.ValidateCrdtMergeResults = true);

        Assert.That(Registered<ILatticeMergeObserver>(builder), Is.True);
    }

    [Test]
    public void AddLatticeSchemaEnforcement_layers_the_options_delegate()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        builder.AddLatticeSchemaEnforcement(o => o.DeadLetterPreviewMaxBytes = 99);

        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeSchemaEnforcementOptions>>()
            .Value;
        Assert.That(options.DeadLetterPreviewMaxBytes, Is.EqualTo(99));
    }

    [Test]
    public void ConfigureLatticeSchemaEnforcement_layers_options_after_add()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        builder.AddLatticeSchemaEnforcement();
        builder.ConfigureLatticeSchemaEnforcement(o => o.DeadLetterPreviewMaxBytes = 33);

        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeSchemaEnforcementOptions>>()
            .Value;
        Assert.That(options.DeadLetterPreviewMaxBytes, Is.EqualTo(33));
    }

    private static bool Registered<TService>(ISiloBuilder builder) =>
        builder.Services.Any(d => d.ServiceType == typeof(TService));
}
