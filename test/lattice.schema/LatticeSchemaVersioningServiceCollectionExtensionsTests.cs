using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaVersioningServiceCollectionExtensions"/>
/// that do not require a live silo: the null-argument guard, the ordering guard
/// (must follow <c>AddLattice</c>), idempotent re-registration of the structural
/// wiring, replacement of the core write-interceptor / value-decoder / envelope
/// codec seams, and the layering of the options delegate.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaVersioningServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeSchemaVersioning_null_builder_throws()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddLatticeSchemaVersioning(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeSchemaVersioning_null_builder_throws()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).ConfigureLatticeSchemaVersioning(_ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeSchemaVersioning_null_configure_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(
            () => builder.ConfigureLatticeSchemaVersioning(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeSchemaVersioning_before_AddLattice_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeSchemaVersioning(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeSchemaVersioning_wires_the_core_seams()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        builder.AddLatticeSchemaVersioning();

        Assert.That(Registered<ILatticeSchemaVersionStore>(builder), Is.True);
        Assert.That(Registered<ILatticeSchemaVersionProvider>(builder), Is.True);
        Assert.That(Registered<ILatticeSchemaVersionAdmin>(builder), Is.True);
        Assert.That(Registered<ILatticeWriteInterceptor>(builder), Is.True);
        Assert.That(Registered<ILatticeValueDecoder>(builder), Is.True);
        Assert.That(Registered<ILatticeEnvelopeCodec>(builder), Is.True);
    }

    [Test]
    public void AddLatticeSchemaVersioning_is_idempotent_for_structural_wiring()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        builder.AddLatticeSchemaVersioning();
        builder.AddLatticeSchemaVersioning();

        var markerCount = builder.Services.Count(
            d => d.ServiceType == typeof(SchemaVersioningRegistrationMarker));
        Assert.That(markerCount, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeSchemaVersioning_layers_the_options_delegate()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        builder.AddLatticeSchemaVersioning(configureOptions: o => o.DeadLetterPreviewMaxBytes = 42);

        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeSchemaVersioningOptions>>()
            .Value;
        Assert.That(options.DeadLetterPreviewMaxBytes, Is.EqualTo(42));
    }

    [Test]
    public void AddLatticeSchemaVersioning_invokes_the_registry_delegate_when_built()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();
        var invoked = false;

        builder.AddLatticeSchemaVersioning(configureRegistry: _ => invoked = true);
        _ = builder.Services.BuildServiceProvider().GetRequiredService<ILatticeSchemaRegistry>();

        Assert.That(invoked, Is.True);
    }

    [Test]
    public void ConfigureLatticeSchemaVersioning_layers_options_after_add()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        builder.AddLatticeSchemaVersioning();
        builder.ConfigureLatticeSchemaVersioning(o => o.DeadLetterPreviewMaxBytes = 11);

        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeSchemaVersioningOptions>>()
            .Value;
        Assert.That(options.DeadLetterPreviewMaxBytes, Is.EqualTo(11));
    }

    private static bool Registered<TService>(ISiloBuilder builder) =>
        builder.Services.Any(d => d.ServiceType == typeof(TService));
}
