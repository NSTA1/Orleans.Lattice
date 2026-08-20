using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaServiceCollectionExtensions"/>: the four
/// <c>AddLatticeValueTransform</c> overloads (instance and generic, on both
/// <see cref="IServiceCollection"/> and <see cref="ISiloBuilder"/>) each register
/// the transform and ensure the resolving registry is present, and every overload
/// guards its null arguments.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaServiceCollectionExtensionsTests
{
    private sealed class StubTransform : ILatticeValueTransform
    {
        public string Id => "stub";

        public byte[] Transform(byte[] value) => value;
    }

    private static ILatticeValueTransform Instance() => new StubTransform();

    [Test]
    public void AddLatticeValueTransform_instance_on_services_registers_transform_and_registry()
    {
        var services = new ServiceCollection();

        services.AddLatticeValueTransform(Instance());

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<ILatticeValueTransform>(), Is.Not.Null);
        Assert.That(provider.GetService<ILatticeValueTransformRegistry>(), Is.Not.Null);
    }

    [Test]
    public void AddLatticeValueTransform_generic_on_services_registers_transform_and_registry()
    {
        var services = new ServiceCollection();

        services.AddLatticeValueTransform<StubTransform>();

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<ILatticeValueTransform>(), Is.Not.Null);
        Assert.That(provider.GetService<ILatticeValueTransformRegistry>(), Is.Not.Null);
    }

    [Test]
    public void AddLatticeValueTransform_instance_on_builder_registers_transform()
    {
        var builder = new FakeSiloBuilder();

        builder.AddLatticeValueTransform(Instance());

        var provider = builder.Services.BuildServiceProvider();
        Assert.That(provider.GetService<ILatticeValueTransform>(), Is.Not.Null);
    }

    [Test]
    public void AddLatticeValueTransform_generic_on_builder_registers_transform()
    {
        var builder = new FakeSiloBuilder();

        builder.AddLatticeValueTransform<StubTransform>();

        var provider = builder.Services.BuildServiceProvider();
        Assert.That(provider.GetService<ILatticeValueTransform>(), Is.Not.Null);
    }

    [Test]
    public void AddLatticeValueTransform_does_not_replace_an_existing_registry()
    {
        var services = new ServiceCollection();
        var registry = Substitute.For<ILatticeValueTransformRegistry>();
        services.AddSingleton(registry);

        services.AddLatticeValueTransform(Instance());

        var resolved = services.BuildServiceProvider().GetRequiredService<ILatticeValueTransformRegistry>();
        Assert.That(resolved, Is.SameAs(registry));
    }

    [Test]
    public void AddLatticeValueTransform_instance_on_services_null_services_throws()
    {
        Assert.That(
            () => ((IServiceCollection)null!).AddLatticeValueTransform(Instance()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeValueTransform_instance_on_services_null_transform_throws()
    {
        Assert.That(
            () => new ServiceCollection().AddLatticeValueTransform((ILatticeValueTransform)null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeValueTransform_generic_on_services_null_services_throws()
    {
        Assert.That(
            () => ((IServiceCollection)null!).AddLatticeValueTransform<StubTransform>(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeValueTransform_instance_on_builder_null_builder_throws()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddLatticeValueTransform(Instance()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeValueTransform_instance_on_builder_null_transform_throws()
    {
        Assert.That(
            () => new FakeSiloBuilder().AddLatticeValueTransform((ILatticeValueTransform)null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeValueTransform_generic_on_builder_null_builder_throws()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddLatticeValueTransform<StubTransform>(),
            Throws.ArgumentNullException);
    }
}
