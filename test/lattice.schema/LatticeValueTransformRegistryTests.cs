using System.Text;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Covers the <see cref="ILatticeValueTransform"/> DI escape hatch: registration
/// through <see cref="LatticeSchemaServiceCollectionExtensions"/>, resolution and
/// dispatch by stable id through <see cref="ILatticeValueTransformRegistry"/>, and
/// the duplicate-id / missing-id / null-argument guards.
/// </summary>
[TestFixture]
public sealed class LatticeValueTransformRegistryTests
{
    private sealed class UpperCaseTransform : ILatticeValueTransform
    {
        public string Id => "upper";

        public byte[] Transform(byte[] value) =>
            Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(value).ToUpperInvariant());
    }

    private sealed class ReverseTransform : ILatticeValueTransform
    {
        public string Id => "reverse";

        public byte[] Transform(byte[] value)
        {
            var copy = (byte[])value.Clone();
            Array.Reverse(copy);
            return copy;
        }
    }

    private sealed class DuplicateUpperTransform : ILatticeValueTransform
    {
        public string Id => "upper";

        public byte[] Transform(byte[] value) => value;
    }

    private static ILatticeValueTransformRegistry BuildRegistry(params ILatticeValueTransform[] transforms)
    {
        var services = new ServiceCollection();
        foreach (var transform in transforms)
            services.AddLatticeValueTransform(transform);
        return services.BuildServiceProvider().GetRequiredService<ILatticeValueTransformRegistry>();
    }

    [Test]
    public void Registry_get_dispatches_to_the_transform_registered_under_the_id()
    {
        var registry = BuildRegistry(new UpperCaseTransform(), new ReverseTransform());

        var resolved = registry.Get("upper");
        var output = resolved.Transform(Encoding.UTF8.GetBytes("abc"));

        Assert.That(resolved, Is.InstanceOf<UpperCaseTransform>());
        Assert.That(Encoding.UTF8.GetString(output), Is.EqualTo("ABC"));
    }

    [Test]
    public void Registry_try_get_returns_true_and_the_transform_for_a_known_id()
    {
        var registry = BuildRegistry(new UpperCaseTransform(), new ReverseTransform());

        var found = registry.TryGet("reverse", out var transform);

        Assert.That(found, Is.True);
        Assert.That(transform, Is.InstanceOf<ReverseTransform>());
    }

    [Test]
    public void Registry_try_get_returns_false_for_an_unknown_id()
    {
        var registry = BuildRegistry(new UpperCaseTransform());

        var found = registry.TryGet("nope", out var transform);

        Assert.That(found, Is.False);
        Assert.That(transform, Is.Null);
    }

    [Test]
    public void Registry_get_throws_key_not_found_for_an_unknown_id()
    {
        var registry = BuildRegistry(new UpperCaseTransform());

        Assert.That(() => registry.Get("nope"), Throws.TypeOf<KeyNotFoundException>());
    }

    [Test]
    public void Registry_construction_throws_on_duplicate_id()
    {
        // A duplicate id surfaces as a clear failure when the registry is first
        // resolved, rather than silently shadowing one transform with another.
        Assert.That(
            () => BuildRegistry(new UpperCaseTransform(), new DuplicateUpperTransform()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void Registry_try_get_null_id_throws_argument_null()
    {
        var registry = BuildRegistry(new UpperCaseTransform());

        Assert.That(() => registry.TryGet(null!, out _), Throws.ArgumentNullException);
    }

    [Test]
    public void Registry_get_null_id_throws_argument_null()
    {
        var registry = BuildRegistry(new UpperCaseTransform());

        Assert.That(() => registry.Get(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeValueTransform_generic_registers_the_type()
    {
        var services = new ServiceCollection();
        services.AddLatticeValueTransform<UpperCaseTransform>();

        var registry = services.BuildServiceProvider().GetRequiredService<ILatticeValueTransformRegistry>();

        Assert.That(registry.Get("upper"), Is.InstanceOf<UpperCaseTransform>());
    }

    [Test]
    public void AddLatticeValueTransform_null_services_throws_argument_null()
    {
        IServiceCollection services = null!;

        Assert.That(() => services.AddLatticeValueTransform(new UpperCaseTransform()), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeValueTransform_null_transform_throws_argument_null()
    {
        var services = new ServiceCollection();

        Assert.That(() => services.AddLatticeValueTransform((ILatticeValueTransform)null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeValueTransform_generic_null_services_throws_argument_null()
    {
        IServiceCollection services = null!;

        Assert.That(() => services.AddLatticeValueTransform<UpperCaseTransform>(), Throws.ArgumentNullException);
    }
}
