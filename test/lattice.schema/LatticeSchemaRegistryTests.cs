using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaRegistryBuilder"/> and the
/// <see cref="ILatticeSchemaRegistry"/> it builds: descriptor resolution,
/// single-hop and multi-hop upcasting, the identity read, and the
/// unknown-hop / downcast / newer-than-target throws.
/// </summary>
public sealed class LatticeSchemaRegistryTests
{
    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static LatticeValueTransform SetMember(string name, long value) =>
        LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember(name, LatticeValueTransform.Const(LatticeConstant.Integer(value))));

    [Test]
    public void TryGetDescriptor_returns_registered_descriptor()
    {
        var registry = new LatticeSchemaRegistryBuilder()
            .AddSchema(1, 1, "orders-v1")
            .Build();

        var found = registry.TryGetDescriptor(1, 1, out var descriptor);

        Assert.That(found, Is.True);
        Assert.That(descriptor.Name, Is.EqualTo("orders-v1"));
    }

    [Test]
    public void TryGetDescriptor_returns_false_for_unregistered()
    {
        var registry = new LatticeSchemaRegistryBuilder().Build();

        Assert.That(registry.TryGetDescriptor(1, 9, out _), Is.False);
    }

    [Test]
    public void AddSchema_duplicate_throws()
    {
        var builder = new LatticeSchemaRegistryBuilder().AddSchema(1, 1, "a");

        Assert.That(() => builder.AddSchema(1, 1, "b"), Throws.ArgumentException);
    }

    [Test]
    public void AddUpcaster_duplicate_from_version_throws()
    {
        var builder = new LatticeSchemaRegistryBuilder().AddUpcaster(1, 1, 2, SetMember("v", 2));

        Assert.That(() => builder.AddUpcaster(1, 1, 3, SetMember("v", 3)), Throws.ArgumentException);
    }

    [Test]
    public void AddUpcaster_null_upcaster_throws()
    {
        var builder = new LatticeSchemaRegistryBuilder();

        Assert.That(() => builder.AddUpcaster(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void CanUpcast_identity_is_true()
    {
        var registry = new LatticeSchemaRegistryBuilder().Build();

        Assert.That(registry.CanUpcast(1, 2, 2), Is.True);
    }

    [Test]
    public void CanUpcast_downcast_is_false()
    {
        var registry = new LatticeSchemaRegistryBuilder().Build();

        Assert.That(registry.CanUpcast(1, 3, 2), Is.False);
    }

    [Test]
    public void CanUpcast_walks_multi_hop_chain()
    {
        var registry = new LatticeSchemaRegistryBuilder()
            .AddUpcaster(1, 1, 2, SetMember("b", 2))
            .AddUpcaster(1, 2, 3, SetMember("c", 3))
            .Build();

        Assert.That(registry.CanUpcast(1, 1, 3), Is.True);
    }

    [Test]
    public void CanUpcast_missing_hop_is_false()
    {
        var registry = new LatticeSchemaRegistryBuilder()
            .AddUpcaster(1, 1, 2, SetMember("b", 2))
            .Build();

        Assert.That(registry.CanUpcast(1, 1, 3), Is.False);
    }

    [Test]
    public void Upcast_identity_returns_same_reference()
    {
        var registry = new LatticeSchemaRegistryBuilder().Build();
        var body = Utf8("{\"a\":1}");

        Assert.That(registry.Upcast(1, 2, 2, body), Is.SameAs(body));
    }

    [Test]
    public void Upcast_single_hop_applies_transform()
    {
        var registry = new LatticeSchemaRegistryBuilder()
            .AddUpcaster(1, 1, 2, SetMember("b", 2))
            .Build();

        var result = registry.Upcast(1, 1, 2, Utf8("{\"a\":1}"));

        var root = JsonDocument.Parse(result).RootElement;
        Assert.That(root.GetProperty("a").GetInt32(), Is.EqualTo(1));
        Assert.That(root.GetProperty("b").GetInt32(), Is.EqualTo(2));
    }

    [Test]
    public void Upcast_multi_hop_applies_each_transform_in_order()
    {
        var registry = new LatticeSchemaRegistryBuilder()
            .AddUpcaster(1, 1, 2, SetMember("b", 2))
            .AddUpcaster(1, 2, 3, SetMember("c", 3))
            .Build();

        var result = registry.Upcast(1, 1, 3, Utf8("{\"a\":1}"));

        var root = JsonDocument.Parse(result).RootElement;
        Assert.That(root.GetProperty("a").GetInt32(), Is.EqualTo(1));
        Assert.That(root.GetProperty("b").GetInt32(), Is.EqualTo(2));
        Assert.That(root.GetProperty("c").GetInt32(), Is.EqualTo(3));
    }

    [Test]
    public void Upcast_downcast_throws_not_supported()
    {
        var registry = new LatticeSchemaRegistryBuilder().Build();

        Assert.That(
            () => registry.Upcast(1, 3, 2, Utf8("{\"a\":1}")),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void Upcast_missing_hop_throws_not_supported()
    {
        var registry = new LatticeSchemaRegistryBuilder()
            .AddUpcaster(1, 1, 2, SetMember("b", 2))
            .Build();

        Assert.That(
            () => registry.Upcast(1, 1, 3, Utf8("{\"a\":1}")),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void Upcast_null_body_throws()
    {
        var registry = new LatticeSchemaRegistryBuilder().Build();

        Assert.That(() => registry.Upcast(1, 1, 2, null!), Throws.ArgumentNullException);
    }
}
