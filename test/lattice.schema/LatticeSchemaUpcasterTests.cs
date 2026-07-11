using System.Text;
using System.Text.Json;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaUpcaster"/>: the IR-backed and
/// DI-transform-backed factories, version-ascending validation, and
/// <see cref="LatticeSchemaUpcaster.Apply"/> for both backing mechanisms.
/// </summary>
public sealed class LatticeSchemaUpcasterTests
{
    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static LatticeValueTransform SetV2() =>
        LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("v", LatticeValueTransform.Const(LatticeConstant.Integer(2))));

    [Test]
    public void FromTransform_sets_versions_and_null_transform_id()
    {
        var upcaster = LatticeSchemaUpcaster.FromTransform(3, 1, 2, SetV2());

        Assert.That(upcaster.SchemaId, Is.EqualTo(3u));
        Assert.That(upcaster.FromVersion, Is.EqualTo(1u));
        Assert.That(upcaster.ToVersion, Is.EqualTo(2u));
        Assert.That(upcaster.TransformId, Is.Null);
    }

    [Test]
    public void FromTransformId_sets_transform_id()
    {
        var upcaster = LatticeSchemaUpcaster.FromTransformId(3, 1, 2, "my-upcaster");

        Assert.That(upcaster.TransformId, Is.EqualTo("my-upcaster"));
    }

    [Test]
    public void FromTransform_non_ascending_throws()
    {
        Assert.That(() => LatticeSchemaUpcaster.FromTransform(1, 2, 2, SetV2()), Throws.ArgumentException);
        Assert.That(() => LatticeSchemaUpcaster.FromTransform(1, 3, 2, SetV2()), Throws.ArgumentException);
    }

    [Test]
    public void FromTransformId_non_ascending_or_empty_throws()
    {
        Assert.That(() => LatticeSchemaUpcaster.FromTransformId(1, 2, 2, "id"), Throws.ArgumentException);
        Assert.That(() => LatticeSchemaUpcaster.FromTransformId(1, 1, 2, string.Empty), Throws.ArgumentException);
    }

    [Test]
    public void Apply_ir_evaluates_transform()
    {
        var upcaster = LatticeSchemaUpcaster.FromTransform(1, 1, 2, SetV2());

        var result = upcaster.Apply(Utf8("{\"a\":1}"), transformRegistry: null);

        var root = JsonDocument.Parse(result).RootElement;
        Assert.That(root.GetProperty("a").GetInt32(), Is.EqualTo(1));
        Assert.That(root.GetProperty("v").GetInt32(), Is.EqualTo(2));
    }

    [Test]
    public void Apply_di_transform_resolves_and_invokes()
    {
        var transform = Substitute.For<ILatticeValueTransform>();
        transform.Id.Returns("my-upcaster");
        transform.Transform(Arg.Any<byte[]>()).Returns(Utf8("upcasted"));
        var registry = new LatticeValueTransformRegistry(new[] { transform });
        var upcaster = LatticeSchemaUpcaster.FromTransformId(1, 1, 2, "my-upcaster");

        var result = upcaster.Apply(Utf8("input"), registry);

        Assert.That(result, Is.EqualTo(Utf8("upcasted")));
    }

    [Test]
    public void Apply_di_transform_without_registry_throws()
    {
        var upcaster = LatticeSchemaUpcaster.FromTransformId(1, 1, 2, "missing");

        Assert.That(
            () => upcaster.Apply(Utf8("input"), transformRegistry: null),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Apply_di_transform_unresolved_id_throws()
    {
        var registry = new LatticeValueTransformRegistry(Array.Empty<ILatticeValueTransform>());
        var upcaster = LatticeSchemaUpcaster.FromTransformId(1, 1, 2, "missing");

        Assert.That(
            () => upcaster.Apply(Utf8("input"), registry),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Apply_null_body_throws()
    {
        var upcaster = LatticeSchemaUpcaster.FromTransform(1, 1, 2, SetV2());

        Assert.That(() => upcaster.Apply(null!, null), Throws.ArgumentNullException);
    }
}
