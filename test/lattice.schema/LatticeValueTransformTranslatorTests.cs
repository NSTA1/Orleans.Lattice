using System.Linq.Expressions;
using System.Reflection;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Covers <see cref="LatticeValueTransformTranslator"/>: each allowlisted
/// construct lowers to the expected IR, and each unsupported construct throws a
/// <see cref="NotSupportedException"/> naming the offending construct - the
/// transform-side sibling of the predicate translator's allowlist contract.
/// </summary>
[TestFixture]
public sealed class LatticeValueTransformTranslatorTests
{
    private sealed record SubModel
    {
        public string Name { get; init; } = string.Empty;
    }

    private sealed record OldModel
    {
        public string First { get; init; } = string.Empty;
        public string Last { get; init; } = string.Empty;
        public int Age { get; init; }
        public string? Nickname { get; init; }
        public SubModel Sub { get; init; } = new();
    }

    private sealed record NewModel
    {
        public string Full { get; init; } = string.Empty;
        public int Age { get; init; }
        public string Display { get; init; } = string.Empty;
        public string Tier { get; init; } = string.Empty;
    }

    private enum Priority
    {
        Low = 1,
    }

    private sealed record EmptyModel;

    private sealed record CtorOnly(string Value);

    private sealed record ListModel
    {
        public List<int> Items { get; } = new();
    }

    private sealed record ObjectModel
    {
        public object? Value { get; init; }
    }

    private sealed record ConstantModel
    {
        public string? NullableText { get; init; }
        public bool Flag { get; init; }
        public char Char { get; init; }
        public byte Byte { get; init; }
        public short Short { get; init; }
        public ushort UShort { get; init; }
        public long Signed { get; init; }
        public long Unsigned { get; init; }
        public ulong WideUnsigned { get; init; }
        public float Float { get; init; }
        public decimal Decimal { get; init; }
        public Priority Priority { get; init; }
        public object? TextObject { get; init; }
    }

    private sealed class NullStringObject
    {
        public override string? ToString() => null;
    }

    [Test]
    public void Translate_lowers_member_access_to_set_member_of_member_expression()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel, NewModel>(o => new NewModel { Age = o.Age });

        Assert.That(ir.Kind, Is.EqualTo(LatticeValueTransformKind.Passthrough));
        var op = ir.Children![0];
        Assert.That(op.Kind, Is.EqualTo(LatticeValueTransformKind.SetMember));
        Assert.That(op.MemberPath, Is.EqualTo("Age"));
        Assert.That(op.Children![0].Kind, Is.EqualTo(LatticeValueTransformKind.Member));
        Assert.That(op.Children![0].MemberPath, Is.EqualTo("Age"));
    }

    [Test]
    public void Translate_lowers_literal_to_constant_expression()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel, NewModel>(o => new NewModel { Full = "fixed" });

        var value = ir.Children![0].Children![0];
        Assert.That(value.Kind, Is.EqualTo(LatticeValueTransformKind.Constant));
        Assert.That(value.Constant.StringValue, Is.EqualTo("fixed"));
    }

    [Test]
    public void Translate_captures_local_as_constant()
    {
        var suffix = "captured";
        var ir = LatticeValueTransformTranslator.Translate<OldModel, NewModel>(o => new NewModel { Full = suffix });

        var value = ir.Children![0].Children![0];
        Assert.That(value.Kind, Is.EqualTo(LatticeValueTransformKind.Constant));
        Assert.That(value.Constant.StringValue, Is.EqualTo("captured"));
    }

    [Test]
    public void Translate_lowers_null_coalescing_to_coalesce_compute()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel, NewModel>(
            o => new NewModel { Display = o.Nickname ?? "anon" });

        var value = ir.Children![0].Children![0];
        Assert.That(value.Kind, Is.EqualTo(LatticeValueTransformKind.Compute));
        Assert.That(value.ComputeOperator, Is.EqualTo(LatticeComputeOperator.Coalesce));
        Assert.That(value.Children![0].Kind, Is.EqualTo(LatticeValueTransformKind.Member));
        Assert.That(value.Children![1].Kind, Is.EqualTo(LatticeValueTransformKind.Constant));
    }

    [Test]
    public void Translate_lowers_string_addition_to_flattened_concat_compute()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel, NewModel>(
            o => new NewModel { Full = o.First + " " + o.Last });

        var value = ir.Children![0].Children![0];
        Assert.That(value.Kind, Is.EqualTo(LatticeValueTransformKind.Compute));
        Assert.That(value.ComputeOperator, Is.EqualTo(LatticeComputeOperator.Concat));
        Assert.That(value.Children, Has.Length.EqualTo(3));
        Assert.That(value.Children![0].MemberPath, Is.EqualTo("First"));
        Assert.That(value.Children![2].MemberPath, Is.EqualTo("Last"));
    }

    [Test]
    public void Translate_lowers_string_concat_call_to_concat_compute()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel, NewModel>(
            o => new NewModel { Full = string.Concat(o.First, o.Last) });

        var value = ir.Children![0].Children![0];
        Assert.That(value.Kind, Is.EqualTo(LatticeValueTransformKind.Compute));
        Assert.That(value.ComputeOperator, Is.EqualTo(LatticeComputeOperator.Concat));
        Assert.That(value.Children, Has.Length.EqualTo(2));
    }

    [Test]
    public void Translate_lowers_ternary_to_conditional_with_predicate()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel, NewModel>(
            o => new NewModel { Tier = o.Age >= 18 ? "adult" : "minor" });

        var value = ir.Children![0].Children![0];
        Assert.That(value.Kind, Is.EqualTo(LatticeValueTransformKind.Conditional));
        Assert.That(value.Condition.Kind, Is.EqualTo(LatticePredicateNodeKind.Compare));
        Assert.That(value.Children![0].Constant.StringValue, Is.EqualTo("adult"));
        Assert.That(value.Children![1].Constant.StringValue, Is.EqualTo("minor"));
    }

    [Test]
    public void Translate_same_type_overload_lowers_member_init()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel>(o => new OldModel { Age = o.Age });

        Assert.That(ir.Kind, Is.EqualTo(LatticeValueTransformKind.Passthrough));
        Assert.That(ir.Children![0].MemberPath, Is.EqualTo("Age"));
    }

    [Test]
    public void Translate_null_transform_throws_argument_null()
    {
        Assert.That(
            () => LatticeValueTransformTranslator.Translate<OldModel, NewModel>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Translate_rejects_non_member_init_body_naming_it()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => LatticeValueTransformTranslator.Translate<OldModel, string>(o => o.First));
        Assert.That(ex!.Message, Does.Contain("projection body"));
    }

    [Test]
    public void Translate_rejects_instance_method_call_naming_it()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => LatticeValueTransformTranslator.Translate<OldModel, NewModel>(
                o => new NewModel { Full = o.First.ToUpper() }));
        Assert.That(ex!.Message, Does.Contain("ToUpper"));
    }

    [Test]
    public void Translate_rejects_nested_member_access_naming_it()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => LatticeValueTransformTranslator.Translate<OldModel, NewModel>(
                o => new NewModel { Full = o.Sub.Name }));
        Assert.That(ex!.Message, Does.Contain("member access"));
    }

    [Test]
    public void Translate_lowers_constructor_member_mapping()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel, object>(
            o => new { Full = o.First, o.Age });

        Assert.That(ir.Children, Has.Length.EqualTo(2));
        Assert.That(ir.Children![0].MemberPath, Is.EqualTo("Full"));
        Assert.That(ir.Children![0].Children![0].MemberPath, Is.EqualTo("First"));
        Assert.That(ir.Children![1].MemberPath, Is.EqualTo("Age"));
    }

    [Test]
    public void Translate_parameterless_constructor_emits_identity_passthrough()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel, EmptyModel>(_ => new EmptyModel());

        Assert.That(ir.Kind, Is.EqualTo(LatticeValueTransformKind.Passthrough));
        Assert.That(ir.Children, Is.Empty);
    }

    [Test]
    public void Translate_rejects_constructor_arguments_without_member_mapping()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => LatticeValueTransformTranslator.Translate<OldModel, CtorOnly>(o => new CtorOnly(o.First)));

        Assert.That(ex!.Message, Does.Contain("constructor"));
    }

    [Test]
    public void Translate_rejects_non_assignment_member_binding()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => LatticeValueTransformTranslator.Translate<OldModel, ListModel>(_ => new ListModel { Items = { 1 } }));

        Assert.That(ex!.Message, Does.Contain("member binding"));
    }

    [Test]
    public void Translate_unwraps_conversion_before_member_access()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel, ObjectModel>(
            o => new ObjectModel { Value = o.Age });

        var value = ir.Children![0].Children![0];
        Assert.That(value.Kind, Is.EqualTo(LatticeValueTransformKind.Member));
        Assert.That(value.MemberPath, Is.EqualTo("Age"));
    }

    [Test]
    public void Translate_captures_each_supported_constant_shape()
    {
        var uri = new Uri("https://example.test/path");
        var flag = true;
        var ir = LatticeValueTransformTranslator.Translate<OldModel, ConstantModel>(
            _ => new ConstantModel
            {
                NullableText = null,
                Flag = flag,
                Char = 'x',
                Byte = (byte)2,
                Short = (short)-3,
                UShort = (ushort)4,
                Signed = (sbyte)-2,
                Unsigned = 3u,
                WideUnsigned = 4ul,
                Float = 1.25f,
                Decimal = 2.5m,
                Priority = Priority.Low,
                TextObject = uri,
            });

        var operations = ir.Children!;
        Assert.That(operations.Single(o => o.MemberPath == "NullableText").Children![0].Constant.Kind, Is.EqualTo(LatticeConstantKind.Null));
        Assert.That(operations.Single(o => o.MemberPath == "Flag").Children![0].Constant.Kind, Is.EqualTo(LatticeConstantKind.Boolean));
        Assert.That(operations.Single(o => o.MemberPath == "Flag").Children![0].Constant.BooleanValue, Is.True);
        Assert.That(operations.Single(o => o.MemberPath == "Char").Children![0].Constant.StringValue, Is.EqualTo("x"));
        Assert.That(operations.Single(o => o.MemberPath == "Byte").Children![0].Constant.Int64Value, Is.EqualTo(2));
        Assert.That(operations.Single(o => o.MemberPath == "Short").Children![0].Constant.Int64Value, Is.EqualTo(-3));
        Assert.That(operations.Single(o => o.MemberPath == "UShort").Children![0].Constant.Int64Value, Is.EqualTo(4));
        Assert.That(operations.Single(o => o.MemberPath == "Signed").Children![0].Constant.Int64Value, Is.EqualTo(-2));
        Assert.That(operations.Single(o => o.MemberPath == "Unsigned").Children![0].Constant.Int64Value, Is.EqualTo(3));
        Assert.That(operations.Single(o => o.MemberPath == "WideUnsigned").Children![0].Constant.Int64Value, Is.EqualTo(4));
        Assert.That(operations.Single(o => o.MemberPath == "Float").Children![0].Constant.DoubleValue, Is.EqualTo(1.25d));
        Assert.That(operations.Single(o => o.MemberPath == "Decimal").Children![0].Constant.DoubleValue, Is.EqualTo(2.5d));
        Assert.That(operations.Single(o => o.MemberPath == "Priority").Children![0].Constant.Int64Value, Is.EqualTo(1));
        Assert.That(operations.Single(o => o.MemberPath == "TextObject").Children![0].Constant.StringValue, Is.EqualTo(uri.ToString()));
    }

    [Test]
    public void Translate_captures_null_literal_as_null_constant()
    {
        var ir = LatticeValueTransformTranslator.Translate<OldModel, NewModel>(
            _ => new NewModel { Full = null! });

        Assert.That(ir.Children![0].Children![0].Constant.Kind, Is.EqualTo(LatticeConstantKind.Null));
    }

    [Test]
    public void Translate_captures_object_with_null_to_string_as_empty_text()
    {
        var value = new NullStringObject();

        var ir = LatticeValueTransformTranslator.Translate<OldModel, ObjectModel>(
            _ => new ObjectModel { Value = value });

        Assert.That(ir.Children![0].Children![0].Constant.StringValue, Is.Empty);
    }
}
