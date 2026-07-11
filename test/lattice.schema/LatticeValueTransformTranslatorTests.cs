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
}
