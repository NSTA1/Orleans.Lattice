using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// Issue #5234: a declarative migration transform reads as plain steps, so the
/// difference a hop makes to a value's shape can be inspected without its JSON.
/// </summary>
[TestFixture]
public sealed class SchemaTransformTextTests
{
    [Test]
    public void Document_steps_read_as_sentences_in_order()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.RenameMember("qty", "quantity"),
            LatticeValueTransform.DropMember("legacy"),
            LatticeValueTransform.SetMember("status", LatticeValueTransform.Const(LatticeConstant.Text("open"))),
            LatticeValueTransform.SetMember("total", LatticeValueTransform.Member("amount")));

        Assert.That(SchemaTransformText.Steps(transform), Is.EqualTo(new[]
        {
            "Renames qty to quantity.",
            "Removes legacy.",
            "Sets status to \"open\".",
            "Sets total to the value of amount.",
        }));
    }

    [Test]
    public void An_empty_passthrough_copies_the_value_unchanged()
    {
        Assert.That(SchemaTransformText.Steps(LatticeValueTransform.Passthrough()), Is.EqualTo(new[] { "Copies the value unchanged." }));
    }

    [Test]
    public void A_bare_operation_is_one_step()
    {
        Assert.That(SchemaTransformText.Steps(LatticeValueTransform.DropMember("legacy")), Is.EqualTo(new[] { "Removes legacy." }));
    }

    [Test]
    public void Conditional_and_computed_values_are_written_out()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember(
                "tier",
                LatticeValueTransform.Conditional(
                    LatticePredicateNode.Compare(
                        LatticeComparisonOperator.GreaterThan,
                        LatticePredicateNode.Member("total"),
                        LatticePredicateNode.Const(LatticeConstant.Integer(100))),
                    LatticeValueTransform.Const(LatticeConstant.Text("gold")),
                    LatticeValueTransform.Const(LatticeConstant.Text("standard")))),
            LatticeValueTransform.SetMember(
                "name",
                LatticeValueTransform.Compute(
                    LatticeComputeOperator.Concat,
                    LatticeValueTransform.Member("first"),
                    LatticeValueTransform.Const(LatticeConstant.Text(" ")),
                    LatticeValueTransform.Member("last"))),
            LatticeValueTransform.SetMember(
                "email",
                LatticeValueTransform.Compute(
                    LatticeComputeOperator.Coalesce,
                    LatticeValueTransform.Member("email"),
                    LatticeValueTransform.Const(LatticeConstant.Null()))));

        Assert.That(SchemaTransformText.Steps(transform), Is.EqualTo(new[]
        {
            "Sets tier to \"gold\" when total > 100, otherwise \"standard\".",
            "Sets name to the text of the value of first, \" \" and the value of last joined.",
            "Sets email to the first present of the value of email and null.",
        }));
    }

    [Test]
    public void A_text_constant_with_a_line_break_stays_on_one_line()
    {
        var transform = LatticeValueTransform.SetMember("note", LatticeValueTransform.Const(LatticeConstant.Text("a\nb")));

        Assert.That(SchemaTransformText.Steps(transform).Single(), Is.EqualTo("Sets note to \"a\\nb\"."));
    }

    [Test]
    public void A_step_it_cannot_read_is_named_rather_than_guessed()
    {
        var malformed = new LatticeValueTransform { Kind = LatticeValueTransformKind.SetMember, MemberPath = "x" };

        Assert.That(SchemaTransformText.Steps(malformed), Is.EqualTo(new[] { "Takes a step this page cannot describe." }));
    }

    [Test]
    public void Many_steps_are_listed_up_to_the_cap_and_the_rest_counted()
    {
        var operations = Enumerable.Range(0, SchemaTransformText.MaximumSteps + 3)
            .Select(index => LatticeValueTransform.DropMember("m" + index))
            .ToArray();

        var steps = SchemaTransformText.Steps(LatticeValueTransform.Passthrough(operations));

        Assert.That(steps, Has.Count.EqualTo(SchemaTransformText.MaximumSteps + 1));
        Assert.That(steps[^1], Is.EqualTo("And 3 more steps."));
    }

    [Test]
    public void Deep_nesting_is_elided()
    {
        var value = LatticeValueTransform.Member("leaf");
        for (var depth = 0; depth < SchemaTransformText.MaximumDepth + 5; depth++)
        {
            value = LatticeValueTransform.Compute(LatticeComputeOperator.Coalesce, value);
        }

        var step = SchemaTransformText.Steps(LatticeValueTransform.SetMember("x", value)).Single();

        Assert.That(step, Does.Contain("..."));
        Assert.That(step, Does.Not.Contain("leaf"));
    }
}
