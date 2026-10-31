using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>A conditional set step (#5476): written in the editor, read back, and sent as the conditional it reads as.</summary>
public sealed partial class SchemaRemediationPanelTests
{
    [Test]
    public void A_set_step_limited_by_a_condition_is_read_back_and_starts_a_remediation_of_only_those_values()
    {
        UseEstate();
        Schema.OperationGate = new TaskCompletionSource<LatticeSchemaRemediationReport>();
        var cut = Open();

        Assert.That(cut.FindAll(".lt-schema-builder select"), Has.Count.EqualTo(2), "no condition fields until the condition is turned on");
        cut.FindAll(".lt-schema-builder select")[1].Change(nameof(SchemaConstantKind.Number));
        cut.FindAll(".lt-schema-builder input")[0].Input("total");
        cut.FindAll(".lt-schema-builder input")[1].Input("0");
        cut.Find(".lt-schema-builder button[role=switch]").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-builder select"), Has.Count.EqualTo(4)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-schema-builder button[role=switch]").GetAttribute("aria-checked"), Is.EqualTo("true"));
            Assert.That(cut.FindAll(".lt-schema-builder select")[2].QuerySelectorAll("option").Select(option => option.TextContent),
                Is.EqualTo(new[] { "less than", "at most", "equal to", "not equal to", "at least", "greater than" }));
        });

        cut.FindAll(".lt-schema-builder input")[2].Input("zero");
        Button(cut, "Add step").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-builder .lt-schema-error").TextContent,
            Is.EqualTo("The condition's value: Enter a number, such as 42 or 2.5.")));

        cut.FindAll(".lt-schema-builder input")[2].Input("0");
        Button(cut, "Add step").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-rules__text").TextContent, Is.EqualTo("Set total to 0 where total is less than 0")));
        Assert.That(cut.FindAll(".lt-schema-builder select"), Has.Count.EqualTo(2), "the condition is off again for the next step");

        Button(cut, "Review and start...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=alertdialog]"), Has.Count.EqualTo(1)));
        cut.Find("[role=alertdialog] input").Input("orders");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => Assert.That(Schema.CountOf("StartRemediation"), Is.EqualTo(1)));
        Assert.That(Schema.LastRemediation!.Value.Transform, Is.EqualTo(LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember(
                "total",
                LatticeValueTransform.Conditional(
                    LatticePredicateNode.Compare(LatticeComparisonOperator.LessThan, LatticePredicateNode.Member("total"), LatticePredicateNode.Const(LatticeConstant.Integer(0))),
                    LatticeValueTransform.Const(LatticeConstant.Integer(0)),
                    LatticeValueTransform.Member("total"))))));
    }

    [Test]
    public void A_null_condition_needs_no_compared_value()
    {
        UseEstate();
        var cut = Open();

        cut.FindAll(".lt-schema-builder input")[0].Input("region");
        cut.FindAll(".lt-schema-builder input")[1].Input("eu");
        cut.Find(".lt-schema-builder button[role=switch]").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-builder select"), Has.Count.EqualTo(4)));
        Assert.That(cut.FindAll(".lt-schema-builder input"), Has.Count.EqualTo(3));

        cut.FindAll(".lt-schema-builder select")[2].Change(nameof(LatticeComparisonOperator.Equal));
        cut.FindAll(".lt-schema-builder select")[3].Change(nameof(SchemaConstantKind.Null));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-builder input"), Has.Count.EqualTo(2), "a null needs no value"));

        Button(cut, "Add step").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-rules__text").TextContent, Is.EqualTo("Set region to \"eu\" where region is equal to null")));
    }
}
