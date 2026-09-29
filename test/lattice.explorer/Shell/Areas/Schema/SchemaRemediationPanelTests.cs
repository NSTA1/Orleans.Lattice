using Bunit;
using Orleans.Lattice.Explorer.Shell.Areas.Schema;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Schema;

/// <summary>
/// The Remediation tab: the resumable status page (a staged order diagram, read
/// from the cluster, re-read on the manual clock only while something runs), the
/// transform editor that starts a remediation after the tree is named, aborted
/// and refused outcomes, and callers who may not remediate or read the status.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaRemediationPanelTests : SchemaTestContext
{
    private IRenderedComponent<SchemaTreePage> Open(string tree = "orders")
    {
        var cut = RenderAt<SchemaTreePage>($"schema/{tree}?tab=remediation");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-operation .lt-skeleton"), Is.Empty));
        return cut;
    }

    private static AngleSharp.Dom.IElement Button(IRenderedComponent<SchemaTreePage> cut, string text) =>
        cut.FindAll("button").Single(button => button.TextContent.Trim() == text);

    private static string[] Stages(IRenderedComponent<SchemaTreePage> cut) =>
        [.. cut.FindAll(".lt-schema-stages__step").Select(step =>
            (step.GetAttribute("aria-current") == "step" ? "*" : string.Empty) + step.TextContent.Trim())];

    [Test]
    public void A_tree_where_nothing_has_run_says_so()
    {
        UseEstate();

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-operation .lt-schema-muted").TextContent, Is.EqualTo("No migration or remediation has run on this tree."));
            Assert.That(cut.Instance.Workspace, Is.Not.Null);
        });
    }

    [Test]
    public void An_operation_started_elsewhere_resumes_from_the_clusters_status_and_is_re_read_while_it_runs()
    {
        UseEstate();
        Schema.Status["orders"] = LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.DryRun, 40, "physical-orders-shadow", "op-42");

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-operation .lt-schema-status").TextContent, Is.EqualTo("An operation is running on this tree."));
            Assert.That(Stages(cut), Is.EqualTo(new[] { "Confirmed", "*Running in the cluster", "Finished" }));
            Assert.That(cut.Find(".lt-schema-operation dl.lt-dl").TextContent, Does.Contain("Checking every value"));
            Assert.That(cut.Find(".lt-schema-operation dl.lt-dl").TextContent, Does.Contain("op-42"));
            Assert.That(cut.Markup, Does.Not.Contain("physical-orders-shadow"), "a physical tree id is never shown");
            Assert.That(cut.Find(".lt-schema-stages__step[aria-current] .lt-node").ClassList, Does.Contain("lt-node--join"));
        });
        Assert.That(Time.ArmedTimers, Is.EqualTo(1));

        Schema.Status["orders"] = LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.Cutover, 90, "physical-orders-shadow", "op-42");
        Time.Advance(SchemaOperationStatus.PollInterval);
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-operation dl.lt-dl").TextContent, Does.Contain("Cutting over")));

        Schema.Status["orders"] = LatticeSchemaRemediationReport.Completed(90, "physical-orders-shadow", "op-42");
        Time.Advance(SchemaOperationStatus.PollInterval);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-operation .lt-schema-status").TextContent, Is.EqualTo("The last operation finished, and the tree serves its result."));
            Assert.That(Stages(cut), Is.EqualTo(new[] { "Confirmed", "Running in the cluster", "*Finished" }));
            Assert.That(Time.ArmedTimers, Is.Zero, "nothing is re-read once nothing runs");
        });
    }

    [Test]
    public void Starting_a_remediation_needs_steps_and_the_tree_named_then_follows_it_to_the_end()
    {
        UseEstate();
        Schema.OperationGate = new TaskCompletionSource<LatticeSchemaRemediationReport>();
        var cut = Open();

        Button(cut, "Review and start...").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-editor .lt-schema-error").TextContent, Is.EqualTo("Add at least one step.")));

        cut.FindAll(".lt-schema-builder input")[0].Input("currency");
        cut.FindAll(".lt-schema-builder input")[1].Input("EUR");
        Button(cut, "Add step").Click();
        cut.FindAll(".lt-schema-builder select")[0].Change(nameof(SchemaTransformStepKind.Remove));
        cut.FindAll(".lt-schema-builder input")[0].Input("legacy");
        Button(cut, "Add step").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-rules__item .lt-schema-rules__text").Select(step => step.TextContent),
            Is.EqualTo(new[] { "Set currency to \"EUR\"", "Remove legacy" })));

        Button(cut, "Review and start...").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("rewritten through 2 steps")));
        Assert.That(cut.Find("[role=alertdialog] button[type=submit]").HasAttribute("disabled"), Is.True);
        cut.Find("[role=alertdialog] input").Input("orders");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Schema.CountOf("Remediate"), Is.EqualTo(1));
            Assert.That(Schema.LastRemediation!.Value.Policy, Is.SameAs(Schema.Policies["orders"]));
            Assert.That(Schema.LastRemediation!.Value.Transform.Children, Has.Length.EqualTo(2));
            Assert.That(cut.Find(".lt-schema-operation .lt-schema-status").TextContent, Is.EqualTo("Running in the cluster."));
            Assert.That(Stages(cut), Is.EqualTo(new[] { "Confirmed", "*Running in the cluster", "Finished" }));
            Assert.That(cut.Find(".lt-schema-operation dl.lt-dl").TextContent, Does.Contain("Remediating every value through 2 steps"));
            Assert.That(Button(cut, "Review and start...").HasAttribute("disabled"), Is.True, "one operation per tree");
            Assert.That(cut.FindAll(".lt-schema-rules__item"), Is.Empty, "the editor clears once it has started");
        });

        Schema.OperationGate.SetResult(LatticeSchemaRemediationReport.Completed(1234, "physical-orders-shadow", "op-9"));

        cut.WaitUntil(() =>
        {
            Assert.That(Stages(cut), Is.EqualTo(new[] { "Confirmed", "Running in the cluster", "*Finished" }));
            Assert.That(cut.Find(".lt-schema-operation dl.lt-dl").TextContent, Does.Contain("1,234"));
            Assert.That(cut.Markup, Does.Not.Contain("physical-orders-shadow"));
            Assert.That(Button(cut, "Clear this result"), Is.Not.Null);
        });

        Button(cut, "Clear this result").Click();
        cut.WaitUntil(() => Assert.That(Operations.Find("orders"), Is.Null));
    }

    [Test]
    public void An_aborted_remediation_names_the_first_value_that_still_fails_as_text()
    {
        UseEstate();
        Schema.OperationGate = new TaskCompletionSource<LatticeSchemaRemediationReport>();
        var cut = Open();
        cut.FindAll(".lt-schema-builder select")[0].Change(nameof(SchemaTransformStepKind.Rename));
        cut.FindAll(".lt-schema-builder input")[0].Input("cur");
        cut.FindAll(".lt-schema-builder input")[1].Input("currency");
        Button(cut, "Add step").Click();
        Button(cut, "Review and start...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=alertdialog]"), Has.Count.EqualTo(1)));
        cut.Find("[role=alertdialog] input").Input("orders");
        cut.Find("[role=alertdialog] form").Submit();
        cut.WaitUntil(() => Assert.That(Schema.CountOf("Remediate"), Is.EqualTo(1)));

        Schema.OperationGate.SetResult(LatticeSchemaRemediationReport.Aborted(
            17, "order/42", "currency does not match", System.Text.Encoding.UTF8.GetBytes("<script>alert(1)</script>"), "op-3"));

        cut.WaitUntil(() =>
        {
            Assert.That(Stages(cut), Is.EqualTo(new[] { "Confirmed", "Running in the cluster", "*Stopped, nothing cut over" }));
            var abort = cut.Find(".lt-schema-abort");
            Assert.That(abort.TextContent, Does.Contain("Nothing was cut over"));
            Assert.That(abort.QuerySelectorAll("dd").Select(value => value.TextContent.Trim()), Is.EqualTo(new[] { "order/42", "currency does not match" }));
            Assert.That(abort.QuerySelector("pre")!.TextContent, Is.EqualTo("<script>alert(1)</script>"));
            Assert.That(abort.QuerySelectorAll("script"), Is.Empty, "a value is text, never markup");
        });
    }

    [Test]
    public void A_refused_remediation_is_explained()
    {
        UseEstate();
        Schema.Faults["Remediate"] = new InvalidOperationException("A remediation with different parameters is already in flight.");
        var cut = Open();
        cut.FindAll(".lt-schema-builder select")[0].Change(nameof(SchemaTransformStepKind.Remove));
        cut.FindAll(".lt-schema-builder input")[0].Input("legacy");
        Button(cut, "Add step").Click();
        Button(cut, "Review and start...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=alertdialog]"), Has.Count.EqualTo(1)));
        cut.Find("[role=alertdialog] input").Input("orders");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Stages(cut), Is.EqualTo(new[] { "Confirmed", "Running in the cluster", "*Refused" }));
            Assert.That(cut.Find(".lt-schema-operation .lt-schema-error").TextContent,
                Is.EqualTo("Could not remediate this tree. A remediation with different parameters is already in flight."));
        });
    }

    [Test]
    public void A_step_that_is_not_complete_is_explained()
    {
        UseEstate();
        var cut = Open();
        cut.FindAll(".lt-schema-builder select")[0].Change(nameof(SchemaTransformStepKind.Set));
        cut.FindAll(".lt-schema-builder select")[1].Change(nameof(SchemaConstantKind.Number));
        cut.FindAll(".lt-schema-builder input")[0].Input("count");
        cut.FindAll(".lt-schema-builder input")[1].Input("lots");

        Button(cut, "Add step").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-builder .lt-schema-error").TextContent, Is.EqualTo("Enter a number, such as 42 or 2.5."));
            Assert.That(cut.FindAll(".lt-schema-rules__item"), Is.Empty);
        });

        cut.FindAll(".lt-schema-builder select")[1].Change(nameof(SchemaConstantKind.Null));
        Assert.That(cut.FindAll(".lt-schema-builder input"), Has.Count.EqualTo(1), "a null needs no value");
        Button(cut, "Add step").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-rules__text").TextContent, Is.EqualTo("Set count to null")));

        cut.FindAll("button").Single(button => button.GetAttribute("aria-label") == "Remove step 1").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-rules__item"), Is.Empty));
    }

    [Test]
    public void A_tree_with_no_policy_cannot_be_remediated_yet()
    {
        UseTrees("audit");
        Schema.Versions["audit"] = new(1, 1);

        var cut = Open("audit");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-editor .lt-schema-note").TextContent, Does.Contain("this tree has none"));
            Assert.That(cut.FindAll(".lt-schema-builder"), Is.Empty);
        });
    }

    [Test]
    public void A_caller_who_may_not_remediate_sees_only_the_status()
    {
        UseEstate();
        Schema.Capabilities["orders"] = FakeSchemaControl.ReadOnly;

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-schema-operation"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll(".lt-schema-editor"), Is.Empty);
        });
    }

    [Test]
    public void A_caller_who_may_not_read_the_status_is_told_so()
    {
        UseEstate();
        Schema.Capabilities["orders"] = tree => FakeSchemaControl.ReadOnly(tree) with { CanViewRemediationStatus = false };

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-operation .lt-schema-muted").TextContent, Is.EqualTo("You may not read this tree's operation status."));
            Assert.That(Schema.CountOf("GetRemediationStatus"), Is.Zero);
        });
    }

    [Test]
    public void A_status_that_does_not_load_can_be_tried_again()
    {
        UseEstate();
        Schema.Faults["GetRemediationStatus"] = new NotSupportedException("x");
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-operation .lt-empty__body").TextContent, Is.EqualTo(SchemaFailure.NotServed)));

        Schema.Faults.Remove("GetRemediationStatus");
        cut.Find(".lt-schema-operation .lt-empty__actions button").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-operation .lt-schema-muted").TextContent, Is.EqualTo("No migration or remediation has run on this tree.")));
    }

    [Test]
    public async Task The_status_page_is_left_while_an_operation_runs_and_stops_reading()
    {
        UseEstate();
        Schema.Status["orders"] = LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.Build, 1, null, "op");
        var cut = Open();
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.EqualTo(1)));

        await DisposeComponentsAsync();

        Assert.That(Time.ArmedTimers, Is.Zero);
    }
}
