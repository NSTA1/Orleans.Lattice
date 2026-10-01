using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

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
    public void The_member_fields_suggest_the_members_the_policy_names_and_accept_any_other()
    {
        UseEstate();
        Schema.Policies["orders"] = new LatticeSchemaPolicy([LatticeSchemaRule.Regex("^[a-z]+$", "customer.name"), LatticeSchemaRule.Regex(".+", "customer.email")]);
        var cut = Open();

        Assert.That(
            Orleans.Lattice.Explorer.Tests.UI.Suggestions.SuggestionFields.Offers(cut, "Member", "customer", atLeast: 2),
            Is.EqualTo(new[] { "customer.email", "customer.name" }));

        Orleans.Lattice.Explorer.Tests.UI.Suggestions.SuggestionFields.Box(cut, "Member").Input("order.total");
        cut.WaitUntil(() => Assert.That(Orleans.Lattice.Explorer.Tests.UI.Suggestions.SuggestionFields.ErrorOf(cut, "Member"), Is.Null, "a member the policy does not name is still accepted"));
    }
    [Test]
    public async Task The_member_source_names_each_member_once_is_forgotten_on_request_and_fails_closed()
    {
        Schema.Policies["orders"] = new LatticeSchemaPolicy([LatticeSchemaRule.Regex("a", "m"), LatticeSchemaRule.Regex("b", "m"), LatticeSchemaRule.Json()]);
        var facades = new SchemaFacades(Services);
        var tree = "orders";
        var source = new SchemaMemberSuggestionSource(facades, () => tree);

        var members = await source.SuggestAsync(string.Empty, 5, CancellationToken.None);
        Schema.Faults["GetPolicy"] = new InvalidOperationException("down");
        var remembered = await source.SuggestAsync(string.Empty, 5, CancellationToken.None);
        source.Invalidate();
        var failed = await source.SuggestAsync(string.Empty, 5, CancellationToken.None);
        tree = string.Empty;
        var none = await source.SuggestAsync(string.Empty, 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(members.Items.Select(item => item.Value), Is.EqualTo(new[] { "m" }));
            Assert.That(remembered.Items, Has.Count.EqualTo(1), "read once per tree and tenant");
            Assert.That(failed.UnavailableReason, Is.EqualTo(SchemaMemberSuggestionSource.UnavailableReason));
            Assert.That(none, Is.SameAs(Orleans.Lattice.Explorer.UI.Design.Components.LtSuggestionSet.Empty));
        });
    }
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
    public void A_fresh_circuit_finds_the_operation_id_in_the_report_and_draws_cluster_progress()
    {
        UseEstate();
        Schema.Status["orders"] = LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.Build, 3, "physical-orders-shadow", "op-42");
        Schema.OperationStatuses["op-42"] = FakeSchemaControl.RunningOperation("op-42", SchemaOperationKinds.Remediation, "orders") with
        {
            Phase = SchemaOperationPhases.Build,
            PhaseIndex = 1,
            CompletedUnits = 3,
            TotalUnits = 8,
            UnitName = SchemaOperationPhases.ValuesUnit,
        };

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Building the remediated copy"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("3 of 8 values"));
            Assert.That(Stages(cut), Is.EqualTo(new[] { "Checking every value", "*Building the remediated copy", "Cutting over" }));
            Assert.That(cut.Markup, Does.Not.Contain("physical-orders-shadow"));
        });
    }

    [Test]
    public void Cluster_progress_advances_when_the_clock_ticks()
    {
        UseEstate();
        Schema.Status["orders"] = LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.Build, 3, null, "op-42");
        Schema.OperationStatuses["op-42"] = FakeSchemaControl.RunningOperation("op-42", SchemaOperationKinds.Remediation, "orders") with
        {
            Phase = SchemaOperationPhases.Build,
            PhaseIndex = 1,
            CompletedUnits = 3,
            TotalUnits = 8,
            UnitName = SchemaOperationPhases.ValuesUnit,
        };
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("3 of 8 values")));

        Schema.MoveOperation("op-42", status => status with { CompletedUnits = 5 });
        Time.Advance(SchemaOperationStatus.PollInterval);

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("5 of 8 values")));
    }

    [Test]
    public void Cancelling_a_running_operation_calls_the_cluster_and_requires_the_grant()
    {
        UseEstate();
        Schema.Status["orders"] = LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.Build, 3, null, "op-42");
        Schema.OperationStatuses["op-42"] = FakeSchemaControl.RunningOperation("op-42", SchemaOperationKinds.Remediation, "orders");
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Cancel operation"), Is.EqualTo(1)));

        Button(cut, "Cancel operation").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Schema.OperationStatuses["op-42"].CancelRequested, Is.True);
            Assert.That(Schema.CountOf(nameof(Orleans.Lattice.Api.Operations.ILatticeOperations.CancelOperationAsync)), Is.EqualTo(1));
        });

        Schema.Capabilities["audit"] = FakeSchemaControl.ReadOnly;
        Schema.Status["audit"] = LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.Build, 3, null, "op-43");
        Schema.OperationStatuses["op-43"] = FakeSchemaControl.RunningOperation("op-43", SchemaOperationKinds.Remediation, "audit");
        var readOnly = Open("audit");
        readOnly.WaitUntil(() => Assert.That(readOnly.FindAll("button").Count(button => button.TextContent.Trim() == "Cancel operation"), Is.Zero));
    }

    [Test]
    public void A_cancelled_cluster_operation_renders_as_cancelled_and_stops_polling()
    {
        UseEstate();
        Schema.Status["orders"] = LatticeSchemaRemediationReport.Cancelled(6, "op-42");
        Schema.OperationStatuses["op-42"] = FakeSchemaControl.RunningOperation("op-42", SchemaOperationKinds.Remediation, "orders") with
        {
            State = Orleans.Lattice.Api.Operations.LatticeOperationState.Cancelled,
            Phase = SchemaOperationPhases.Build,
            PhaseIndex = 1,
            CompletedUnits = 6,
            TotalUnits = 8,
            UnitName = SchemaOperationPhases.ValuesUnit,
            FinishedAtUtc = Time.GetUtcNow(),
        };

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-status").TextContent, Is.EqualTo("The last operation was cancelled before cutover."));
            Assert.That(cut.Find(".lt-operation-progress__state").TextContent, Does.Contain("Cancelled"));
            Assert.That(Time.ArmedTimers, Is.Zero);
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
            Assert.That(Schema.CountOf("StartRemediation"), Is.EqualTo(1));
            Assert.That(Schema.LastRemediation!.Value.Policy, Is.SameAs(Schema.Policies["orders"]));
            Assert.That(Schema.LastRemediation!.Value.Transform.Children, Has.Length.EqualTo(2));
            Assert.That(cut.Find(".lt-schema-operation .lt-schema-status").TextContent, Is.EqualTo("Running in the cluster."));
            Assert.That(Stages(cut), Is.EqualTo(new[] { "*Checking every value", "Building the remediated copy", "Cutting over" }));
            Assert.That(cut.Find(".lt-schema-operation dl.lt-dl").TextContent, Does.Contain("Remediating every value through 2 steps"));
            Assert.That(Button(cut, "Review and start...").HasAttribute("disabled"), Is.True, "one operation per tree");
            Assert.That(cut.FindAll(".lt-schema-rules__item"), Is.Empty, "the editor clears once it has started");
        });

        Schema.OperationGate.SetResult(LatticeSchemaRemediationReport.Completed(1234, "physical-orders-shadow", "op-9"));
        Button(cut, "Refresh status").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Stages(cut), Is.EqualTo(new[] { "Checking every value", "Building the remediated copy", "Cutting over" }));
            Assert.That(cut.Find(".lt-schema-operation dl.lt-dl").TextContent, Does.Contain("1,234"));
            Assert.That(cut.Markup, Does.Not.Contain("physical-orders-shadow"));
        });
    }

    [Test]
    public void A_renamed_members_new_name_is_a_text_box_that_flags_a_name_the_policy_already_uses()
    {
        UseEstate();
        Schema.Policies["orders"] = new LatticeSchemaPolicy([LatticeSchemaRule.Regex("^[a-z]+$", "customer.name"), LatticeSchemaRule.Regex(".+", "customer.email")]);
        var cut = Open();
        cut.FindAll(".lt-schema-builder select")[0].Change(nameof(SchemaTransformStepKind.Rename));

        var name = Orleans.Lattice.Explorer.Tests.UI.Suggestions.SuggestionFields.NameBox(cut, "New name");
        name.Input("customer");
        Assert.That(cut.FindAll(".lt-schema-builder [role=option]"), Is.Empty, "members are not offered as the new name");

        Orleans.Lattice.Explorer.Tests.UI.Suggestions.SuggestionFields.NameBox(cut, "New name").Input("customer.email");
        cut.WaitUntil(() =>
        {
            Assert.That(Orleans.Lattice.Explorer.Tests.UI.Suggestions.SuggestionFields.FlagOf(cut, "New name"), Is.EqualTo("The policy already names a member with this name."));
            Assert.That(Orleans.Lattice.Explorer.Tests.UI.Suggestions.SuggestionFields.ErrorOf(cut, "New name"), Is.Null, "renaming onto a named member is allowed, and flagged");
        });
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
        cut.WaitUntil(() => Assert.That(Schema.CountOf("StartRemediation"), Is.EqualTo(1)));

        Schema.OperationGate.SetResult(LatticeSchemaRemediationReport.Aborted(
            17, "order/42", "currency does not match", System.Text.Encoding.UTF8.GetBytes("<script>alert(1)</script>"), "op-3"));
        Button(cut, "Refresh status").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Stages(cut), Is.EqualTo(new[] { "Checking every value", "Building the remediated copy", "Cutting over" }));
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
        Schema.Faults["StartRemediation"] = new InvalidOperationException("A remediation with different parameters is already in flight.");
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
