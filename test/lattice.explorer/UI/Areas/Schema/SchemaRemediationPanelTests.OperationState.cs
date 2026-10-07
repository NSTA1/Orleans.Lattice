using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

public sealed partial class SchemaRemediationPanelTests
{
    [Test]
    public void A_successful_retry_refreshes_status_after_a_prior_local_abort()
    {
        UseEstate();
        var first = new TaskCompletionSource<LatticeSchemaRemediationReport>();
        Schema.OperationGate = first;
        var cut = Open();

        void StartRename()
        {
            cut.FindAll(".lt-schema-builder select")[0].Change(nameof(SchemaTransformStepKind.Rename));
            cut.FindAll(".lt-schema-builder input")[0].Input("currency");
            cut.FindAll(".lt-schema-builder input")[1].Input("currencyCode");
            Button(cut, "Add step").Click();
            Button(cut, "Review and start...").Click();
            cut.WaitUntil(() => Assert.That(cut.FindAll("[role=alertdialog]"), Has.Count.EqualTo(1)));
            cut.Find("[role=alertdialog] input").Input("orders");
            cut.Find("[role=alertdialog] form").Submit();
        }

        StartRename();
        cut.WaitUntil(() => Assert.That(Schema.OperationStatuses["op-1"].State, Is.EqualTo(Orleans.Lattice.Api.Operations.LatticeOperationState.Running)));

        first.SetResult(LatticeSchemaRemediationReport.Aborted(17, "order/42", "still fails", Array.Empty<byte>(), "op-1"));
        cut.WaitUntil(() => Assert.That(Schema.OperationStatuses["op-1"].IsTerminal, Is.True));
        Time.Advance(SchemaOperationStatus.PollInterval);
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-abort").TextContent, Does.Contain("still fails")));
        cut.WaitUntil(() => Assert.That(Button(cut, "Review and start...").HasAttribute("disabled"), Is.False));

        var second = new TaskCompletionSource<LatticeSchemaRemediationReport>();
        Schema.OperationGate = second;
        StartRename();
        cut.WaitUntil(() =>
        {
            Assert.That(Schema.CountOf("StartRemediation"), Is.EqualTo(2));
            Assert.That(cut.FindAll(".lt-schema-abort"), Is.Empty);
        });

        second.SetResult(LatticeSchemaRemediationReport.Completed(1234, "physical-orders-shadow", "op-2"));
        cut.WaitUntil(() => Assert.That(Schema.OperationStatuses["op-2"].IsTerminal, Is.True));
        Time.Advance(SchemaOperationStatus.PollInterval);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-schema-abort"), Is.Empty);
            Assert.That(cut.Find(".lt-schema-operation dl.lt-dl").TextContent, Does.Contain("1,234"));
            Assert.That(cut.Markup, Does.Not.Contain("still fails"));
        });
    }

    [Test]
    public void A_status_read_overtaken_by_a_new_operation_does_not_repaint_the_old_report()
    {
        UseEstate();
        Schema.Status["orders"] = LatticeSchemaRemediationReport.Aborted(17, "order/42", "still fails", Array.Empty<byte>(), "op-0");
        var operation = new TaskCompletionSource<LatticeSchemaRemediationReport>();
        Schema.OperationGate = operation;
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-abort").TextContent, Does.Contain("still fails")));

        var staleRead = new TaskCompletionSource();
        Schema.StatusGate = staleRead;
        Button(cut, "Refresh status").Click();
        cut.WaitUntil(() => Assert.That(Schema.StatusGate, Is.Null));

        cut.FindAll(".lt-schema-builder select")[0].Change(nameof(SchemaTransformStepKind.Rename));
        cut.FindAll(".lt-schema-builder input")[0].Input("currency");
        cut.FindAll(".lt-schema-builder input")[1].Input("currencyCode");
        Button(cut, "Add step").Click();
        Button(cut, "Review and start...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=alertdialog]"), Has.Count.EqualTo(1)));
        cut.Find("[role=alertdialog] input").Input("orders");
        cut.Find("[role=alertdialog] form").Submit();
        cut.WaitUntil(() => Assert.That(Schema.CountOf("StartRemediation"), Is.EqualTo(1)));

        operation.SetResult(LatticeSchemaRemediationReport.Completed(1234, "physical-orders-shadow", "op-1"));
        cut.WaitUntil(() => Assert.That(Schema.OperationStatuses["op-1"].IsTerminal, Is.True));
        Time.Advance(SchemaOperationStatus.PollInterval);
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-operation dl.lt-dl").TextContent, Does.Contain("1,234")));

        staleRead.SetResult();
        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-schema-abort"), Is.Empty);
            Assert.That(cut.Find(".lt-schema-operation dl.lt-dl").TextContent, Does.Contain("1,234"));
            Assert.That(cut.Markup, Does.Not.Contain("still fails"));
        });
    }
}
