using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupOperationOwnershipTests : BackupsTestContext
{
    [Test]
    public async Task Retry_releases_the_replaced_follower_and_disposal_stops_all_polling()
    {
        Backups.Statuses["op-1"] = FakeBackupControl.Running("op-1", BackupOperationKinds.Capture, "orders");
        Backups.StatusFault = new TimeoutException("slow");
        var cut = RenderAt<BackupOperationPage>("backups/operations/op-1");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Select(button => button.TextContent), Does.Contain("Try again")));
        await TestPoll.UntilAsync(() => Time.ArmedTimers == 1, "the failed read arms its retry");

        Backups.StatusFault = null;
        await cut.FindAll("button").Single(button => button.TextContent == "Try again").ClickAsync(new MouseEventArgs());
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-operation-progress .lt-pill").TextContent.Trim(), Is.EqualTo("Queued")));
        await TestPoll.UntilAsync(() => Time.ArmedTimers >= 1, "the replacement follower arms its wait");
        Assert.That(Time.ArmedTimers, Is.EqualTo(1), "retry must not leave the previous follower polling");

        cut.Instance.Dispose();
        Assert.That(Time.ArmedTimers, Is.Zero, "every poller belongs to the page");
    }

    [TestCase("success")]
    [TestCase("missing")]
    [TestCase("fault")]
    public async Task Cancellation_of_the_previous_operation_cannot_refresh_or_change_the_current_page(string outcome)
    {
        Backups.Statuses["op-1"] = FakeBackupControl.Running("op-1", BackupOperationKinds.Capture, "orders");
        Backups.Statuses["op-2"] = FakeBackupControl.Running("op-2", BackupOperationKinds.Capture, "stock");
        var pending = new TaskCompletionSource<LatticeOperationStatus?>();
        Backups.CancelOperation = _ => pending.Task;
        var cut = RenderAt<BackupOperationPage>("backups/operations/op-1");
        cut.WaitUntil(() => Assert.That(CancelButton(cut).HasAttribute("disabled"), Is.False));
        var cancellation = CancelButton(cut).ClickAsync(new MouseEventArgs());
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.CancelOperationAsync)), Is.EqualTo(1)));

        await cut.InvokeAsync(() => Navigation.NavigateTo("backups/operations/op-2"));
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Does.Contain("stock")));
        var reads = Backups.StatusReads;
        await cut.InvokeAsync(() =>
        {
            if (outcome == "fault")
            {
                pending.SetException(new InvalidOperationException("old cancellation failed"));
            }
            else
            {
                pending.SetResult(outcome == "missing" ? null : Backups.Statuses["op-1"]);
            }
        });
        await cancellation;

        Assert.Multiple(() =>
        {
            Assert.That(CancelButton(cut).HasAttribute("disabled"), Is.False);
            Assert.That(cut.FindAll("[role=alert]"), Is.Empty);
            Assert.That(Backups.StatusReads, Is.EqualTo(reads), "an old cancel must not refresh the new follower");
        });
    }

    [Test]
    public async Task An_old_cancel_completion_cannot_clear_a_new_cancellations_busy_state()
    {
        Backups.Statuses["op-1"] = FakeBackupControl.Running("op-1", BackupOperationKinds.Capture, "orders");
        Backups.Statuses["op-2"] = FakeBackupControl.Running("op-2", BackupOperationKinds.Capture, "stock");
        var old = new TaskCompletionSource<LatticeOperationStatus?>();
        var current = new TaskCompletionSource<LatticeOperationStatus?>();
        Backups.CancelOperation = id => id == "op-1" ? old.Task : current.Task;
        var cut = RenderAt<BackupOperationPage>("backups/operations/op-1");
        var first = CancelButton(cut).ClickAsync(new MouseEventArgs());
        await cut.InvokeAsync(() => Navigation.NavigateTo("backups/operations/op-2"));
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Does.Contain("stock")));
        Assert.That(CancelButton(cut).HasAttribute("disabled"), Is.False);
        var second = CancelButton(cut).ClickAsync(new MouseEventArgs());
        await cut.InvokeAsync(() => old.SetResult(Backups.Statuses["op-1"]));
        await first;
        Assert.That(CancelButton(cut).HasAttribute("disabled"), Is.True);
        await cut.InvokeAsync(() => current.SetResult(Backups.Statuses["op-2"]));
        await second;
        Assert.That(CancelButton(cut).HasAttribute("disabled"), Is.False);
    }

    private static AngleSharp.Dom.IElement CancelButton(IRenderedComponent<BackupOperationPage> cut) =>
        cut.FindAll("button").Single(button => button.TextContent == "Cancel operation");
}
