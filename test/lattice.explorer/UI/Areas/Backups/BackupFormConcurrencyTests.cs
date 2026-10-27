using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Areas.Backups;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>Backup forms keep validation and mutations attached to one coherent user action.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupFormConcurrencyTests : BackupsTestContext
{
    [Test]
    [TestCase("Name", "")]
    [TestCase("Name", "renamed")]
    [TestCase("Scope", "prefix")]
    public async Task Capture_details_changed_during_tree_validation_require_a_new_submit(string field, string value)
    {
        var client = Services.UseTreeCatalogue("orders");
        client.CatalogGate = new TaskCompletionSource();
        var cut = RenderAt<BackupCapturePage>("backups/new?tree=orders");
        SuggestionFields.Field(cut, "Name").Input("nightly");
        var submit = cut.Find("form").SubmitAsync();
        cut.WaitUntil(() => Assert.That(client.Calls, Does.Contain("ListTreesAsync")));

        if (field == "Scope")
        {
            SuggestionFields.Field(cut, field).Change(value);
        }
        else
        {
            SuggestionFields.Field(cut, field).Input(value);
        }

        await cut.InvokeAsync(() => client.CatalogGate.SetResult());
        await submit;

        Assert.Multiple(() =>
        {
            Assert.That(Operations.Recent, Is.Empty, "no capture may combine old validation with new details");
            Assert.That(CurrentPath, Is.EqualTo("/backups/new?tree=orders"));
            Assert.That(cut.Find("[role=alert]").TextContent, Does.Contain("details changed"));
        });

        SuggestionFields.Field(cut, "Name").Input("reviewed");
        if (field == "Scope")
        {
            SuggestionFields.Field(cut, "Key prefix").Input("order/");
        }

        cut.Find("form").Submit();
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartBackupAsync)), Is.EqualTo(1)));
        var request = Backups.LastOf<LatticeBackupCaptureRequest>(nameof(ILatticeBackupOperations.StartBackupAsync));
        Assert.Multiple(() =>
        {
            Assert.That(request.Name, Is.EqualTo("reviewed"));
            Assert.That(request.Scope.Kind, Is.EqualTo(field == "Scope" ? BackupScopeKind.Prefix : BackupScopeKind.WholeTree));
        });
    }

    [Test]
    public void Switching_to_a_set_does_not_import_a_tree_the_picker_refuses()
    {
        Services.UseTreeCatalogue("orders");
        var cut = RenderAt<BackupCapturePage>("backups/new?tree=missing");
        SuggestionFields.Field(cut, "Name").Input("nightly");

        SuggestionFields.Field(cut, "Kind").Change("set");

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Tree"), Does.Contain("No tree is named missing.")));
        Assert.That(cut.FindAll(".lt-backups-trees__item"), Is.Empty);
        cut.Find("form").Submit();
        Assert.That(Operations.Recent, Is.Empty);
    }

    [Test]
    [TestCase(false)]
    [TestCase(true)]
    public async Task A_pending_kind_switch_does_not_import_a_tree_after_it_is_superseded(bool leave)
    {
        var client = Services.UseTreeCatalogue("orders");
        client.CatalogGate = new TaskCompletionSource();
        var cut = RenderAt<BackupCapturePage>("backups/new?tree=orders");
        var change = SuggestionFields.Field(cut, "Kind").ChangeAsync(new Microsoft.AspNetCore.Components.ChangeEventArgs { Value = "set" });
        cut.WaitUntil(() => Assert.That(client.Calls, Does.Contain("ListTreesAsync")));
        if (leave)
        {
            await DisposeComponentsAsync();
        }
        else
        {
            SuggestionFields.Field(cut, "Kind").Change("incremental");
        }

        await cut.InvokeAsync(() => client.CatalogGate.SetResult());
        await change;

        Assert.That(Operations.Recent, Is.Empty);
        if (!leave)
        {
            Assert.Multiple(() =>
            {
                Assert.That(SuggestionFields.Field(cut, "Kind").QuerySelector("option[selected]")?.GetAttribute("value"), Is.EqualTo("incremental"));
                Assert.That(cut.FindAll(".lt-backups-trees__item"), Is.Empty);
            });
        }
    }

    [Test]
    [TestCase(false)]
    [TestCase(true)]
    public async Task Cancellation_blocks_conflicting_schedule_changes_until_it_settles(bool fails)
    {
        var pending = new TaskCompletionSource();
        Backups.CancelSchedule = (_, _) => pending.Task;
        Backups.ScopeStatuses["orders"] = new BackupScopeStatus(
            BackupScopeSelector.WholeTree("orders"), true, true, null, null, null, null,
            BackupScopeRunOutcome.None, 0);
        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody button"), Has.Count.EqualTo(2)));
        cut.Find("tbody button").Click();
        var cancel = cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == "Cancel schedule")
            .ClickAsync(new MouseEventArgs());
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.CancelScheduleAsync)), Is.EqualTo(1)));

        await cut.Find("form.lt-backups-form").SubmitAsync();
        var saves = Backups.CountOf(nameof(ILatticeBackupControl.ScheduleBackupAsync));
        var disabled = cut.Find("form.lt-backups-form button[type=submit]").HasAttribute("disabled");
        await cut.InvokeAsync(() =>
        {
            if (fails)
            {
                pending.SetException(new InvalidOperationException("cancel failed"));
            }
            else
            {
                pending.SetResult();
            }
        });
        await cancel;

        Assert.Multiple(() =>
        {
            Assert.That(saves, Is.Zero, "registering while cancellation is pending can undo the user's cancellation");
            Assert.That(disabled, Is.True);
            Assert.That(cut.Find("form.lt-backups-form button[type=submit]").HasAttribute("disabled"), Is.False);
            Assert.That(cut.FindAll("tbody button").All(button => !button.HasAttribute("disabled")), Is.True);
        });
        if (fails)
        {
            Assert.That(cut.Find("form.lt-backups-form [role=alert]").TextContent, Does.Contain("cancel failed"));
        }

        cut.Find("form.lt-backups-form").Submit();
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ScheduleBackupAsync)), Is.EqualTo(1)));
    }

    [Test]
    public async Task An_open_cancellation_dialog_cannot_cancel_while_a_save_is_pending()
    {
        var pending = new TaskCompletionSource();
        Backups.Schedule = _ => pending.Task;
        Backups.ScopeStatuses["orders"] = new BackupScopeStatus(
            BackupScopeSelector.WholeTree("orders"), true, false, null, null, null, null,
            BackupScopeRunOutcome.None, 0);
        var cut = RenderAt<BackupSchedulesPage>("backups/schedules?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody button"), Has.Count.EqualTo(1)));
        cut.Find("tbody button").Click();
        var save = cut.Find("form.lt-backups-form").SubmitAsync();
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ScheduleBackupAsync)), Is.EqualTo(1)));
        await cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == "Cancel schedule")
            .ClickAsync(new MouseEventArgs());
        await cut.InvokeAsync(() => pending.SetResult());
        await save;

        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.CancelScheduleAsync)), Is.Zero);
    }
}
