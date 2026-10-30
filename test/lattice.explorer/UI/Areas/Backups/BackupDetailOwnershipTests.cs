using Bunit;
using Microsoft.AspNetCore.Components.Web;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>Backup detail reads and actions publish only to their originating page lifetime.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupDetailOwnershipTests : BackupsTestContext
{
    [Test]
    public async Task A_superseded_description_fault_cannot_replace_the_current_backup()
    {
        SeedBackups();
        var pending = new TaskCompletionSource<BackupChainDescription?>();
        Backups.Describe = id => id == "b1" ? pending.Task : Task.FromResult<BackupChainDescription?>(
            new BackupChainDescription(Backups.Catalogue[1], ["b2"]));
        var cut = RenderAt<BackupPage>("backups/b1");
        await NavigateAsync(cut, "b2");

        var renders = cut.RenderCount;
        await cut.InvokeAsync(() => pending.SetException(new InvalidOperationException("old description failed")));
        cut.WaitUntil(() => Assert.That(cut.RenderCount, Is.GreaterThan(renders)));

        AssertCurrent(cut);
    }

    [Test]
    public async Task A_superseded_capability_probe_cannot_hide_the_current_backups_actions()
    {
        SeedBackups();
        var pending = new TaskCompletionSource<BackupScopeCapabilities>();
        Backups.Probe = scope => scope.TreeId == "orders" ? pending.Task : Task.FromResult(FakeBackupControl.AllowAll(scope));
        var cut = RenderAt<BackupPage>("backups/b1");
        await NavigateAsync(cut, "b2");
        cut.WaitUntil(() => Assert.That(DeleteButton(cut), Is.Not.Null));

        var renders = cut.RenderCount;
        await cut.InvokeAsync(() => pending.SetResult(FakeBackupControl.DenyAll(BackupScopeSelector.WholeTree("orders"))));
        cut.WaitUntil(() => Assert.That(cut.RenderCount, Is.GreaterThan(renders)));

        Assert.Multiple(() =>
        {
            AssertCurrent(cut);
            Assert.That(DeleteButton(cut), Is.Not.Null);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_superseded_health_reply_cannot_replace_the_current_backups_report(bool fails)
    {
        SeedBackups();
        Backups.HealthAvailable = () => Task.FromResult(true);
        var pending = new TaskCompletionSource<BackupHealthReport?>();
        Backups.GetHealth = id => id == "b1" ? pending.Task : Task.FromResult<BackupHealthReport?>(Health("b2", BackupHealthStatus.Healthy));
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetBackupHealthAsync)), Is.EqualTo(1)));
        await NavigateAsync(cut, "b2");
        cut.WaitUntil(() => Assert.That(HealthText(cut), Is.EqualTo("Healthy")));

        var renders = cut.RenderCount;
        await cut.InvokeAsync(() =>
        {
            if (fails)
            {
                pending.SetException(new InvalidOperationException("old health failed"));
            }
            else
            {
                pending.SetResult(Health("b1", BackupHealthStatus.Missing));
            }
        });
        cut.WaitUntil(() => Assert.That(cut.RenderCount, Is.GreaterThan(renders)));

        Assert.That(HealthText(cut), Is.EqualTo("Healthy"));
    }

    [Test]
    public async Task A_superseded_app_description_cannot_replace_the_current_backups_app()
    {
        Seed(
            FakeBackupControl.Manifest("b1", "first backup", "a/old/orders"),
            FakeBackupControl.Manifest("b2", "second backup", "a/current/stock"));
        var pending = new TaskCompletionSource<AppDescriptor?>();
        AppsControl.DescribeAsync("old", Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(pending.Task);
        AppsControl.DescribeAsync("current", Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(new AppDescriptor
        {
            Slug = "current",
            Version = "1.0.0",
            Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "Contoso" },
            Presentation = new AppPresentationDescriptor { DisplayName = "Current app" },
        });
        var cut = RenderAt<BackupPage>("backups/b1");
        await NavigateAsync(cut, "b2");
        cut.WaitUntil(() => Assert.That(cut.Find("#lt-backups-app-title").ParentElement!.TextContent, Does.Contain("Current app")));

        var renders = cut.RenderCount;
        await cut.InvokeAsync(() => pending.SetResult(new AppDescriptor
        {
            Slug = "old",
            Version = "1.0.0",
            Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "Contoso" },
            Presentation = new AppPresentationDescriptor { DisplayName = "Old app" },
        }));
        cut.WaitUntil(() => Assert.That(cut.RenderCount, Is.GreaterThan(renders)));

        Assert.That(cut.Find("#lt-backups-app-title").ParentElement!.TextContent, Does.Contain("Current app").And.Not.Contain("Old app"));
    }

    [Test]
    public async Task Returning_to_the_same_backup_does_not_revive_its_original_health_read()
    {
        SeedBackups();
        Backups.HealthAvailable = () => Task.FromResult(true);
        var pending = new TaskCompletionSource<BackupHealthReport?>();
        var firstReads = 0;
        Backups.GetHealth = id => id == "b1" && ++firstReads == 1
            ? pending.Task
            : Task.FromResult<BackupHealthReport?>(Health(id, BackupHealthStatus.Healthy));
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(firstReads, Is.EqualTo(1)));
        await NavigateAsync(cut, "b2");
        await cut.InvokeAsync(() => Navigation.NavigateTo("backups/b1"));
        cut.Render();
        cut.WaitUntil(() => Assert.That(HealthText(cut), Is.EqualTo("Healthy")));

        var renders = cut.RenderCount;
        await cut.InvokeAsync(() => pending.SetResult(Health("b1", BackupHealthStatus.Missing)));
        cut.WaitUntil(() => Assert.That(cut.RenderCount, Is.GreaterThan(renders)));

        Assert.That(HealthText(cut), Is.EqualTo("Healthy"));
    }

    [TestCase("success")]
    [TestCase("fault")]
    [TestCase("cancel")]
    public async Task A_superseded_deletion_cannot_redirect_or_fault_the_current_backup(string outcome)
    {
        SeedBackups();
        var pending = new TaskCompletionSource<bool>();
        Backups.Delete = _ => pending.Task;
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(DeleteButton(cut), Is.Not.Null));
        DeleteButton(cut).Click();
        cut.Find("[role=alertdialog] input").Input("first backup");
        var deleting = cut.Find("[role=alertdialog] form").SubmitAsync();
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.DeleteBackupAsync)), Is.EqualTo(1)));
        await NavigateAsync(cut, "b2");

        await cut.InvokeAsync(() => Complete(pending, outcome));
        await deleting;

        Assert.Multiple(() =>
        {
            Assert.That(CurrentPath, Is.EqualTo("/backups/b2"));
            Assert.That(Toasts.Toasts, Is.Empty);
            AssertCurrent(cut);
        });
    }

    [TestCase("success")]
    [TestCase("fault")]
    [TestCase("cancel")]
    public async Task A_superseded_export_cannot_disable_or_fault_the_current_backup(string outcome)
    {
        SeedBackups();
        var pending = new TaskCompletionSource<bool>();
        Backups.BeforeExport = (id, _) => id == "b1" ? pending.Task : Task.CompletedTask;
        var module = JSInterop.SetupModule(BackupsAssets.ModuleSpecifier);
        module.SetupVoid("saveArtifact", _ => true).SetVoidResult();
        var cut = RenderAt<BackupPage>("backups/b1");
        var exporting = ExportButton(cut).ClickAsync(new MouseEventArgs());
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ExportArtifactAsync)), Is.EqualTo(1)));
        await NavigateAsync(cut, "b2");
        var currentWasDisabled = ExportButton(cut).HasAttribute("disabled");

        await cut.InvokeAsync(() => Complete(pending, outcome));
        await exporting;

        Assert.Multiple(() =>
        {
            Assert.That(currentWasDisabled, Is.False, "an old transfer must not disable the next backup");
            AssertCurrent(cut);
            Assert.That(module.Invocations["saveArtifact"], Is.Empty, "a superseded stream must not start a download");
        });
    }

    [Test]
    public async Task An_old_export_completion_cannot_clear_a_new_export_of_the_same_artifact()
    {
        SeedBackups();
        var first = new TaskCompletionSource<bool>();
        var second = new TaskCompletionSource<bool>();
        Backups.BeforeExport = (id, _) => id == "b1" ? first.Task : second.Task;
        var module = JSInterop.SetupModule(BackupsAssets.ModuleSpecifier);
        module.SetupVoid("saveArtifact", _ => true).SetVoidResult();
        var cut = RenderAt<BackupPage>("backups/b1");
        var oldExport = ExportButton(cut).ClickAsync(new MouseEventArgs());
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ExportArtifactAsync)), Is.EqualTo(1)));
        await NavigateAsync(cut, "b2");
        var couldStart = !ExportButton(cut).HasAttribute("disabled");
        var newExport = couldStart ? ExportButton(cut).ClickAsync(new MouseEventArgs()) : Task.CompletedTask;
        if (couldStart)
        {
            cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ExportArtifactAsync)), Is.EqualTo(2)));
        }

        await cut.InvokeAsync(() => first.SetResult(true));
        await oldExport;
        var stillBusy = ExportButton(cut).HasAttribute("disabled");
        await cut.InvokeAsync(() => second.SetResult(true));
        await newExport;

        Assert.Multiple(() =>
        {
            Assert.That(couldStart, Is.True);
            Assert.That(stillBusy, Is.True, "the current export still owns the busy indicator");
            Assert.That(ExportButton(cut).HasAttribute("disabled"), Is.False);
            Assert.That(module.Invocations["saveArtifact"].Select(call => call.Arguments[0]), Is.EqualTo(new[] { "b2-a1.bin" }));
        });
    }

    [Test]
    public async Task A_confirmed_delete_dialog_is_not_carried_to_another_backup()
    {
        SeedBackups();
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(DeleteButton(cut), Is.Not.Null));
        DeleteButton(cut).Click();

        await NavigateAsync(cut, "b2");

        Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_deletion_completed_after_disposal_publishes_neither_toast_nor_fault(bool fails)
    {
        SeedBackups();
        var pending = new TaskCompletionSource<bool>();
        Backups.Delete = _ => pending.Task;
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(DeleteButton(cut), Is.Not.Null));
        DeleteButton(cut).Click();
        cut.Find("[role=alertdialog] input").Input("first backup");
        var deleting = cut.Find("[role=alertdialog] form").SubmitAsync();
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.DeleteBackupAsync)), Is.EqualTo(1)));
        await DisposeComponentsAsync();

        await cut.InvokeAsync(() => Complete(pending, fails ? "fault" : "success"));
        await deleting;

        Assert.Multiple(() =>
        {
            Assert.That(Toasts.Toasts, Is.Empty);
            Assert.That(CurrentPath, Is.EqualTo("/backups/b1"));
        });
    }

    private void SeedBackups()
    {
        Seed(
            FakeBackupControl.Manifest("b1", "first backup", "orders", artifacts: ["a1"]),
            FakeBackupControl.Manifest("b2", "second backup", "stock", artifacts: ["a1"]));
        Backups.Artifacts["a1"] = [1, 2, 3];
    }

    private async Task NavigateAsync(IRenderedComponent<BackupPage> cut, string id)
    {
        await cut.InvokeAsync(() => Navigation.NavigateTo("backups/" + id));
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("second backup")));
    }

    private static void AssertCurrent(IRenderedComponent<BackupPage> cut)
    {
        Assert.That(cut.Find("h1").TextContent, Is.EqualTo("second backup"));
        Assert.That(cut.FindAll("[role=alert]"), Is.Empty);
    }

    private static AngleSharp.Dom.IElement DeleteButton(IRenderedComponent<BackupPage> cut) =>
        cut.FindAll("button").Single(button => button.TextContent == "Delete backup...");

    private static AngleSharp.Dom.IElement ExportButton(IRenderedComponent<BackupPage> cut) =>
        cut.Find("tbody button[aria-label='Export artifact a1']");

    private static string HealthText(IRenderedComponent<BackupPage> cut) =>
        cut.FindAll(".lt-dl__row").Single(row => row.QuerySelector(".lt-dl__term")!.TextContent == "Health")
            .QuerySelector(".lt-pill")!.TextContent.Trim();

    private static BackupHealthReport Health(string id, BackupHealthStatus status) =>
        new(id, status, status == BackupHealthStatus.Healthy, [], [], DateTimeOffset.UnixEpoch, "report");

    private static void Complete(TaskCompletionSource<bool> pending, string outcome)
    {
        if (outcome == "fault")
        {
            pending.SetException(new InvalidOperationException("superseded operation failed"));
        }
        else if (outcome == "cancel")
        {
            pending.SetCanceled();
        }
        else
        {
            pending.SetResult(true);
        }
    }
}
