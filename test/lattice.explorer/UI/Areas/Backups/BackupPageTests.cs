using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// One backup at <c>/backups/{id}</c>: describe, the owning app and its
/// rebuildable trees, restore (each mode, a restore point, cold restore) and
/// delete behind the probe and a type-the-name confirmation, export, and the
/// not-found, error and compact states.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupPageTests : BackupsTestContext
{
    [Test]
    public void The_page_describes_the_backup_and_its_restore_chain()
    {
        Seed(
            FakeBackupControl.Manifest("base1", "sunday", "orders", artifacts: ["a0"]),
            FakeBackupControl.Manifest("inc1", "monday", "orders", baseId: "base1", artifacts: ["a1", "a2"]) with { CapturingClusterId = "eu-west", SetName = "weekly" });

        var cut = RenderAt<BackupPage>("backups/inc1");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("monday"));
            var terms = cut.FindAll(".lt-dl__term").Select(term => term.TextContent).ToArray();
            Assert.That(terms, Is.SupersetOf(new[] { "Backup id", "Tree", "Scope", "Kind", "Captured", "Built on", "Backup set", "Captured on", "Size" }));
            Assert.That(cut.Markup, Does.Contain("4 KiB in 2").And.Contain("artifacts"));
            var chain = cut.FindAll("ol.lt-backups-list li");
            Assert.That(chain.Select(item => item.TextContent.Trim()), Is.EqualTo(new[] { "base1", "inc1 (this backup)" }));
            Assert.That(chain[0].QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("backups/base1"));
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2), "one row per artifact");
            Assert.That(cut.FindAll(".lt-dl__term").Select(term => term.TextContent), Does.Not.Contain("Health"), "health appears only where it applies");
        });
    }

    [Test]
    public void An_unknown_backup_is_not_found()
    {
        var notFound = false;
        Navigation.OnNotFound += (_, _) => notFound = true;

        RenderAt<BackupPage>("backups/nope");

        Assert.That(notFound, Is.True);
    }

    [Test]
    public void A_failed_describe_says_why_and_can_be_retried()
    {
        Seed(FakeBackupControl.Manifest("b1"));
        var calls = 0;
        Backups.Describe = id => ++calls == 1
            ? Task.FromException<BackupChainDescription?>(new LatticeAuthorizationDeniedException())
            : Task.FromResult<BackupChainDescription?>(new BackupChainDescription(Backups.Catalogue[0], [id]));

        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alert]").TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));

        cut.FindAll("button").Single(button => button.TextContent == "Try again").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("nightly")));
    }

    [Test]
    public void The_page_shows_a_skeleton_while_it_describes()
    {
        var describe = new TaskCompletionSource<BackupChainDescription?>();
        Backups.Describe = _ => describe.Task;

        var cut = RenderAt<BackupPage>("backups/b1");

        Assert.That(cut.FindAll(".lt-skeleton"), Is.Not.Empty);
    }

    [Test]
    public void An_app_tree_names_its_app_as_text_and_says_when_it_is_rebuildable()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "a/crm/search"));
        AppsControl.DescribeAsync("crm", Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(new AppDescriptor
        {
            Slug = "crm",
            Version = "1.0.0",
            Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "Contoso" },
            Presentation = new AppPresentationDescriptor { DisplayName = "<b>CRM</b>" },
            Trees = [new AppTreeDescriptor { Name = "search", Rebuildable = true }],
        });

        var cut = RenderAt<BackupPage>("backups/b1");

        cut.WaitUntil(() =>
        {
            var owner = cut.Find("#lt-backups-app-title").ParentElement!;
            Assert.That(owner.TextContent, Does.Contain("belongs to the app <b>CRM</b> (crm)"), "presentation text renders as text");
            Assert.That(owner.QuerySelectorAll("b"), Is.Empty, "no app-supplied markup is ever rendered");
            Assert.That(owner.TextContent, Does.Contain("re-deriving may replace a restore"));
        });
    }

    [Test]
    public void An_app_tree_whose_app_cannot_be_read_is_named_by_its_slug()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "a/crm/orders"));

        var cut = RenderAt<BackupPage>("backups/b1");

        cut.WaitUntil(() =>
        {
            var owner = cut.Find("#lt-backups-app-title").ParentElement!;
            Assert.That(owner.TextContent, Does.Contain("belongs to the app crm."));
            Assert.That(owner.TextContent, Does.Not.Contain("rebuildable"));
        });
    }

    [Test]
    public void A_restore_asks_for_the_tree_name_states_its_consequences_and_starts_a_staged_operation()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(RestoreButton(cut), Is.Not.Null));

        cut.FindAll("select").Single(select => select.QuerySelector("option[value=point-in-time]") is not null).Change("point-in-time");
        cut.Find("form.lt-backups-form").Submit();

        var dialog = cut.Find("[role=alertdialog]");
        Assert.Multiple(() =>
        {
            Assert.That(dialog.TextContent, Does.Contain("Every write made after the backup was taken is dropped"));
            Assert.That(dialog.QuerySelector("code")!.TextContent, Is.EqualTo("orders"));
            Assert.That(ConfirmButton(cut, "Restore").HasAttribute("disabled"), Is.True, "nothing runs until the name is typed");
        });

        cut.Find("[role=alertdialog] input").Input("orders");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => Assert.That(CurrentPath, Is.EqualTo("/backups/operations/1")));
        var operation = Operations.Find("1")!;
        Assert.Multiple(() =>
        {
            Assert.That(operation.Kind, Is.EqualTo(BackupOperationKind.Restore));
            var request = Backups.LastOf<LatticeRestoreRequest>(nameof(ILatticeBackupControl.RestoreBackupAsync));
            Assert.That(request.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
            Assert.That(request.TargetTreeId, Is.EqualTo("orders"));
            Assert.That(request.BackupId, Is.EqualTo("b1"));
        });
    }

    [Test]
    public void The_in_place_restore_says_it_never_overwrites_a_newer_write()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(RestoreButton(cut), Is.Not.Null));

        cut.Find("form.lt-backups-form").Submit();

        Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("any key written since the backup keeps its newer value"));
    }

    [Test]
    public void A_restore_into_another_tree_or_to_an_earlier_point_uses_them()
    {
        Seed(
            FakeBackupControl.Manifest("base1", "sunday", "orders"),
            FakeBackupControl.Manifest("inc1", "monday", "orders", baseId: "base1"));
        var cut = RenderAt<BackupPage>("backups/inc1");
        cut.WaitUntil(() => Assert.That(RestoreButton(cut), Is.Not.Null));

        cut.FindAll("select").Single(select => select.QuerySelector("option[value=base1]") is not null).Change("base1");
        cut.Find("form.lt-backups-form input").Input("orders-copy");
        cut.Find("form.lt-backups-form").Submit();
        cut.Find("[role=alertdialog] input").Input("orders-copy");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.RestoreBackupAsync)), Is.EqualTo(1)));
        var request = Backups.LastOf<LatticeRestoreRequest>(nameof(ILatticeBackupControl.RestoreBackupAsync));
        Assert.Multiple(() =>
        {
            Assert.That(request.BackupId, Is.EqualTo("base1"));
            Assert.That(request.TargetTreeId, Is.EqualTo("orders-copy"));
        });
    }

    [Test]
    public void A_restore_with_no_target_tree_asks_for_one()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(RestoreButton(cut), Is.Not.Null));

        cut.Find("form.lt-backups-form input").Input("  ");
        cut.Find("form.lt-backups-form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Name the tree to restore into."));
        });
    }

    [Test]
    public void A_restore_into_a_rebuildable_app_tree_says_re_deriving_may_be_better()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "a/crm/search"));
        Workspace.DescribeMyAppAsync("crm", Arg.Any<CancellationToken>()).Returns(new WorkspaceAppDescriptor
        {
            Slug = "crm",
            Version = "1.0.0",
            Trees = [new WorkspaceTreeDescriptor { Name = "search", Rebuildable = true }],
        });
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(cut.FindAll("#lt-backups-app-title"), Has.Count.EqualTo(1)));
        cut.WaitUntil(() => Assert.That(RestoreButton(cut), Is.Not.Null));

        cut.Find("form.lt-backups-form").Submit();

        Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("declares this tree rebuildable"));
    }

    [Test]
    public void Cold_restore_is_offered_only_where_the_connection_serves_it()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        var unserved = RenderAt<BackupPage>("backups/b1");
        unserved.WaitUntil(() => Assert.That(RestoreButton(unserved), Is.Not.Null));
        Assert.That(unserved.FindAll("input[type=checkbox]"), Is.Empty);
    }

    [Test]
    public void A_served_cold_restore_runs_as_a_cold_restore()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        Backups.Inventory = () => Task.FromResult(new BackupInventoryReport(1, 1, 1, 0, null, null, 0, 0, 0));
        Backups.ColdRestore = request => Task.FromResult(FakeBackupControl.RestoreResult(request));
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(cut.FindAll("input[type=checkbox]"), Has.Count.EqualTo(1)));

        cut.Find("input[type=checkbox]").Change(true);
        cut.Find("form.lt-backups-form").Submit();
        cut.Find("[role=alertdialog] input").Input("orders");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ColdRestoreAsync)), Is.EqualTo(1)));
        Assert.That(Operations.Find("1")!.Kind, Is.EqualTo(BackupOperationKind.ColdRestore));
    }

    [Test]
    public void A_restricted_identity_sees_no_restore_or_delete_controls()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.AllowAll(scope) with { CanRestore = false, CanDelete = false });

        var cut = RenderAt<BackupPage>("backups/b1");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("You may not restore this backup's tree."));
            Assert.That(cut.Markup, Does.Contain("You may not delete this backup."));
            Assert.That(RestoreButton(cut), Is.Null);
            Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Delete backup..."), Is.Empty);
        });
    }

    [Test]
    public void Delete_asks_for_the_backup_name_states_it_cannot_be_undone_and_returns_to_the_catalogue()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(DeleteButton(cut), Is.Not.Null));

        DeleteButton(cut)!.Click();
        Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("This cannot be undone."));
        cut.Find("[role=alertdialog] input").Input("nightly");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => Assert.That(CurrentPath, Is.EqualTo("/backups")));
        Assert.Multiple(() =>
        {
            Assert.That(Backups.Catalogue, Is.Empty);
            Assert.That(Toasts.Toasts.Single().Message, Is.EqualTo("Deleted backup nightly."));
            Assert.That(Toasts.Toasts.Single().Tone, Is.EqualTo(LtToastTone.Success));
        });
    }

    [Test]
    public void Deleting_a_backup_that_has_already_gone_warns()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        Backups.Delete = _ => Task.FromResult(false);
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(DeleteButton(cut), Is.Not.Null));

        DeleteButton(cut)!.Click();
        cut.Find("[role=alertdialog] input").Input("nightly");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => Assert.That(Toasts.Toasts.Single().Tone, Is.EqualTo(LtToastTone.Warning)));
    }

    [Test]
    public void A_refused_delete_says_why_and_stays()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        Backups.Delete = _ => Task.FromException<bool>(new LatticeAuthorizationDeniedException());
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(DeleteButton(cut), Is.Not.Null));

        DeleteButton(cut)!.Click();
        cut.Find("[role=alertdialog] input").Input("nightly");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find("#lt-backups-delete-title").ParentElement!.QuerySelector("[role=alert]")!.TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));
        Assert.That(CurrentPath, Is.EqualTo("/backups/b1"));
    }

    [Test]
    public void Export_streams_an_artifact_to_a_download()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders", artifacts: ["a1"]));
        Backups.Artifacts["a1"] = [1, 2, 3, 4, 5];
        var module = JSInterop.SetupModule(BackupsAssets.ModuleSpecifier);
        module.SetupVoid("saveArtifact", _ => true).SetVoidResult();
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        cut.Find("tbody button[aria-label='Export artifact a1']").Click();

        cut.WaitUntil(() => Assert.That(module.Invocations["saveArtifact"], Has.Count.EqualTo(1)));
        Assert.That(module.Invocations["saveArtifact"].Single().Arguments[0], Is.EqualTo("b1-a1.bin"));
    }

    [Test]
    public void A_refused_export_says_why_without_starting_a_download()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders", artifacts: ["a1"]));
        Backups.ExportFault = new LatticeAuthorizationDeniedException();
        var module = JSInterop.SetupModule(BackupsAssets.ModuleSpecifier);
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        cut.Find("tbody button[aria-label='Export artifact a1']").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("#lt-backups-artifacts-title").ParentElement!.QuerySelector("[role=alert]")!.TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));
        Assert.That(module.Invocations["saveArtifact"], Is.Empty);
    }

    [Test]
    public void Where_health_applies_the_page_shows_the_latest_report_and_links_to_health()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        Backups.HealthAvailable = () => Task.FromResult(true);
        Backups.HealthReports["b1"] = new BackupHealthReport("b1", BackupHealthStatus.Healthy, true, [], [], DateTimeOffset.UnixEpoch, "ok");

        var cut = RenderAt<BackupPage>("backups/b1");

        cut.WaitUntil(() =>
        {
            var health = cut.FindAll(".lt-dl__row").Single(row => row.QuerySelector(".lt-dl__term")!.TextContent == "Health");
            Assert.That(health.QuerySelector(".lt-pill")!.TextContent.Trim(), Is.EqualTo("Healthy"));
            Assert.That(health.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("backups/health?backup=b1"));
        });
    }

    [Test]
    public void The_artifacts_table_is_compact_below_the_small_breakpoint()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders", artifacts: ["a1"]));
        Backups.Artifacts["a1"] = [1];

        var cut = RenderAt<BackupPage>("backups/b1", LtBreakpoint.Compact);

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(1)));
        cut.Find(".lt-table-list__row button").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog]").TextContent, Does.Contain("Export artifact")));
    }

    [Test]
    public void Physical_tree_ids_never_reach_the_page()
    {
        Seed(FakeBackupControl.Manifest("b1", "nightly", "orders"));
        var cut = RenderAt<BackupPage>("backups/b1");
        cut.WaitUntil(() => Assert.That(RestoreButton(cut), Is.Not.Null));

        Assert.That(cut.Markup, Does.Not.Contain("physical"));
    }

    private static AngleSharp.Dom.IElement? RestoreButton<T>(IRenderedComponent<T> cut)
        where T : Microsoft.AspNetCore.Components.IComponent =>
        cut.FindAll("button").SingleOrDefault(button => button.TextContent == "Restore...");

    private static AngleSharp.Dom.IElement? DeleteButton<T>(IRenderedComponent<T> cut)
        where T : Microsoft.AspNetCore.Components.IComponent =>
        cut.FindAll("button").SingleOrDefault(button => button.TextContent == "Delete backup...");

    private static AngleSharp.Dom.IElement ConfirmButton<T>(IRenderedComponent<T> cut, string text)
        where T : Microsoft.AspNetCore.Components.IComponent =>
        cut.FindAll("[role=alertdialog] button").Single(button => button.TextContent == text);
}
