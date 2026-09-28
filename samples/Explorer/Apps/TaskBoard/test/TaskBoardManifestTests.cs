using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Samples.Explorer.TaskBoard.Tests;

/// <summary>The task board's manifest: one tree, two roles, a presentation and a UI that asks only for what it uses.</summary>
[TestFixture]
public sealed class TaskBoardManifestTests
{
    [Test]
    public void The_manifest_validates()
    {
        using var stream = new MemoryStream(TaskBoardFiles.ReadEmbedded(TaskBoardApp.ManifestResourceName));
        var result = AppManifestParser.Parse(stream);

        Assert.That(result.IsValid, Is.True, string.Join("; ", result.Errors.Select(e => e.Path + ": " + e.Message)));
    }

    [Test]
    public void The_identity_is_the_task_board_slug()
    {
        var identity = TaskBoardFiles.Manifest().Identity;

        Assert.That(identity.Slug.Value, Is.EqualTo(TaskBoardApp.Slug));
        Assert.That(identity.Provenance.Source, Is.EqualTo(InImageAppSource.SourceKey));
    }

    [Test]
    public void The_app_declares_one_tree_named_tasks()
    {
        Assert.That(TaskBoardFiles.Manifest().Trees.Select(t => t.Name), Is.EqualTo(new[] { "tasks" }));
    }

    [Test]
    public void The_viewer_reads_and_the_editor_reads_and_writes_only_its_own_tree()
    {
        var roles = TaskBoardFiles.Manifest().Roles.ToDictionary(r => r.Name);

        Assert.Multiple(() =>
        {
            Assert.That(roles.Keys, Is.EquivalentTo(new[] { "viewer", "editor" }));
            Assert.That(roles["viewer"].Operations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.RangeRead));
            Assert.That(roles["editor"].Operations,
                Is.EqualTo(LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete));
            foreach (var role in roles.Values)
            {
                Assert.That(role.Scopes, Has.Length.EqualTo(1), role.Name);
                Assert.That(role.Scopes[0].Tree, Is.EqualTo("tasks"), role.Name);
                Assert.That(role.Scopes[0].App, Is.Null, role.Name + " stays in the app's own namespace");
                Assert.That(role.Scopes[0].Kind, Is.EqualTo(LatticeScopeKind.Tree), role.Name);
            }
        });
    }

    [Test]
    public void The_presentation_has_a_name_summary_description_and_svg_icon()
    {
        var presentation = TaskBoardFiles.Manifest().Presentation!;

        Assert.Multiple(() =>
        {
            Assert.That(presentation.DisplayName, Is.EqualTo("Task board"));
            Assert.That(presentation.Summary, Is.Not.Null.And.Not.Empty);
            Assert.That(presentation.Description, Is.Not.Null.And.Not.Empty);
            Assert.That(presentation.Icon!.Path, Is.EqualTo("icon.svg"));
        });
    }

    [Test]
    public void The_ui_is_a_fragment_one_stylesheet_and_one_module()
    {
        var ui = TaskBoardFiles.Manifest().Ui!;

        Assert.Multiple(() =>
        {
            Assert.That(ui.Entry, Is.EqualTo("index.html"));
            Assert.That(ui.Styles, Is.EqualTo(new[] { "app.css" }));
            Assert.That(ui.Scripts, Has.Length.EqualTo(1));
            Assert.That(ui.Scripts![0].Path, Is.EqualTo("app.mjs"));
            Assert.That(ui.Scripts[0].Module, Is.True);
            Assert.That(ui.MinProtocol, Is.EqualTo(AppUiProtocol.Current));
        });
    }

    [Test]
    public void The_ui_requests_exactly_the_pilot_bridge_operations()
    {
        var bridge = TaskBoardFiles.Manifest().Ui!.Bridge!;

        Assert.That(bridge.Select(b => b.Operation), Is.EqualTo(new[]
        {
            AppUiBridgeOperations.ContextRead,
            AppUiBridgeOperations.DataRead,
            AppUiBridgeOperations.DataWrite,
            AppUiBridgeOperations.DataDelete,
            AppUiBridgeOperations.NavSync,
            AppUiBridgeOperations.UiNotify,
        }));
        Assert.That(bridge.Where(b => AppUiBridgeOperations.IsDataOperation(b.Operation)).Select(b => b.Trees),
            Is.All.EqualTo(new[] { "tasks" }));
    }

    [Test]
    public void The_module_uses_every_operation_it_requests_and_no_other()
    {
        var bridge = TaskBoardFiles.Manifest().Ui!.Bridge!.Select(b => b.Operation).ToHashSet();
        var module = TaskBoardFiles.ReadText("ui/app.mjs");

        Assert.Multiple(() =>
        {
            foreach (var operation in AppUiBridgeOperations.All)
            {
                var used = module.Contains("request(\"" + operation + "\"", StringComparison.Ordinal);
                Assert.That(used, Is.EqualTo(bridge.Contains(operation)), operation);
            }
        });
    }

    [Test]
    public void The_entry_is_a_valid_fragment()
    {
        Assert.That(AppManifestValidator.ValidateUiEntryFragment(TaskBoardFiles.ReadAsset("index.html")), Is.Empty);
    }
}
