using Bunit;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>The upgrade review renders every kind of change from the real install flow.</summary>
public sealed partial class AppReviewPageTests
{
    [Test]
    public void Upgrade_lists_added_removed_and_changed_trees_roles_and_bridge_grants()
    {
        var installed = AppsTestData.TaskBoard("1.0.0");
        var next = AppsTestData.TaskBoard("2.0.0", bridge:
        [
            new AppUiBridgeGrantDescriptor { Operation = "data.read" },
            new AppUiBridgeGrantDescriptor { Operation = "ui.notify" },
        ]) with
        {
            Trees = [new AppTreeDescriptor { Name = "notes" }],
            Roles =
            [
                new AppRoleDescriptor { Name = "viewer", Operations = LatticeOperation.Read, Scopes = [new AppRoleScope { Tree = "notes" }] },
                new AppRoleDescriptor { Name = "auditor", Operations = LatticeOperation.Read, Scopes = [new AppRoleScope { Tree = "notes" }] },
            ],
        };

        var cut = ReviewUpgrade(installed, next);
        var diff = cut.Find("section[aria-labelledby=lt-apps-diff]");

        Assert.Multiple(() =>
        {
            Assert.That(diff.QuerySelector("h2")!.TextContent, Is.EqualTo("What changes from v1.0.0 to v2.0.0"));
            Assert.That(diff.QuerySelectorAll("li").Select(item => item.TextContent), Is.EqualTo(new[]
            {
                "Adds the tree a/task-board/notes",
                "No longer declares a/task-board/tasks; its data is kept",
                "Adds the role auditor, which needs a group binding",
                "Drops the role editor",
                "Changes what the role viewer may do",
                "Its UI newly asks to show you short notifications needs re-consent",
                "Its UI no longer asks to write to the tasks tree",
                "Its UI no longer asks to see your display name",
            }));
            Assert.That(diff.QuerySelector("[data-lt-reconsent=true]")!.TextContent,
                Is.EqualTo("This upgrade needs your consent again before it can activate."));
            Assert.That(Control.Installs, Is.Empty, "reviewing a change never installs it");
            Assert.That(Control.ConsentUpdates, Is.Empty, "reviewing is not consent");
        });
    }

    [TestCase("ceiling", "Needs the ceiling to allow it to administer needs re-consent")]
    [TestCase("scope", "Needs a/crm/contacts, keys starting \"eu-\", outside a/task-board/ needs re-consent")]
    [TestCase("bridge", "Its UI newly asks to show you short notifications needs re-consent")]
    public void Upgrade_flags_each_independent_reason_for_reconsent(string change, string expected)
    {
        var installed = AppsTestData.TaskBoard("1.0.0");
        var next = AppsTestData.TaskBoard("2.0.0",
            crossApp: change == "scope",
            editorOperations: change == "ceiling"
                ? LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete | LatticeOperation.Admin
                : LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete,
            bridge: change == "bridge" ? installed.Ui!.Bridge.Add(new AppUiBridgeGrantDescriptor { Operation = "ui.notify" }) : null);

        var cut = ReviewUpgrade(installed, next);
        var diff = cut.Find("section[aria-labelledby=lt-apps-diff]");

        Assert.Multiple(() =>
        {
            Assert.That(diff.QuerySelectorAll("li").Select(item => item.TextContent), Does.Contain(expected));
            Assert.That(diff.QuerySelectorAll("li .lt-apps-flag"), Has.Length.EqualTo(1));
            Assert.That(diff.QuerySelector("[data-lt-reconsent=true]"), Is.Not.Null);
        });
    }

    [Test]
    public void Upgrade_with_only_removals_preserves_data_and_does_not_request_reconsent()
    {
        var installed = AppsTestData.TaskBoard("1.0.0");
        var next = AppsTestData.TaskBoard("2.0.0", bridge: []) with
        {
            Trees = [],
            Roles = [],
        };

        var cut = ReviewUpgrade(installed, next);
        var diff = cut.Find("section[aria-labelledby=lt-apps-diff]");

        Assert.Multiple(() =>
        {
            Assert.That(diff.QuerySelectorAll("li"), Has.Length.EqualTo(6));
            Assert.That(diff.TextContent, Does.Contain("its data is kept").And.Contain("Drops the role editor").And.Contain("Drops the role viewer"));
            Assert.That(diff.QuerySelectorAll(".lt-apps-flag, [data-lt-reconsent]"), Is.Empty);
            Assert.That(diff.TextContent, Does.Not.Contain("Nothing that needs your attention"));
        });
    }

    [Test]
    public void Upgrade_with_the_same_contract_says_nothing_needs_attention()
    {
        var cut = ReviewUpgrade(AppsTestData.TaskBoard("1.0.0"), AppsTestData.TaskBoard("2.0.0"));
        var diff = cut.Find("section[aria-labelledby=lt-apps-diff]");

        Assert.Multiple(() =>
        {
            Assert.That(diff.TextContent, Does.Contain("Nothing that needs your attention: the same trees, roles, ceiling and bridge operations."));
            Assert.That(diff.QuerySelectorAll("li, .lt-apps-flag"), Is.Empty);
        });
    }

    private IRenderedComponent<AppReviewPage> ReviewUpgrade(AppDescriptor installed, AppDescriptor next)
    {
        Control.Install(installed, AppLifecycleState.Enabled);
        Offer(next);
        var cut = RenderReady("/apps/catalogue/in-image/task-board%402.0.0");
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-diff]"), Has.Count.EqualTo(1)));
        return cut;
    }
}
