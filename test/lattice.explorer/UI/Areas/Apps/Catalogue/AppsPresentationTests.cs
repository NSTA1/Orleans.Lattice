using System.Text;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>How the Apps area words and draws what the facades report.</summary>
[TestFixture]
public sealed class AppsPresentationTests
{
    [Test]
    public void The_display_name_falls_back_to_the_slug()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsPresentation.DisplayName(new AppPresentationDescriptor { DisplayName = "  CRM " }, "crm"), Is.EqualTo("CRM"));
            Assert.That(AppsPresentation.DisplayName(new AppPresentationDescriptor { DisplayName = " " }, "crm"), Is.EqualTo("crm"));
            Assert.That(AppsPresentation.DisplayName(null, "crm"), Is.EqualTo("crm"));
        });
    }

    [Test]
    public void An_icon_becomes_an_image_data_url_only_for_an_allowed_media_type()
    {
        var html = new AppIconAsset { Bytes = Encoding.UTF8.GetBytes("<script/>"), MediaType = "text/html", Sha256 = AppsTestData.Digest() };
        var empty = new AppIconAsset { MediaType = "image/png", Sha256 = AppsTestData.Digest() };

        Assert.Multiple(() =>
        {
            Assert.That(AppsPresentation.IconDataUrl(AppsTestData.Icon), Does.StartWith("data:image/svg+xml;base64,"));
            Assert.That(AppsPresentation.IconDataUrl(html), Is.Null);
            Assert.That(AppsPresentation.IconDataUrl(empty), Is.Null);
            Assert.That(AppsPresentation.IconDataUrl(null), Is.Null);
        });
    }

    [TestCase("task-board", "tb")]
    [TestCase("crm", "cr")]
    [TestCase("a", "a")]
    public void The_monogram_takes_two_letters(string slug, string monogram) =>
        Assert.That(AppsPresentation.Monogram(slug), Is.EqualTo(monogram));

    [Test]
    public void Operations_read_as_words()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsPresentation.OperationsText(LatticeOperation.None), Is.EqualTo("nothing"));
            Assert.That(AppsPresentation.OperationsText(LatticeOperation.Read), Is.EqualTo("read"));
            Assert.That(AppsPresentation.OperationsText(LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete), Is.EqualTo("read, write and delete"));
            Assert.That(AppsPresentation.OperationText(LatticeOperation.AppInstall), Is.EqualTo("install apps"));
            Assert.That(AppsPresentation.Operations, Has.Count.EqualTo(16));
        });
    }

    [Test]
    public void Scopes_read_as_logical_paths_never_physical_ids()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsPresentation.ScopeText(new AppRoleScope { Tree = "tasks" }, "task-board"), Is.EqualTo("a/task-board/tasks"));
            Assert.That(AppsPresentation.ScopeText(new AppRoleScope { Tree = "contacts", App = "crm", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "eu-" }, "task-board"),
                Is.EqualTo("a/crm/contacts, keys starting \"eu-\""));
            Assert.That(AppsPresentation.ScopeText(new AppExceptionScope { App = "crm", Tree = "contacts", Kind = LatticeScopeKind.Key, KeyOrPrefix = "k" }),
                Is.EqualTo("a/crm/contacts, key \"k\""));
            Assert.That(AppsPresentation.ScopeText(new AppExceptionScope { AdoptedTreeId = "legacy" }), Is.EqualTo("legacy (adopted)"));
        });
    }

    [TestCase("data.read", null, "read its own trees")]
    [TestCase("data.write", "tasks", "write its tree tasks")]
    [TestCase("data.delete", null, "delete keys in its own trees")]
    [TestCase("context.user", null, "see your display name")]
    [TestCase("context.read", null, "know its version, the theme and your tenant's display name")]
    [TestCase("nav.sync", null, "keep its page in the address line")]
    [TestCase("ui.notify", null, "show you short notifications")]
    [TestCase("net.fetch", null, "use the unrecognised operation \"net.fetch\"")]
    public void Bridge_grants_read_in_plain_language(string operation, string? tree, string text)
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsPresentation.BridgeText(new AppUiBridgeGrantDescriptor { Operation = operation, Tree = tree }), Is.EqualTo(text));
            Assert.That(AppsPresentation.IsKnownBridgeOperation(operation), Is.EqualTo(operation != "net.fetch"));
        });
    }

    [Test]
    public void The_explorer_recognises_exactly_the_canonical_bridge_vocabulary()
    {
        Assert.That(
            Orleans.Lattice.Apps.AppUiBridgeOperations.All.All(AppsPresentation.IsKnownBridgeOperation),
            Is.True,
            "every operation in the one canonical vocabulary (F1) has plain-language wording");
    }

    [Test]
    public void Source_hints_name_kind_and_capabilities()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsPresentation.SourceHints(AppsTestData.InImage), Is.EqualTo("Static"));
            Assert.That(AppsPresentation.SourceHints(AppsTestData.Feed), Is.EqualTo("Dynamic - search, several versions, acquired on install"));
        });
    }

    [Test]
    public void A_row_state_reads_available_installed_enabled_disabled_failed_or_update()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsPresentation.RowState(AppsTestData.Offer("a", "s")), Is.EqualTo((LtStateRole.Uninstalled, "Available")));
            Assert.That(AppsPresentation.RowState(AppsTestData.Offer("a", "s", "1.0.0", "1.0.0", AppLifecycleState.Installed)), Is.EqualTo((LtStateRole.Installed, "Installed v1.0.0")));
            Assert.That(AppsPresentation.RowState(AppsTestData.Offer("a", "s", "1.0.0", "1.0.0", AppLifecycleState.Enabled)), Is.EqualTo((LtStateRole.Enabled, "Enabled")));
            Assert.That(AppsPresentation.RowState(AppsTestData.Offer("a", "s", "1.0.0", "1.0.0", AppLifecycleState.Disabled)), Is.EqualTo((LtStateRole.Disabled, "Disabled")));
            Assert.That(AppsPresentation.RowState(AppsTestData.Offer("a", "s", "1.0.0", "1.0.0", AppLifecycleState.Failed)), Is.EqualTo((LtStateRole.Failed, "Activation failed")));
            Assert.That(AppsPresentation.RowState(AppsTestData.Offer("a", "s", "2.0.0", "1.0.0", AppLifecycleState.Enabled)), Is.EqualTo((LtStateRole.Lagging, "Update available")));
            Assert.That(AppsPresentation.LifecycleState(AppLifecycleState.Uninstalled, null), Is.EqualTo((LtStateRole.Uninstalled, "Uninstalled")));
        });
    }

    [Test]
    public void Categories_join_or_read_as_none()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsPresentation.CategoriesText(["a", " ", "b"]), Is.EqualTo("a, b"));
            Assert.That(AppsPresentation.CategoriesText([]), Is.Null);
        });
    }
}
