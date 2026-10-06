using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Apps.App;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The app pages' sections and their plain-language text: which sections exist for whom,
/// and how operations, bridge operations, scopes, shapes and retention read.
/// </summary>
[TestFixture]
public sealed class AppPageTabsAndTextTests
{
    private static AppPageModel RoleHolder(bool canOpen) => new() { Slug = Slug, Version = "1.0.0", CanOpen = canOpen };

    [Test]
    public void Every_caller_with_access_gets_the_manifest_sections_and_only_those()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppPageTabs.For(RoleHolder(canOpen: false)),
                Is.EqualTo(new[] { "overview", "trees", "roles", "tools", "subscriptions", "replication" }));
            Assert.That(AppPageTabs.For(RoleHolder(canOpen: true)), Is.EqualTo(AppPageTabs.For(RoleHolder(canOpen: false))), "opening an app is never a tab");
            Assert.That(AppPageTabs.For(RoleHolder(canOpen: false) with { Admin = Admin() }), Does.Contain("consent"));
            Assert.That(AppPageTabs.IsOffered(RoleHolder(canOpen: true), "settings"), Is.False);
            Assert.That(AppPageTabs.IsOffered(RoleHolder(canOpen: true), null), Is.False);
            Assert.That(() => AppPageTabs.For(null!), Throws.ArgumentNullException);
            Assert.That(() => AppPageTabs.IsOffered(null!, "trees"), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_window_is_offered_exactly_when_the_caller_can_open_the_app_and_open_is_no_longer_a_section()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppPageTabs.Window, Is.EqualTo("window"));
            Assert.That(AppPageTabs.IsOffered(RoleHolder(canOpen: true), AppPageTabs.Window), Is.True);
            Assert.That(AppPageTabs.IsOffered(RoleHolder(canOpen: false), AppPageTabs.Window), Is.False);
            Assert.That(AppPageTabs.IsOffered(RoleHolder(canOpen: true), "open"), Is.False, "the old in-console section is gone; its address redirects to the window");
            Assert.That(AppPageTabs.All, Does.Not.Contain(AppPageTabs.Window));
            Assert.That(AppPageTabs.For(RoleHolder(canOpen: true)), Does.Not.Contain(AppPageTabs.Window));
        });
    }

    [Test]
    public void Every_section_is_a_lower_case_segment_with_a_title()
    {
        Assert.Multiple(() =>
        {
            foreach (var tab in AppPageTabs.All)
            {
                Assert.That(tab, Is.EqualTo(tab.ToLowerInvariant()));
                Assert.That(AppPageTabs.Title(tab), Is.Not.Empty);
            }

            Assert.That(() => AppPageTabs.Title("settings"), Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void The_display_name_falls_back_to_the_slug()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RoleHolder(false).DisplayName, Is.EqualTo(Slug));
            Assert.That((RoleHolder(false) with { Presentation = Presentation() with { DisplayName = " " } }).DisplayName, Is.EqualTo(Slug));
            Assert.That((RoleHolder(false) with { Presentation = Presentation() }).DisplayName, Is.EqualTo("CRM"));
        });
    }

    [Test]
    public void Operations_read_as_words()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppPageText.Operations(LatticeOperation.None), Is.EqualTo("none"));
            Assert.That(AppPageText.Operations(LatticeOperation.Read | LatticeOperation.CrdtApply | LatticeOperation.AppInstall), Is.EqualTo("read, CRDT apply, app install"));
            Assert.That(AppPageText.Operations((LatticeOperation)(1 << 20)), Is.EqualTo("operation 1048576"));
        });
    }

    [TestCase("context.read", "read its launch context")]
    [TestCase("context.user", "see your display name")]
    [TestCase("data.read", "read its own trees")]
    [TestCase("data.write", "write to its own trees")]
    [TestCase("data.delete", "delete from its own trees")]
    [TestCase("nav.sync", "keep the address line in step with its page")]
    [TestCase("ui.notify", "show notifications")]
    [TestCase("app.install", "an operation this Explorer does not recognise")]
    public void Bridge_operations_read_in_plain_language(string operation, string text)
    {
        Assert.That(AppPageText.BridgeOperation(operation), Is.EqualTo(text));
    }

    [Test]
    public void Bridge_reach_names_the_tree_or_every_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppPageText.BridgeReach(new AppUiBridgeGrantDescriptor { Operation = "data.read" }), Is.EqualTo("every declared tree"));
            Assert.That(AppPageText.BridgeReach(new AppUiBridgeGrantDescriptor { Operation = "data.write", Tree = "orders" }), Is.EqualTo("tree orders"));
            Assert.That(AppPageText.BridgeReach(new AppUiBridgeGrantDescriptor { Operation = "nav.sync" }), Is.EqualTo("not tree-scoped"));
            Assert.That(() => AppPageText.BridgeReach(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Scopes_read_as_logical_addresses_with_their_extent()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppPageText.Scope(new AppRoleScope { Tree = "orders" }, Slug), Is.EqualTo("a/crm/orders, whole tree"));
            Assert.That(AppPageText.Scope(new AppRoleScope { Tree = "orders", Kind = LatticeScopeKind.Key, KeyOrPrefix = "k1" }, Slug), Is.EqualTo("a/crm/orders, key k1"));
            Assert.That(AppPageText.Scope(new AppRoleScope { Tree = "invoices", App = "billing", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "eu/" }, Slug), Is.EqualTo("a/billing/invoices, prefix eu/"));
            Assert.That(AppPageText.ExceptionTarget(new AppExceptionScope { App = "billing", Tree = "invoices" }), Is.EqualTo("a/billing/invoices"));
            Assert.That(AppPageText.ExceptionTarget(new AppExceptionScope { AdoptedTreeId = "legacy" }), Is.EqualTo("legacy tree legacy"));
            Assert.That(AppPageText.ExceptionExtent(new AppExceptionScope { Kind = LatticeScopeKind.Key, KeyOrPrefix = "k" }), Is.EqualTo("key k"));
            Assert.That(AppPageText.ExceptionExtent(new AppExceptionScope { Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "p/" }), Is.EqualTo("prefix p/"));
            Assert.That(AppPageText.ExceptionExtent(new AppExceptionScope()), Is.EqualTo("whole tree"));
            Assert.That(AppPageText.SubscriptionSource(new AppSubscriptionDescriptor { Name = "s", Tree = "orders" }, Slug), Is.EqualTo("a/crm/orders"));
            Assert.That(() => AppPageText.Scope(null!, Slug), Throws.ArgumentNullException);
            Assert.That(() => AppPageText.ExceptionTarget(null!), Throws.ArgumentNullException);
            Assert.That(() => AppPageText.ExceptionExtent(null!), Throws.ArgumentNullException);
            Assert.That(() => AppPageText.SubscriptionSource(null!, Slug), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Shapes_and_retention_read_as_numbers_and_periods()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppPageText.Declared(null), Is.EqualTo("host default"));
            Assert.That(AppPageText.Declared(1024), Is.EqualTo("1,024"));
            Assert.That(AppPageText.Retention(null), Is.EqualTo("host default"));
            Assert.That(AppPageText.Retention(TimeSpan.FromDays(1)), Is.EqualTo("1 day"));
            Assert.That(AppPageText.Retention(TimeSpan.FromDays(30)), Is.EqualTo("30 days"));
            Assert.That(AppPageText.Retention(TimeSpan.FromHours(36)), Is.EqualTo("36 hours"));
            Assert.That(AppPageText.Retention(TimeSpan.FromSeconds(90)), Is.EqualTo("2 minutes"));
            Assert.That(AppPageText.Count(1, "shard"), Is.EqualTo("1 shard"));
        });
    }

    [TestCase(59.5, "1 hour")]
    [TestCase(119.1, "2 hours")]
    [TestCase(1_439.2, "1 day")]
    [TestCase(58.5, "59 minutes")]
    [TestCase(90.0, "90 minutes")]
    [TestCase(0.0, "0 minutes")]
    public void A_retention_is_named_on_its_rounded_figure_so_it_never_reads_a_whole_larger_unit(double minutes, string expected)
    {
        // The minutes round up; deciding the unit before that rounding read 59.5
        // minutes as "60 minutes" and 1,439.2 minutes as "1,440 minutes".
        Assert.That(AppPageText.Retention(TimeSpan.FromMinutes(minutes)), Is.EqualTo(expected));
    }

    [TestCase(AppLifecycleState.Installed, "Installed")]
    [TestCase(AppLifecycleState.Enabled, "Enabled")]
    [TestCase(AppLifecycleState.Disabled, "Disabled")]
    [TestCase(AppLifecycleState.Uninstalled, "Uninstalled")]
    [TestCase(AppLifecycleState.Failed, "Activation failed")]
    [TestCase(AppLifecycleState.NotInstalled, "Not installed")]
    public void Lifecycle_states_read_as_labels(AppLifecycleState state, string text)
    {
        Assert.That(AppPageText.State(state), Is.EqualTo(text));
    }

    [Test]
    public void Trees_project_from_both_read_paths_without_their_adoption_id()
    {
        var owned = AppPageTree.From(new WorkspaceTreeDescriptor { Name = "orders", ShardCount = 4, Adopted = false });
        var adopted = AppPageTree.From(new AppTreeDescriptor { Name = "legacy", AdoptedTreeId = AdoptedTreeId, WalPartitions = 2 });

        Assert.Multiple(() =>
        {
            Assert.That((owned.Name, owned.Adopted, owned.ShardCount), Is.EqualTo(("orders", false, (int?)4)));
            Assert.That((adopted.Name, adopted.Adopted, adopted.WalPartitions), Is.EqualTo(("legacy", true, (int?)2)));
            Assert.That(adopted.ToString(), Does.Not.Contain(AdoptedTreeId));
            Assert.That(() => AppPageTree.From((WorkspaceTreeDescriptor)null!), Throws.ArgumentNullException);
            Assert.That(() => AppPageTree.From((AppTreeDescriptor)null!), Throws.ArgumentNullException);
        });
    }
}
