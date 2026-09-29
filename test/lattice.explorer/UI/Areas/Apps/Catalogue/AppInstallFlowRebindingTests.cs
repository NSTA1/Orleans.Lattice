using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// The install flow's role re-binding path (issue #3884), driven against fakes: it starts only
/// from the installed version's review, starts from the recorded bindings, compares recorded
/// and proposed groups, and applies only through <see cref="ILatticeAppRoleBindings"/>.
/// </summary>
[TestFixture]
public sealed class AppInstallFlowRebindingTests
{
    private FakeAppCatalog _catalog = null!;
    private FakeAppsControl _control = null!;
    private ServiceProvider _services = null!;

    [SetUp]
    public void SetUp()
    {
        _catalog = new FakeAppCatalog();
        _control = new FakeAppsControl();
        _services = Services(withRebinding: true);
    }

    [TearDown]
    public void TearDown() => _services.Dispose();

    [Test]
    public async Task Rebinding_starts_from_the_recorded_bindings_and_compares_each_role()
    {
        var flow = await InstalledAsync();

        Assert.That(flow.CanRebind, Is.True);
        flow.Bind("editor", "changed-before-starting");
        flow.BeginRebind();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.BindRoles));
            Assert.That(flow.IsRebinding, Is.True);
            Assert.That(flow.Bindings, Is.EquivalentTo(new Dictionary<string, string> { ["viewer"] = "g-viewers", ["editor"] = "g-editors" }));
            Assert.That(flow.HasBindingChanges, Is.False);
        });

        flow.Bind("editor", "g-new-editors");
        flow.Bind("viewer", null);
        flow.ConfirmBindings();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.ConfirmBindings), "a role may be left unbound while re-binding");
            Assert.That(flow.BindingChanges, Is.EqualTo(new[]
            {
                new AppRoleBindingChange("viewer", "g-viewers", null),
                new AppRoleBindingChange("editor", "g-editors", "g-new-editors"),
            }));
            Assert.That(flow.HasBindingChanges, Is.True);
        });
    }

    [Test]
    public async Task Committing_applies_only_the_bound_roles_updates_the_installed_version_and_ends_rebinding()
    {
        var flow = await InstalledAsync();
        flow.BeginRebind();
        flow.Bind("viewer", null);
        flow.ConfirmBindings();

        await flow.CommitBindingsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Rebound));
            Assert.That(flow.IsRebinding, Is.False);
            Assert.That(_control.RoleBindingUpdates.Single().RoleBindings.Select(binding => (binding.RoleName, binding.GroupId)),
                Is.EqualTo(new[] { ("editor", "g-editors") }));
            Assert.That(flow.Installed!.RoleBindings.Select(binding => binding.RoleName), Is.EqualTo(new[] { "editor" }));
            Assert.That(flow.Installed.State, Is.EqualTo(AppLifecycleState.Enabled));
            Assert.That(_control.Calls.Select(call => call.Verb), Is.EqualTo(new[] { "rebind" }));
        });

        flow.Back();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Review));
            Assert.That(flow.Bindings, Is.EquivalentTo(new Dictionary<string, string> { ["editor"] = "g-editors" }));
        });
    }

    [Test]
    public async Task Stepping_back_out_of_rebinding_discards_the_draft()
    {
        var flow = await InstalledAsync();
        flow.BeginRebind();
        flow.Bind("editor", "g-draft");
        flow.ConfirmBindings();

        flow.Back();
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.BindRoles));
        flow.Back();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Review));
            Assert.That(flow.IsRebinding, Is.False);
            Assert.That(flow.Bindings["editor"], Is.EqualTo("g-editors"));
            Assert.That(_control.RoleBindingUpdates, Is.Empty);
        });
    }

    [Test]
    public async Task A_refused_rebinding_fails_back_to_the_confirmation()
    {
        _control.Failures["rebind"] = new KeyNotFoundException();
        var flow = await InstalledAsync();
        flow.BeginRebind();
        flow.ConfirmBindings();

        await flow.CommitBindingsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Failed));
            Assert.That(flow.ResumeStage, Is.EqualTo(AppInstallStage.ConfirmBindings));
            Assert.That(flow.Error, Does.StartWith("Could not change the role bindings of Task board."));
            Assert.That(flow.IsRebinding, Is.True);
        });

        flow.Back();
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.ConfirmBindings));
    }

    [Test]
    public async Task Without_a_rebinding_facade_committing_fails_closed()
    {
        _services.Dispose();
        _services = Services(withRebinding: false);
        var flow = await InstalledAsync();
        flow.BeginRebind();
        flow.ConfirmBindings();

        await flow.CommitBindingsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Failed));
            Assert.That(flow.Error, Does.Contain("does not serve role re-binding"));
            Assert.That(_control.RoleBindingUpdates, Is.Empty);
        });
    }

    [Test]
    public async Task Only_the_installed_versions_review_can_start_rebinding()
    {
        var app = AppsTestData.TaskBoard();
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        var notInstalled = Flow();
        await notInstalled.LoadAsync();

        Assert.That(notInstalled.CanRebind, Is.False);
        Assert.That(notInstalled.BeginRebind, Throws.InvalidOperationException);

        var installed = await InstalledAsync();
        installed.BeginRebind();
        Assert.That(installed.BeginRebind, Throws.InvalidOperationException, "only from the review");
    }

    [Test]
    public async Task An_app_that_declares_no_role_has_nothing_to_rebind()
    {
        var app = AppsTestData.TaskBoard() with { Roles = [] };
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        _control.Install(app, AppLifecycleState.Enabled);
        var flow = Flow();
        await flow.LoadAsync();

        Assert.That(flow.CanRebind, Is.False);
    }

    [Test]
    public void A_binding_change_reports_whether_it_changes_anything()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new AppRoleBindingChange("viewer", "g", "g").IsChanged, Is.False);
            Assert.That(new AppRoleBindingChange("viewer", null, null).IsChanged, Is.False);
            Assert.That(new AppRoleBindingChange("viewer", "g", "h").IsChanged, Is.True);
            Assert.That(new AppRoleBindingChange("viewer", null, "h").IsChanged, Is.True);
            Assert.That(new AppRoleBindingChange("viewer", "g", null).IsChanged, Is.True);
        });
    }

    private ServiceProvider Services(bool withRebinding)
    {
        var services = new ServiceCollection()
            .AddKeyedSingleton<ILatticeAppCatalog>(ShellFacades.Key, _catalog)
            .AddKeyedSingleton<ILatticeAppsControl>(ShellFacades.Key, _control);
        if (withRebinding)
        {
            services.AddKeyedSingleton<ILatticeAppRoleBindings>(ShellFacades.Key, _control);
        }

        return services.BuildServiceProvider();
    }

    private AppInstallFlow Flow() => new(new AppsFacades(_services), new AppInstallFlowKey(null, "in-image", "task-board", null), AppsTestData.InImage);

    private async Task<AppInstallFlow> InstalledAsync()
    {
        var app = AppsTestData.TaskBoard();
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        _control.Install(app with
        {
            RoleBindings =
            [
                new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = "g-viewers" },
                new AppRoleBindingDescriptor { RoleName = "editor", GroupId = "g-editors" },
            ],
        }, AppLifecycleState.Enabled);
        var flow = Flow();
        await flow.LoadAsync();
        Assert.That(flow.Mode, Is.EqualTo(AppInstallMode.Reconsent));
        return flow;
    }
}
