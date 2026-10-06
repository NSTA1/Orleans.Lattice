using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// The Apps area's spine stop: its fail-closed availability, Home status and
/// badge, its palette commands, and the memoized per-circuit probe behind them.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppsAreaTests : AppsTestContext
{
    [Test]
    public void The_shell_registers_exactly_one_apps_area_at_its_spine_position()
    {
        var areas = Services.GetServices<IExplorerArea>().Where(area => area.Key == "apps").ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(areas, Has.Length.EqualTo(1));
            Assert.That(areas[0], Is.InstanceOf<AppsArea>());
            Assert.That(areas[0].DisplayName, Is.EqualTo("Apps"));
            Assert.That(areas[0].DirectoryOrder, Is.EqualTo(20));
            Assert.That(areas[0].IsTenantScoped, Is.True, "installs are per tenant (E9)");
            Assert.That(areas[0].Completions, Is.InstanceOf<AppsCompletionSource>());
        });
    }

    [Test]
    public void Only_an_apps_window_renders_standalone()
    {
        IExplorerArea area = Area();

        Assert.Multiple(() =>
        {
            Assert.That(area.IsStandaloneAt(AppsRoutes.Window(null, "crm")), Is.True);
            Assert.That(area.IsStandaloneAt(AppsRoutes.Window("acme", "crm")), Is.True);
            Assert.That(area.IsStandaloneAt(ExplorerAddress.Parse("/apps/crm/open")), Is.False);
            Assert.That(area.IsStandaloneAt(AppsRoutes.App(null, "crm")), Is.False);
            Assert.That(area.IsStandaloneAt(AppsRoutes.Landing(null)), Is.False);
        });
    }

    [Test]
    public async Task Every_signed_in_user_sees_the_area_even_with_no_app()
    {
        Restrict();
        var area = Area();

        var v0 = await area.GetAvailabilityAsync(default);
        var v1 = await area.GetDirectoryBadgeAsync(default);
        var v2 = await area.GetHomeStatusAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(v0, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(v1, Is.Null);
            Assert.That(v2, Is.EqualTo("No app is assigned to you yet"));
            Assert.That(area.Commands, Is.Empty, "a restricted identity is offered no lifecycle command");
        });
    }

    [Test]
    public async Task A_role_holder_sees_how_many_apps_are_theirs()
    {
        Restrict();
        Workspace.Apps.Add(AppsTestData.Mine("crm"));
        Workspace.Apps.Add(AppsTestData.Mine("notes"));
        var area = Area();

        var v0 = await area.GetDirectoryBadgeAsync(default);
        var v1 = await area.GetHomeStatusAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(v0, Is.EqualTo("2"));
            Assert.That(v1, Is.EqualTo("2 apps available to you"));
        });
    }

    [Test]
    public async Task A_caller_who_is_not_signed_in_and_holds_no_app_install_is_hidden()
    {
        Restrict();
        Workspace.Failure = new UnauthorizedAccessException();

        var v0 = await Area().GetAvailabilityAsync(default);
        var v1 = await Area().GetHomeStatusAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(v0, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(v1, Is.Null);
        });
    }

    [Test]
    public async Task A_probe_that_throws_is_a_denial_never_a_grant()
    {
        Catalog.CapabilitiesFailure = new TimeoutException();
        Control.Capabilities = new LatticeAppsCapabilities();
        Workspace.Failure = new TimeoutException();

        var snapshot = await Services.GetRequiredService<AppsAccess>().GetAsync();

        var v0 = await Area().GetAvailabilityAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.CanBrowseCatalogue, Is.False);
            Assert.That(snapshot.WorkspaceServed, Is.False);
            Assert.That(v0, Is.EqualTo(AreaAvailability.Hidden));
        });
    }

    [Test]
    public async Task A_head_serving_no_app_facade_hides_the_area()
    {
        Services.RemoveAllKeyed<ILatticeAppCatalog>(ShellFacades.Key);
        Services.RemoveAllKeyed<ILatticeAppsControl>(ShellFacades.Key);
        Services.RemoveAllKeyed<ILatticeAppWorkspace>(ShellFacades.Key);

        Assert.That(await Area().GetAvailabilityAsync(default), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task An_app_install_holder_sees_installs_failures_and_updates()
    {
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Enabled);
        Control.Install(AppsTestData.TaskBoard() with { Slug = "crm" }, AppLifecycleState.Failed);
        Catalog.Offers.Add(AppsTestData.Offer("task-board", "in-image", "2.0.0", "1.0.0", AppLifecycleState.Enabled));

        var area = Area();

        var v0 = await area.GetAvailabilityAsync(default);
        var v1 = await area.GetDirectoryBadgeAsync(default);
        var v2 = await area.GetHomeStatusAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(v0, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(v1, Is.EqualTo("2"));
            Assert.That(v2, Is.EqualTo("2 apps installed, 1 failed activation, 1 update available"));
        });
    }

    [Test]
    public async Task The_probe_is_memoized_per_circuit_until_invalidated()
    {
        var access = Services.GetRequiredService<AppsAccess>();
        var changed = 0;
        access.Changed += () => changed++;

        Assert.That(access.Current, Is.Null, "nothing is known before the first probe");
        await Area().GetAvailabilityAsync(default);
        await Area().GetDirectoryBadgeAsync(default);
        await access.GetAsync();
        Assert.That(Workspace.ListCalls, Is.EqualTo(1));

        access.Invalidate();
        await access.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(Workspace.ListCalls, Is.EqualTo(2));
            Assert.That(changed, Is.EqualTo(1));
            Assert.That(access.Current, Is.Not.Null);
        });
    }

    [Test]
    public void A_waiter_that_gives_up_does_not_cancel_the_probe()
    {
        var access = Services.GetRequiredService<AppsAccess>();
        Workspace.ListGate = new TaskCompletionSource();
        using var cancelled = new CancellationTokenSource();
        var waiting = access.GetAsync(cancelled.Token);
        cancelled.Cancel();
        Workspace.ListGate.SetResult();

        Assert.Multiple(() =>
        {
            Assert.CatchAsync<OperationCanceledException>(() => waiting);
            Assert.DoesNotThrowAsync(() => access.GetAsync());
        });
    }

    [Test]
    public async Task An_app_install_holder_is_offered_install_upgrade_and_disable_commands()
    {
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Enabled);
        Control.Install(AppsTestData.TaskBoard() with { Slug = "crm" }, AppLifecycleState.Disabled);
        Catalog.Offers.Add(AppsTestData.Offer("task-board", "in-image", "2.0.0", "1.0.0", AppLifecycleState.Enabled));
        var area = Area();
        await area.GetAvailabilityAsync(default);

        var commands = area.Commands.ToDictionary(command => command.Id);

        Assert.Multiple(() =>
        {
            Assert.That(commands.Keys, Is.EquivalentTo(new[] { "apps.install", "apps.upgrade.task-board", "apps.disable.task-board" }));
            Assert.That(commands["apps.install"].Title, Is.EqualTo("Install app..."));
            Assert.That(commands["apps.install"].Target!.Format(), Is.EqualTo("/apps"));
            Assert.That(commands["apps.upgrade.task-board"].Title, Is.EqualTo("Upgrade task-board"));
            Assert.That(commands["apps.upgrade.task-board"].Target!.Format(), Is.EqualTo("/apps/catalogue/in-image/task-board%402.0.0"));
            Assert.That(commands["apps.disable.task-board"].Title, Is.EqualTo("Disable task-board"));
            Assert.That(commands["apps.disable.task-board"].Target!.Format(), Is.EqualTo("/apps/catalogue/in-image/task-board"));
        });
    }

    [Test]
    public async Task Invoking_a_command_navigates_or_posts_the_lifecycle_intent()
    {
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Enabled);
        Catalog.Offers.Add(AppsTestData.Offer("task-board", "in-image", "2.0.0", "1.0.0", AppLifecycleState.Enabled));
        var area = Area();
        await area.GetAvailabilityAsync(default);
        var intents = Services.GetRequiredService<AppsLifecycleIntents>();
        var commands = area.Commands.ToDictionary(command => command.Id);

        await commands["apps.install"].InvokeAsync!(default);
        await commands["apps.upgrade.task-board"].InvokeAsync!(default);
        await commands["apps.disable.task-board"].InvokeAsync!(default);

        Assert.Multiple(() =>
        {
            Assert.That(Navigation.Uri, Does.EndWith("apps/catalogue?source=all&filter=available"));
            Assert.That(intents.TryTake("task-board", AppLifecycleVerb.Upgrade), Is.False, "a later intent for the same app replaces the earlier one");
            Assert.That(intents.TryTake("task-board", AppLifecycleVerb.Disable), Is.True);
            Assert.That(intents.TryTake("task-board", AppLifecycleVerb.Disable), Is.False, "an intent is taken once");
        });
    }

    [Test]
    public async Task With_tenancy_on_every_command_target_is_rooted_at_the_active_tenant()
    {
        UseTenancy("acme");
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Enabled);
        var area = Area();
        await area.GetAvailabilityAsync(default);

        Assert.That(area.Commands.Select(command => command.Target!.Tenant), Is.All.EqualTo("acme"));
    }

    [Test]
    public async Task Commands_skip_apps_whose_slug_cannot_be_a_command_id()
    {
        Control.Install(AppsTestData.TaskBoard() with { Slug = "Odd_Slug" }, AppLifecycleState.Enabled);
        var area = Area();
        await area.GetAvailabilityAsync(default);

        Assert.That(area.Commands.Select(command => command.Id), Is.EqualTo(new[] { "apps.install" }));
    }

    private AppsArea Area() => Services.GetServices<IExplorerArea>().OfType<AppsArea>().Single();
}
