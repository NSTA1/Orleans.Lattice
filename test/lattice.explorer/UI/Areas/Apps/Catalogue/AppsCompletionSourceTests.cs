using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// The Apps area's address completions: <c>a/{slug}</c> and <c>a/{slug}/open</c>
/// for apps the caller can see, and <c>app:{slug}</c> catalogue entries only for an
/// <c>AppInstall</c> holder.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppsCompletionSourceTests : AppsTestContext
{
    [Test]
    public async Task After_a_slash_it_completes_the_callers_apps_and_their_ui()
    {
        Workspace.Apps.Add(AppsTestData.Mine("crm", hasUi: true, name: "CRM"));
        Workspace.Apps.Add(AppsTestData.Mine("notes", hasUi: false));

        var all = await CompleteAsync("", AddressQueryMode.App);
        var open = await CompleteAsync("crm/o", AddressQueryMode.App);
        var byName = await CompleteAsync("CR", AddressQueryMode.App);

        Assert.Multiple(() =>
        {
            Assert.That(all.Select(completion => completion.Label), Is.EqualTo(new[] { "a/crm", "a/crm/open", "a/notes" }));
            Assert.That(all[0].Target.Format(), Is.EqualTo("/apps/crm"));
            Assert.That(all[0].Detail, Is.EqualTo("CRM"));
            Assert.That(all[1].Target.Format(), Is.EqualTo("/apps/crm/open"));
            Assert.That(open.Select(completion => completion.Label), Is.EqualTo(new[] { "a/crm/open" }));
            Assert.That(byName.Select(completion => completion.Label), Is.EqualTo(new[] { "a/crm", "a/crm/open" }));
        });
    }

    [Test]
    public async Task An_app_install_holder_also_completes_installed_apps_that_grant_them_no_role()
    {
        Control.Install(AppsTestData.TaskBoard(), Api.Apps.AppLifecycleState.Disabled);

        var completions = await CompleteAsync("task", AddressQueryMode.App);

        Assert.Multiple(() =>
        {
            Assert.That(completions.Select(completion => completion.Label), Is.EqualTo(new[] { "a/task-board" }));
            Assert.That(completions[0].Detail, Is.EqualTo("Disabled"));
        });
    }

    [Test]
    public async Task A_free_search_completes_catalogue_entries_one_per_source_for_an_app_install_holder()
    {
        Catalog.Offers.Add(AppsTestData.Offer("quality-gates", "nuget-contoso", name: "Quality gates"));
        Catalog.Offers.Add(AppsTestData.Offer("quality-gates", "blob-ops", name: "Quality gates"));
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image"));

        var completions = await CompleteAsync("quality", AddressQueryMode.Search);
        var prefixed = await CompleteAsync("app:crm", AddressQueryMode.Search);

        Assert.Multiple(() =>
        {
            Assert.That(completions.Select(completion => (completion.Label, completion.Detail)), Is.EqualTo(new[]
            {
                ("app:quality-gates", "Quality gates - blob-ops"),
                ("app:quality-gates", "Quality gates - nuget-contoso"),
            }));
            Assert.That(completions[0].Target.Format(), Is.EqualTo("/apps/catalogue/blob-ops/quality-gates"));
            Assert.That(prefixed.Select(completion => completion.Label), Is.EqualTo(new[] { "app:crm" }));
        });
    }

    [Test]
    public async Task A_restricted_identity_never_completes_the_catalogue()
    {
        Restrict();
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image"));
        Workspace.Apps.Add(AppsTestData.Mine("crm"));

        var search = await CompleteAsync("crm", AddressQueryMode.Search);
        var prefixed = await CompleteAsync("app:crm", AddressQueryMode.Search);

        Assert.Multiple(() =>
        {
            Assert.That(search.Select(completion => completion.Label), Is.EqualTo(new[] { "a/crm", "a/crm/open" }));
            Assert.That(prefixed, Is.Empty);
        });
    }

    [Test]
    public async Task With_tenancy_on_every_target_is_rooted_at_the_active_tenant()
    {
        UseTenancy("acme");
        Workspace.Apps.Add(AppsTestData.Mine("crm"));
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image"));

        var completions = await CompleteAsync("crm", AddressQueryMode.Search);

        Assert.That(completions.Select(completion => completion.Target.Tenant), Is.All.EqualTo("acme"));
    }

    [Test]
    public async Task Other_modes_and_the_result_limit_are_respected()
    {
        for (var i = 0; i < 30; i++)
        {
            Workspace.Apps.Add(AppsTestData.Mine($"app-{i:D2}", hasUi: false));
        }

        var address = await CompleteAsync("/apps", AddressQueryMode.Address);
        var many = await CompleteAsync("app", AddressQueryMode.App);

        Assert.Multiple(() =>
        {
            Assert.That(address, Is.Empty);
            Assert.That(many, Has.Count.EqualTo(AddressQuery.MaximumResults));
        });
    }

    private async Task<IReadOnlyList<AddressCompletion>> CompleteAsync(string text, AddressQueryMode mode) =>
        await Services.GetRequiredService<AppsCompletionSource>().CompleteAsync(new AddressQuery(text, mode, ExplorerAddress.Home), default);
}
