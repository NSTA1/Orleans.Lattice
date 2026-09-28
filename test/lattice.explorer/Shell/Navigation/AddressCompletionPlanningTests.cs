using NSubstitute;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;
using Orleans.Lattice.Explorer.Shell.Navigation.Completion;

namespace Orleans.Lattice.Explorer.Tests.Shell.Navigation;

/// <summary>
/// Reading typed input into a mode, choosing the sources that answer it, and the
/// chrome's own sources: areas by name, and tenants.
/// </summary>
[TestFixture]
public sealed class AddressCompletionPlanningTests
{
    [Test]
    [TestCase(null, "Search", "")]
    [TestCase("  orders ", "Search", "orders")]
    [TestCase(">", "Command", "")]
    [TestCase("> theme", "Command", "theme")]
    [TestCase("t/ac", "Tenant", "ac")]
    [TestCase("T/ac", "Tenant", "ac")]
    [TestCase("a/crm", "App", "crm")]
    [TestCase("/data/a/crm ", "Address", "/data/a/crm")]
    [TestCase("data/orders", "Search", "data/orders")]
    public void Input_is_read_by_its_prefix(string? raw, string mode, string text)
    {
        var input = AddressInput.Read(raw);

        Assert.Multiple(() =>
        {
            Assert.That(input.Mode, Is.EqualTo(Enum.Parse<AddressQueryMode>(mode)));
            Assert.That(input.Text, Is.EqualTo(text));
        });
    }

    [Test]
    public void A_search_asks_the_area_names_and_every_visible_areas_source()
    {
        var entries = Entries();
        var tenants = FakeCompletionSource.Answering();

        var sources = AddressCompletionPlanner.SourcesFor(AddressQueryMode.Search, entries, tenants);

        Assert.That(sources.Select(source => source.Key), Is.EqualTo(new[] { AddressCompletionPlanner.AreasSourceKey, "data" }),
            "an unavailable area and an area without a source are not asked");
    }

    [Test]
    [TestCase("App")]
    [TestCase("Address")]
    public void An_app_or_address_asks_every_visible_areas_source_only(string mode)
    {
        var sources = AddressCompletionPlanner.SourcesFor(Enum.Parse<AddressQueryMode>(mode), Entries(), FakeCompletionSource.Answering());

        Assert.That(sources.Select(source => source.Key), Is.EqualTo(new[] { "data" }));
    }

    [Test]
    public void A_tenant_asks_the_tenant_source_and_a_command_asks_none()
    {
        var tenants = FakeCompletionSource.Answering();

        Assert.Multiple(() =>
        {
            Assert.That(AddressCompletionPlanner.SourcesFor(AddressQueryMode.Tenant, Entries(), tenants).Single().Source, Is.SameAs(tenants));
            Assert.That(AddressCompletionPlanner.SourcesFor(AddressQueryMode.Command, Entries(), tenants), Is.Empty);
            Assert.That(() => AddressCompletionPlanner.SourcesFor(AddressQueryMode.Search, null!, tenants), Throws.ArgumentNullException);
            Assert.That(() => AddressCompletionPlanner.SourcesFor(AddressQueryMode.Search, [], null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_area_source_matches_visible_areas_by_key_or_name_under_the_current_tenant()
    {
        var source = new AreaCompletionSource(Entries());

        var matches = await source.CompleteAsync(
            new AddressQuery("DAT", AddressQueryMode.Search, ExplorerAddress.Home.WithTenant("acme")),
            CancellationToken.None);
        var byName = await source.CompleteAsync(new AddressQuery("back", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(matches.Single().Label, Is.EqualTo("data"));
            Assert.That(matches.Single().Detail, Is.EqualTo("Data"));
            Assert.That(matches.Single().Target.Format(), Is.EqualTo("/t/acme/data"));
            Assert.That(byName, Is.Empty, "an unavailable area is never offered");
            Assert.That(() => new AreaCompletionSource(null!), Throws.ArgumentNullException);
            Assert.That(async () => await source.CompleteAsync(null!, CancellationToken.None), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_tenant_source_offers_reachable_tenants_re_rooting_the_current_address()
    {
        var (tenancy, navigator) = Tenancy(active: "acme", "acme", "globex", "initech");
        var source = new TenantCompletionSource(tenancy, navigator);
        var current = ExplorerAddress.Parse("/t/acme/data/orders");

        var matches = await source.CompleteAsync(new AddressQuery("e", AddressQueryMode.Tenant, current), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(matches.Select(match => match.Label), Is.EqualTo(new[] { "t/acme", "t/globex", "t/initech" }));
            Assert.That(matches[1].Target.Format(), Is.EqualTo("/t/globex/data/orders"));
            Assert.That(matches[0].Detail, Is.EqualTo("Active tenant"));
            Assert.That(matches[1].Detail, Is.EqualTo("Switch to this tenant"));
        });
    }

    [Test]
    public async Task The_tenant_source_offers_nothing_when_tenancy_is_off()
    {
        var directory = new ExplorerAreaDirectory([], new ExplorerChromeOptions(), new ManualTimeProvider());
        var tenancy = new ExplorerTenancy();
        var source = new TenantCompletionSource(tenancy, new ExplorerNavigator(new TestNavigationManager(), directory, tenancy));

        Assert.Multiple(async () =>
        {
            Assert.That(await source.CompleteAsync(new AddressQuery(string.Empty, AddressQueryMode.Tenant, ExplorerAddress.Home), CancellationToken.None), Is.Empty);
            Assert.That(() => new TenantCompletionSource(null!, null!), Throws.ArgumentNullException);
        });
    }

    private static IReadOnlyList<ExplorerAreaEntry> Entries() =>
    [
        new(new FakeArea("data", "Data") { Completions = FakeCompletionSource.Answering() }, AreaAvailability.Visible),
        new(new FakeArea("apps", "Apps"), AreaAvailability.Visible),
        new(new FakeArea("backups", "Backups") { Completions = FakeCompletionSource.Answering() }, AreaAvailability.Unavailable("No grant.")),
    ];

    private static (ExplorerTenancy Tenancy, ExplorerNavigator Navigator) Tenancy(string active, params string[] accessible)
    {
        var view = Substitute.For<IExplorerTenantView>();
        view.IsActive.Returns(true);
        view.ActiveTenant.Returns(new ExplorerTenantId(active));
        var source = Substitute.For<IExplorerAccessibleTenantSource>();
        source.GetAccessibleTenantsAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<IReadOnlyList<ExplorerTenantId>>([.. accessible.Select(tenant => new ExplorerTenantId(tenant))]));

        var tenancy = new ExplorerTenancy(view, null, source);
        var directory = new ExplorerAreaDirectory([], new ExplorerChromeOptions(), new ManualTimeProvider());
        return (tenancy, new ExplorerNavigator(new TestNavigationManager(), directory, tenancy));
    }
}
