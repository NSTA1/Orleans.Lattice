using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using static Orleans.Lattice.Apps.Tests.SubscriptionTestData;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed partial class AppSubscriptionRouterTests
{
    private FakeAppRegistryProjection _projection = null!;
    private FakeAppSource _source = null!;
    private List<AppSubscriptionHandlerRegistration> _registrations = null!;
    private InMemoryAppRegistryStore _owners = null!;
    private InMemoryAppTreeLedgerStore _ledger = null!;

    [SetUp]
    public void SetUp()
    {
        _projection = new FakeAppRegistryProjection();
        _source = new FakeAppSource();
        _registrations = [];
        _owners = new InMemoryAppRegistryStore();
        _ledger = new InMemoryAppTreeLedgerStore();
    }

    private RecordingChangeFeedHandler Handle(AppSlug app, string subscription)
    {
        var handler = new RecordingChangeFeedHandler();
        _registrations.Add(new(app, subscription, _ => handler));
        return handler;
    }

    private AppSubscriptionRouter CreateRouter() =>
        new(
            _projection,
            _source,
            new AppSubscriptionHandlerCatalog(Substitute.For<IServiceProvider>(), _registrations),
            AppRegistryTestData.CreateLedger(_owners, _ledger),
            NullLogger<AppSubscriptionRouter>.Instance);

    private async Task<AppSubscriptionRouter> CreateWarmRouterAsync()
    {
        var router = CreateRouter();
        await router.RefreshAsync();
        return router;
    }

    [Test]
    public async Task Own_tree_subscription_is_delivered_without_an_exception_entry()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes));
        var handler = Handle(Notes, "docs-feed");
        var router = await CreateWarmRouterAsync();

        await router.OnMutationAsync(Set("a/notes/docs", "k1"), CancellationToken.None);
        await router.OnMutationAsync(Set("a/notes/audit", "k2"), CancellationToken.None);

        var delivery = handler.Deliveries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(delivery.Mutation.Key, Is.EqualTo("k1"));
            Assert.That(delivery.Subscription.Name, Is.EqualTo("docs-feed"));
            Assert.That(delivery.Subscription.IsCrossApp, Is.False);
            Assert.That(router.Table.TryGetFailure(TenantId.Default, Notes, out _), Is.False);
        });
    }

    [Test]
    public async Task Cross_app_subscription_is_delivered_when_the_ceiling_approves_it()
    {
        InstallOwner(Billing, "invoices");
        _source.Add(Manifest(Subscription("invoices", "invoices", Billing)));
        _projection.Publish(Record(Notes, ceiling: Ceiling(TreeException("a/billing/invoices"))));
        var handler = Handle(Notes, "invoices");
        var router = await CreateWarmRouterAsync();

        await router.OnMutationAsync(Set("a/billing/invoices", "inv-1"), CancellationToken.None);

        var delivery = handler.Deliveries.Single();
        Assert.That(delivery.Subscription.ObservedApp, Is.EqualTo(Billing));
        Assert.That(delivery.Subscription.IsCrossApp, Is.True);
    }

    [Test]
    public async Task Cross_app_subscription_is_not_activated_while_the_observed_app_is_not_an_installed_owner()
    {
        _source.Add(Manifest(Subscription("invoices", "invoices", Billing)));
        _projection.Publish(Record(Notes, ceiling: Ceiling(TreeException("a/billing/invoices"))));
        var handler = Handle(Notes, "invoices");
        var router = await CreateWarmRouterAsync();

        await router.OnMutationAsync(Set("a/billing/invoices", "inv-1"), CancellationToken.None);

        Assert.That(handler.Deliveries, Is.Empty);
        Assert.That(router.Table.TryGetFailure(TenantId.Default, Notes, out var reasons), Is.True);
        Assert.That(string.Join(" ", reasons!), Does.Contain("not installed as the owner"));
    }

    [Test]
    public async Task Uninstalling_the_observed_owner_withdraws_the_cross_app_subscription_on_the_next_rebuild()
    {
        InstallOwner(Billing, "invoices");
        _source.Add(Manifest(Subscription("invoices", "invoices", Billing)));
        var notes = Record(Notes, ceiling: Ceiling(TreeException("a/billing/invoices")));
        _projection.Publish(notes);
        var handler = Handle(Notes, "invoices");
        var router = await CreateWarmRouterAsync();

        _owners.Seed(AppRegistryTreeNames.ComposeKey(TenantId.Default, Billing),
            AppRegistryTestData.Record(AppRegistryLifecycleState.Uninstalled, slug: Billing));
        _projection.Publish(notes with { Revision = notes.Revision + 1 });
        await router.RefreshAsync();
        await router.OnMutationAsync(Set("a/billing/invoices", "inv-1"), CancellationToken.None);

        Assert.That(handler.Deliveries, Is.Empty);
    }

    private void InstallOwner(AppSlug app, string tree)
    {
        _owners.Seed(AppRegistryTreeNames.ComposeKey(TenantId.Default, app),
            AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, slug: app));
        _ledger.Seed(AppActivationTreeNames.StructuralTree(TenantId.Default, app, tree), new AppTreeClaim
        {
            Tenant = TenantId.Default,
            Slug = app,
            Publisher = new AppProvenance().Publisher,
            Kind = AppTreeClaimKind.Structural,
        });
    }

    [Test]
    public async Task Cross_app_subscription_without_approval_fails_the_apps_subscription_activation()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs"), Subscription("invoices", "invoices", Billing)));
        _projection.Publish(Record(Notes));
        var docs = Handle(Notes, "docs-feed");
        var invoices = Handle(Notes, "invoices");
        var router = await CreateWarmRouterAsync();

        await router.OnMutationAsync(Set("a/billing/invoices", "inv-1"), CancellationToken.None);
        await router.OnMutationAsync(Set("a/notes/docs", "k1"), CancellationToken.None);

        Assert.That(invoices.Deliveries, Is.Empty);
        Assert.That(docs.Deliveries, Is.Empty, "a denial activates none of the app's subscriptions");
        Assert.That(router.Table.TryGetFailure(TenantId.Default, Notes, out var reasons), Is.True);
        Assert.That(reasons!.Single(), Does.Contain("'billing'"));
        Assert.That(router.Table.TreeCount, Is.Zero);
    }

    [Test]
    public async Task Disable_stops_delivery_immediately_and_the_rebuild_tears_the_routes_down()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes, revision: 1));
        var handler = Handle(Notes, "docs-feed");
        var router = await CreateWarmRouterAsync();
        await router.OnMutationAsync(Set("a/notes/docs", "before"), CancellationToken.None);

        _projection.Publish(Record(Notes, AppRegistryLifecycleState.Disabled, revision: 2));
        await router.OnMutationAsync(Set("a/notes/docs", "racing-the-rebuild"), CancellationToken.None);
        await router.BackgroundRebuild;
        await router.OnMutationAsync(Set("a/notes/docs", "after"), CancellationToken.None);

        Assert.That(handler.Deliveries.Select(d => d.Mutation.Key), Is.EqualTo(new[] { "before" }));
        Assert.That(router.Table.Epoch, Is.EqualTo(_projection.CurrentEpoch));
        Assert.That(router.Table.TryGetRoutes("a/notes/docs", out _), Is.False);
    }

    [Test]
    public async Task Uninstall_tears_the_routes_down()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes));
        var handler = Handle(Notes, "docs-feed");
        var router = await CreateWarmRouterAsync();

        _projection.Publish(Record(Notes, AppRegistryLifecycleState.Uninstalled, revision: 2));
        await router.OnMutationAsync(Set("a/notes/docs", "k"), CancellationToken.None);
        await router.BackgroundRebuild;

        Assert.That(handler.Deliveries, Is.Empty);
        Assert.That(router.Table.TreeCount, Is.Zero);
    }

    [Test]
    public async Task Re_enable_resumes_delivery_once_the_table_is_rebuilt()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes, AppRegistryLifecycleState.Disabled, revision: 2));
        var handler = Handle(Notes, "docs-feed");
        var router = await CreateWarmRouterAsync();

        _projection.Publish(Record(Notes, revision: 3));
        await router.RefreshAsync();
        await router.OnMutationAsync(Set("a/notes/docs", "k"), CancellationToken.None);

        Assert.That(handler.Deliveries.Single().Mutation.Key, Is.EqualTo("k"));
    }

    [Test]
    public async Task Cold_router_schedules_a_rebuild_on_the_first_mutation()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes));
        var handler = Handle(Notes, "docs-feed");
        var router = CreateRouter();

        await router.OnMutationAsync(Set("a/notes/docs", "cold"), CancellationToken.None);
        await router.BackgroundRebuild;
        await router.OnMutationAsync(Set("a/notes/docs", "warm"), CancellationToken.None);

        Assert.That(handler.Deliveries.Select(d => d.Mutation.Key), Is.EqualTo(new[] { "warm" }));
        Assert.That(_projection.EnsureWarmCalls, Is.GreaterThan(0));
        Assert.That(router.Table.Epoch, Is.EqualTo(_projection.CurrentEpoch));
    }

    [Test]
    public async Task Tenant_install_observes_only_its_tenant_composed_tree()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes, tenant: Acme));
        var handler = Handle(Notes, "docs-feed");
        var router = await CreateWarmRouterAsync();

        await router.OnMutationAsync(Set("a/notes/docs", "default-tenant"), CancellationToken.None);
        await router.OnMutationAsync(Set("t/acme/a/notes/docs", "acme"), CancellationToken.None);

        var delivery = handler.Deliveries.Single();
        Assert.That(delivery.Mutation.Key, Is.EqualTo("acme"));
        Assert.That(delivery.Subscription.Tenant, Is.EqualTo(Acme));
    }

    [Test]
    public async Task Installed_but_not_enabled_apps_are_not_activated()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes, AppRegistryLifecycleState.Installed));
        Handle(Notes, "docs-feed");
        var router = await CreateWarmRouterAsync();

        Assert.That(router.Table.TreeCount, Is.Zero);
        Assert.That(_source.ResolveCalls, Is.Zero);
        Assert.That(router.Table.TryGetFailure(TenantId.Default, Notes, out _), Is.False);
    }
}
