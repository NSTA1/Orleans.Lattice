using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using static Orleans.Lattice.Apps.Tests.SubscriptionTestData;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppSubscriptionRouterTests
{
    [Test]
    public async Task Missing_handler_fails_the_apps_subscription_activation()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs"), Subscription("audit-feed", "audit")));
        _projection.Publish(Record(Notes));
        var docs = Handle(Notes, "docs-feed");
        var router = await CreateWarmRouterAsync();

        await router.OnMutationAsync(Set("a/notes/docs", "k"), CancellationToken.None);

        Assert.That(docs.Deliveries, Is.Empty);
        Assert.That(router.Table.TryGetFailure(TenantId.Default, Notes, out var reasons), Is.True);
        Assert.That(reasons!.Single(), Does.Contain("audit-feed"));
    }

    [Test]
    public async Task Unpinned_ceiling_fails_activation_without_resolving_the_manifest()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, slug: Notes, version: V1, ceilingVersion: AppVersion.Parse("0.9.0")));
        Handle(Notes, "docs-feed");
        var router = await CreateWarmRouterAsync();

        Assert.That(router.Table.TryGetFailure(TenantId.Default, Notes, out var reasons), Is.True);
        Assert.That(reasons!.Single(), Does.Contain("0.9.0"));
        Assert.That(_source.ResolveCalls, Is.Zero);
    }

    [Test]
    public async Task Unresolvable_manifest_fails_that_app_only()
    {
        _source.Add(Manifest(Billing, [Tree("invoices")], Subscription("self", "invoices")));
        _projection.Publish(Record(Notes), Record(Billing));
        var billing = Handle(Billing, "self");
        var router = await CreateWarmRouterAsync();

        await router.OnMutationAsync(Set("a/billing/invoices", "k"), CancellationToken.None);

        Assert.That(router.Table.TryGetFailure(TenantId.Default, Notes, out var reasons), Is.True);
        Assert.That(reasons!.Single(), Does.Contain("notes"));
        Assert.That(billing.Deliveries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Maintenance_writes_are_not_delivered()
    {
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes));
        var handler = Handle(Notes, "docs-feed");
        var router = await CreateWarmRouterAsync();

        await router.OnMutationAsync(Set("a/notes/docs", "k") with { Category = MutationCategory.Maintenance }, CancellationToken.None);

        Assert.That(handler.Deliveries, Is.Empty);
    }

    [Test]
    public async Task Key_prefix_limits_delivery()
    {
        _source.Add(Manifest(Subscription("log-feed", "audit", keyPrefix: "log/")));
        _projection.Publish(Record(Notes));
        var handler = Handle(Notes, "log-feed");
        var router = await CreateWarmRouterAsync();

        await router.OnMutationAsync(Set("a/notes/audit", "log/1"), CancellationToken.None);
        await router.OnMutationAsync(Set("a/notes/audit", "other/1"), CancellationToken.None);

        Assert.That(handler.Deliveries.Select(d => d.Mutation.Key), Is.EqualTo(new[] { "log/1" }));
    }

    [Test]
    public async Task A_throwing_or_faulting_handler_does_not_block_the_others()
    {
        _source.Add(Manifest(Subscription("a", "docs"), Subscription("b", "docs"), Subscription("c", "docs")));
        _projection.Publish(Record(Notes));
        var throws = Handle(Notes, "a");
        throws.Throw = new InvalidOperationException("boom");
        var faults = Handle(Notes, "b");
        faults.Result = () => Task.FromException(new InvalidOperationException("faulted"));
        var healthy = Handle(Notes, "c");
        var router = await CreateWarmRouterAsync();

        await router.OnMutationAsync(Set("a/notes/docs", "k"), CancellationToken.None);

        Assert.That(throws.Deliveries, Has.Count.EqualTo(1));
        Assert.That(faults.Deliveries, Has.Count.EqualTo(1));
        Assert.That(healthy.Deliveries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task An_incomplete_handler_task_is_awaited_before_the_next_handler_runs()
    {
        _source.Add(Manifest(Subscription("a", "docs"), Subscription("b", "docs")));
        _projection.Publish(Record(Notes));
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = Handle(Notes, "a");
        first.Result = () => gate.Task;
        var second = Handle(Notes, "b");
        var router = await CreateWarmRouterAsync();

        var dispatch = router.OnMutationAsync(Set("a/notes/docs", "k"), CancellationToken.None);

        Assert.That(dispatch.IsCompleted, Is.False);
        Assert.That(second.Deliveries, Is.Empty);
        gate.SetResult();
        await dispatch;
        Assert.That(second.Deliveries, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Mutation_without_a_tree_id_is_ignored()
    {
        var router = await CreateWarmRouterAsync();

        Assert.DoesNotThrowAsync(() => router.OnMutationAsync(default, CancellationToken.None));
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var catalog = new AppSubscriptionHandlerCatalog(Substitute.For<IServiceProvider>(), []);
        var logger = NullLogger<AppSubscriptionRouter>.Instance;
        var ledger = AppRegistryTestData.CreateLedger(new InMemoryAppRegistryStore());

        Assert.Throws<ArgumentNullException>(() => new AppSubscriptionRouter(null!, _source, catalog, ledger, logger));
        Assert.Throws<ArgumentNullException>(() => new AppSubscriptionRouter(_projection, null!, catalog, ledger, logger));
        Assert.Throws<ArgumentNullException>(() => new AppSubscriptionRouter(_projection, _source, null!, ledger, logger));
        Assert.Throws<ArgumentNullException>(() => new AppSubscriptionRouter(_projection, _source, catalog, null!, logger));
        Assert.Throws<ArgumentNullException>(() => new AppSubscriptionRouter(_projection, _source, catalog, ledger, null!));
    }
}
