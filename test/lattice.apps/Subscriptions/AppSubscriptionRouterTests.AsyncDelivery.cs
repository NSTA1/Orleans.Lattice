using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using static Orleans.Lattice.Apps.Tests.SubscriptionTestData;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// The <see cref="AppSubscriptionRouter"/> arms that only an asynchronous or
/// failing collaborator reaches: the slow delivery path that resumes the remaining
/// routes after an incomplete handler task, the background rebuild loop's fault
/// handler, and the activation fault handler that turns a source failure into a
/// recorded table entry rather than an escaping exception.
/// </summary>
/// <remarks>
/// Delivery is sequential by design, so one handler that returns an incomplete task
/// suspends the whole fan-out. Everything after that suspension point runs in a
/// different method from the synchronous fast path, and a suite whose handlers all
/// complete synchronously never enters it - which is how the resumed loop's own
/// filtering and fault isolation can be entirely untested while delivery looks
/// thoroughly covered.
/// </remarks>
public sealed partial class AppSubscriptionRouterTests
{
    [Test]
    public async Task A_handler_that_completes_asynchronously_does_not_stop_later_deliveries()
    {
        // The resumed loop must deliver to every remaining matching route. If it
        // returned after awaiting the pending one, a single slow handler would
        // silently starve every subscription ordered after it.
        _source.Add(Manifest(Subscription("first", "docs"), Subscription("second", "docs")));
        _projection.Publish(Record(Notes));
        var gate = new TaskCompletionSource();
        var slow = Handle(Notes, "first");
        slow.Result = () => gate.Task;
        var fast = Handle(Notes, "second");
        var router = await CreateWarmRouterAsync();

        var delivery = router.OnMutationAsync(Set("a/notes/docs", "k1"), CancellationToken.None);
        Assert.That(delivery.IsCompleted, Is.False, "the pending handler must suspend the fan-out");

        gate.SetResult();
        await delivery;

        Assert.Multiple(() =>
        {
            Assert.That(slow.Deliveries, Has.Count.EqualTo(1));
            Assert.That(fast.Deliveries, Has.Count.EqualTo(1),
                "a route after the suspension point must still be delivered to");
        });
    }

    [Test]
    public async Task A_handler_that_faults_asynchronously_does_not_stop_later_deliveries()
    {
        // The pending task's fault is isolated the same way a synchronous throw is:
        // one app's broken handler must not deny every other app its change feed.
        _source.Add(Manifest(Subscription("first", "docs"), Subscription("second", "docs")));
        _projection.Publish(Record(Notes));
        var gate = new TaskCompletionSource();
        var faulting = Handle(Notes, "first");
        faulting.Result = () => gate.Task;
        var later = Handle(Notes, "second");
        var router = await CreateWarmRouterAsync();

        var delivery = router.OnMutationAsync(Set("a/notes/docs", "k1"), CancellationToken.None);
        gate.SetException(new InvalidOperationException("handler exploded"));
        await delivery;

        Assert.That(later.Deliveries, Has.Count.EqualTo(1),
            "a faulted pending delivery must be logged and stepped over, not rethrown");
    }

    [Test]
    public async Task The_resumed_delivery_loop_skips_routes_the_mutation_does_not_match()
    {
        // The resumed loop re-applies the same key-prefix filter as the fast path.
        // Were it to skip that filter, a suspended fan-out would deliver a mutation
        // to subscriptions whose prefix excludes it - a cross-subscription leak that
        // only ever appears once a handler goes asynchronous. Routes are grouped by
        // tree, so the non-matching route has to be on the SAME tree with a
        // different prefix; one on another tree is never in the same array and
        // cannot exercise this filter at all.
        _source.Add(Manifest(
            Subscription("first", "docs", keyPrefix: "k"),
            Subscription("elsewhere", "docs", keyPrefix: "zzz"),
            Subscription("last", "docs", keyPrefix: "k")));
        _projection.Publish(Record(Notes));
        var gate = new TaskCompletionSource();
        var first = Handle(Notes, "first");
        first.Result = () => gate.Task;
        var elsewhere = Handle(Notes, "elsewhere");
        var last = Handle(Notes, "last");
        var router = await CreateWarmRouterAsync();

        var delivery = router.OnMutationAsync(Set("a/notes/docs", "k1"), CancellationToken.None);
        Assert.That(delivery.IsCompleted, Is.False, "the pending handler must suspend the fan-out");
        gate.SetResult();
        await delivery;

        Assert.Multiple(() =>
        {
            Assert.That(first.Deliveries, Has.Count.EqualTo(1));
            Assert.That(last.Deliveries, Has.Count.EqualTo(1),
                "a matching route after the suspension point must still be delivered to");
            Assert.That(elsewhere.Deliveries, Is.Empty,
                "a route whose prefix excludes the key must not receive it just because delivery suspended");
        });
    }

    [Test]
    public async Task A_throwing_handler_after_the_suspension_point_does_not_stop_the_rest()
    {
        // Fault isolation has to hold on the resumed loop as well as the fast path.
        // The two are different code, and only the fast path's isolation is reached
        // when every handler completes synchronously.
        _source.Add(Manifest(
            Subscription("first", "docs", keyPrefix: "k"),
            Subscription("broken", "docs", keyPrefix: "k"),
            Subscription("last", "docs", keyPrefix: "k")));
        _projection.Publish(Record(Notes));
        var gate = new TaskCompletionSource();
        var first = Handle(Notes, "first");
        first.Result = () => gate.Task;
        var broken = Handle(Notes, "broken");
        broken.Throw = new InvalidOperationException("handler exploded");
        var last = Handle(Notes, "last");
        var router = await CreateWarmRouterAsync();

        var delivery = router.OnMutationAsync(Set("a/notes/docs", "k1"), CancellationToken.None);
        gate.SetResult();
        await delivery;

        Assert.Multiple(() =>
        {
            Assert.That(first.Deliveries, Has.Count.EqualTo(1));
            Assert.That(broken.Deliveries, Has.Count.EqualTo(1), "the broken handler was still invoked");
            Assert.That(last.Deliveries, Has.Count.EqualTo(1),
                "one app's broken handler must not deny every app ordered after it");
        });
    }

    [Test]
    public async Task A_delete_range_with_no_matched_keys_is_matched_against_the_range_bounds()
    {
        // A range delete need not carry its matched keys - a large or unmaterialised
        // range reports bounds only. Matching on the bounds is what keeps such a
        // delete deliverable at all; falling back to "no keys, no match" would drop
        // exactly the deletions a subscription most needs to see.
        _source.Add(Manifest(Subscription("docs-feed", "docs", keyPrefix: "k")));
        _projection.Publish(Record(Notes));
        var handler = Handle(Notes, "docs-feed");
        var router = await CreateWarmRouterAsync();

        var intersecting = new LatticeMutation
        {
            TreeId = "a/notes/docs",
            Kind = MutationKind.DeleteRange,
            Key = "j",
            EndExclusiveKey = "l",
        };
        var disjoint = new LatticeMutation
        {
            TreeId = "a/notes/docs",
            Kind = MutationKind.DeleteRange,
            Key = "x",
            EndExclusiveKey = "y",
        };

        await router.OnMutationAsync(intersecting, CancellationToken.None);
        await router.OnMutationAsync(disjoint, CancellationToken.None);

        Assert.That(handler.Deliveries.Select(d => d.Mutation.Key), Is.EqualTo(new[] { "j" }),
            "a range covering the prefix is delivered; one that cannot touch it is not");
    }

    [Test]
    public async Task A_background_rebuild_that_faults_is_logged_and_the_loop_settles()
    {
        // The background rebuild is a fire-and-forget task, so an escaping exception
        // there is unobserved: the loop would end, the routing table would stay
        // pinned to a stale epoch, and every later mutation would re-schedule a
        // rebuild that never runs. The fault must be swallowed inside the loop.
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes, revision: 1));
        var handler = Handle(Notes, "docs-feed");
        var projection = new FaultingProjection(_projection);
        var router = new AppSubscriptionRouter(
            projection,
            _source,
            new AppSubscriptionHandlerCatalog(Substitute.For<IServiceProvider>(), _registrations),
            AppRegistryTestData.CreateLedger(_owners, _ledger),
            NullLogger<AppSubscriptionRouter>.Instance);
        await router.RefreshAsync();

        // Move the projection on so the next mutation schedules a rebuild, and make
        // that rebuild fault.
        _projection.Publish(Record(Notes, revision: 2));
        projection.Fault = new InvalidOperationException("projection unavailable");
        await router.OnMutationAsync(Set("a/notes/docs", "k1"), CancellationToken.None);
        await router.BackgroundRebuild;

        Assert.That(router.BackgroundRebuild.IsCompletedSuccessfully, Is.True,
            "the rebuild loop must absorb the fault rather than ending faulted and unobserved");

        // The loop must still be usable afterwards: once the projection recovers, a
        // rebuild brings the table back to the current epoch and delivery resumes.
        projection.Fault = null;
        await router.RefreshAsync();
        await router.OnMutationAsync(Set("a/notes/docs", "k2"), CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(router.Table.Epoch, Is.EqualTo(_projection.CurrentEpoch));
            Assert.That(handler.Deliveries.Select(d => d.Mutation.Key), Does.Contain("k2"));
        });
    }

    [Test]
    public async Task An_activation_fault_is_recorded_against_the_app_rather_than_thrown()
    {
        // Activation resolves manifests through the source, which is a separate
        // system that can be down. The router must degrade to "this app has no live
        // routes, and here is why" rather than failing the whole rebuild - one
        // broken app would otherwise take the routing table for every app with it.
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes));
        Handle(Notes, "docs-feed");
        var router = new AppSubscriptionRouter(
            _projection,
            new ThrowingAppSource(new InvalidOperationException("source unavailable")),
            new AppSubscriptionHandlerCatalog(Substitute.For<IServiceProvider>(), _registrations),
            AppRegistryTestData.CreateLedger(_owners, _ledger),
            NullLogger<AppSubscriptionRouter>.Instance);

        await router.RefreshAsync();

        Assert.That(router.Table.TryGetFailure(TenantId.Default, Notes, out var failures), Is.True,
            "a source fault must be recorded as this app's activation failure");
        Assert.That(failures!, Has.Some.Contains("source unavailable"));
    }

    [Test]
    public async Task A_rebuild_that_faults_leaves_the_previous_routing_table_in_effect()
    {
        // The background rebuild loop is a fire-and-forget task: an escaping
        // exception there is unobserved, and the table it was rebuilding would be
        // left in whatever state the fault interrupted. It must instead log and
        // leave the last good table serving.
        _source.Add(Manifest(Subscription("docs-feed", "docs")));
        _projection.Publish(Record(Notes));
        var handler = Handle(Notes, "docs-feed");
        var projection = new FaultingProjection(_projection);
        var router = new AppSubscriptionRouter(
            projection,
            _source,
            new AppSubscriptionHandlerCatalog(Substitute.For<IServiceProvider>(), _registrations),
            AppRegistryTestData.CreateLedger(_owners, _ledger),
            NullLogger<AppSubscriptionRouter>.Instance);
        await router.RefreshAsync();
        var before = router.Table;

        projection.Fault = new InvalidOperationException("projection unavailable");
        Assert.That(async () => await router.RefreshAsync(), Throws.InvalidOperationException);

        Assert.That(router.Table, Is.SameAs(before),
            "a faulted rebuild must leave the previous table in effect");

        // The previous table must still actually deliver - an intact reference to a
        // table that no longer routes would be the same outage in a different shape.
        projection.Fault = null;
        await router.OnMutationAsync(Set("a/notes/docs", "k1"), CancellationToken.None);
        Assert.That(handler.Deliveries, Has.Count.EqualTo(1));
    }

    /// <summary>An <see cref="IAppSource"/> whose resolution always throws.</summary>
    private sealed class ThrowingAppSource(Exception fault) : IAppSource
    {
        public ValueTask<AppSourceResult> ResolveAsync(
            AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default) =>
            throw fault;
    }

    /// <summary>
    /// A projection whose warm-up can be made to fault on demand. The fault is
    /// confined to <see cref="EnsureWarmAsync"/> because <see cref="Current"/> is
    /// read on the delivery path outside any handler, where a throw would surface
    /// from the mutation call itself rather than from the background rebuild.
    /// </summary>
    private sealed class FaultingProjection(FakeAppRegistryProjection inner) : IAppRegistryProjection
    {
        public Exception? Fault { get; set; }

        public long CurrentEpoch => inner.CurrentEpoch;

        public CompiledAppRegistrySnapshot Current => inner.Current;

        public Task EnsureWarmAsync(CancellationToken cancellationToken = default) =>
            Fault is { } ex ? Task.FromException(ex) : inner.EnsureWarmAsync(cancellationToken);
    }
}
