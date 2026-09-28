using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using static Orleans.Lattice.Apps.Tests.SubscriptionTestData;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// The dispatch path runs inline on every committed write in the silo, so a mutation on a tree no
/// subscription observes must allocate nothing. Measured differentially (two loop sizes, full-size
/// warm-up, minimum across attempts) so one-off runtime costs cancel and only a per-call allocation
/// survives. The measured method is synchronous, so the build configuration does not change it.
/// </summary>
[TestFixture]
public sealed class AppSubscriptionRouterAllocationTests
{
    private const int Small = 1_000;
    private const int Large = 11_000;
    private const int Attempts = 5;

    [Test]
    public async Task Dispatch_for_a_tree_no_subscription_observes_allocates_nothing()
    {
        var projection = new FakeAppRegistryProjection();
        var source = new FakeAppSource().Add(Manifest(Subscription("docs-feed", "docs")));
        projection.Publish(Record(Notes));
        var handler = new RecordingChangeFeedHandler();
        var router = new AppSubscriptionRouter(
            projection,
            source,
            new AppSubscriptionHandlerCatalog(Substitute.For<IServiceProvider>(), [new(Notes, "docs-feed", _ => handler)]),
            AppRegistryTestData.CreateLedger(new InMemoryAppRegistryStore()),
            NullLogger<AppSubscriptionRouter>.Instance);
        await router.RefreshAsync();
        var mutation = Set("unrelated-tree", "k");

        Run(router, mutation, Large);
        var best = long.MaxValue;
        for (var attempt = 0; attempt < Attempts; attempt++)
            best = Math.Min(best, Measure(router, mutation, Large) - Measure(router, mutation, Small));

        Assert.That(best, Is.LessThanOrEqualTo(0), $"dispatch allocated {best} bytes across {Large - Small} extra calls");
        Assert.That(handler.Deliveries, Is.Empty);
    }

    [Test]
    public async Task Dispatch_to_a_synchronously_completing_handler_allocates_nothing()
    {
        var projection = new FakeAppRegistryProjection();
        var source = new FakeAppSource().Add(Manifest(Subscription("log-feed", "audit", keyPrefix: "log/")));
        projection.Publish(Record(Notes));
        var handler = new CountingHandler();
        var router = new AppSubscriptionRouter(
            projection,
            source,
            new AppSubscriptionHandlerCatalog(Substitute.For<IServiceProvider>(), [new(Notes, "log-feed", _ => handler)]),
            AppRegistryTestData.CreateLedger(new InMemoryAppRegistryStore()),
            NullLogger<AppSubscriptionRouter>.Instance);
        await router.RefreshAsync();
        var mutation = Set("a/notes/audit", "log/1");

        Run(router, mutation, Large);
        var best = long.MaxValue;
        for (var attempt = 0; attempt < Attempts; attempt++)
            best = Math.Min(best, Measure(router, mutation, Large) - Measure(router, mutation, Small));

        Assert.That(best, Is.LessThanOrEqualTo(0), $"dispatch allocated {best} bytes across {Large - Small} extra calls");
        Assert.That(handler.Count, Is.EqualTo(Large + (Attempts * (Large + Small))));
    }

    private static long Measure(AppSubscriptionRouter router, LatticeMutation mutation, int iterations)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        Run(router, mutation, iterations);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static void Run(AppSubscriptionRouter router, LatticeMutation mutation, int iterations)
    {
        for (var i = 0; i < iterations; i++)
            _ = router.OnMutationAsync(mutation, CancellationToken.None);
    }

    private sealed class CountingHandler : IAppChangeFeedHandler
    {
        public long Count;

        public Task HandleAsync(AppSubscriptionContext subscription, LatticeMutation mutation, CancellationToken cancellationToken)
        {
            Count++;
            return Task.CompletedTask;
        }
    }
}
