using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterTreeSuggestionSourceTests : ClusterTestContext
{
    [Test]
    public async Task SuggestAsync_caller_cancellation_propagates_and_a_later_read_recovers()
    {
        using var cancellation = new CancellationTokenSource();
        var pending = new TaskCompletionSource<TreeCatalogPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        Explorer.Connection.ListTreesAsync(Arg.Any<CatalogRequest>(), cancellation.Token).Returns(pending.Task);
        var source = Source();
        var read = source.SuggestAsync("", 5, cancellation.Token).AsTask();

        Assert.That(read.IsCompleted, Is.False);
        cancellation.Cancel();
        pending.SetCanceled(cancellation.Token);
        var exception = Assert.ThrowsAsync<TaskCanceledException>(async () => await read);
        Assert.That(exception!.CancellationToken, Is.EqualTo(cancellation.Token));

        UseTrees(Tree("orders"));
        AssertOrders(await source.SuggestAsync("ord", 5, CancellationToken.None));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task SuggestAsync_backend_failure_is_unavailable_and_not_cached(bool backendCancellation)
    {
        Exception fault = backendCancellation ? new OperationCanceledException() : new TimeoutException();
        Explorer.Connection.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<TreeCatalogPage>(fault));
        var source = Source();

        var unavailable = await source.SuggestAsync("", 5, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(unavailable.IsAvailable, Is.False);
            Assert.That(unavailable.Items, Is.Empty);
            Assert.That(unavailable.UnavailableReason, Is.EqualTo(ClusterTreeSuggestionSource.UnavailableReason));
        });

        UseTrees(Tree("orders"));
        AssertOrders(await source.SuggestAsync("ord", 5, CancellationToken.None));
        await Explorer.Connection.Received(2).ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SuggestAsync_failed_refresh_never_serves_the_previous_projection()
    {
        UseTrees(Tree("orders"));
        var source = Source();
        AssertOrders(await source.SuggestAsync("", 5, CancellationToken.None));
        Services.GetRequiredService<ClusterTreeCatalog>().Invalidate();
        Explorer.Connection.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<TreeCatalogPage>(new TimeoutException()));

        var unavailable = await source.SuggestAsync("", 5, CancellationToken.None);

        Assert.That(unavailable.IsAvailable, Is.False);
        Assert.That(unavailable.Items, Is.Empty);
        UseTrees(Tree("billing"));
        var recovered = await source.SuggestAsync("", 5, CancellationToken.None);
        Assert.That(recovered.Items.Select(item => item.Value), Is.EqualTo(new[] { "billing" }));
    }

    [Test]
    public async Task SuggestAsync_scope_failure_is_unavailable_and_can_recover()
    {
        UseTrees(Tree("orders"));
        var failScope = true;
        var source = Source(() => failScope ? throw new InvalidOperationException("scope unavailable") : null);

        var unavailable = await source.SuggestAsync("", 5, CancellationToken.None);
        Assert.That(unavailable.UnavailableReason, Is.EqualTo(ClusterTreeSuggestionSource.UnavailableReason));
        Assert.That(unavailable.Items, Is.Empty);

        failScope = false;
        AssertOrders(await source.SuggestAsync("", 5, CancellationToken.None));
    }

    [Test]
    public void SuggestAsync_null_text_is_rejected_before_reading_the_catalogue()
    {
        var source = Source();
        Explorer.Connection.ClearReceivedCalls();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await source.SuggestAsync(null!, 5, CancellationToken.None));

        Assert.That(Explorer.Connection.ReceivedCalls(), Is.Empty);
    }

    private ClusterTreeSuggestionSource Source(Func<string?>? scope = null) =>
        new(Services.GetRequiredService<ClusterTreeCatalog>(), scope);

    private static void AssertOrders(LtSuggestionSet answer)
    {
        Assert.That(answer.IsAvailable, Is.True);
        Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "orders" }));
        Assert.That(answer.UnavailableReason, Is.Null);
    }
}
