using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantTreeSuggestionSourceTests : TenantRulesTestContext
{
    [Test]
    public async Task SuggestAsync_head_without_a_directory_is_unavailable()
    {
        using var services = new ServiceCollection().BuildServiceProvider();
        var source = new TenantTreeSuggestionSource(services, () => Acme);

        var answer = await source.SuggestAsync("orders", 5, CancellationToken.None);

        Assert.That(answer.IsAvailable, Is.False);
        Assert.That(answer.Items, Is.Empty);
        Assert.That(answer.UnavailableReason, Is.EqualTo(TenantTreeSuggestionSource.UnavailableReason));
    }

    [Test]
    public async Task SuggestAsync_cancelled_waiter_does_not_cancel_a_shared_load_or_poison_later_suggestions()
    {
        var client = UseTrees("t/acme/orders");
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.CatalogGate = gate;
        using var cancellation = new CancellationTokenSource();
        var source = new TenantTreeSuggestionSource(Services, () => Acme);
        var cancelledRead = source.SuggestAsync("", 5, cancellation.Token).AsTask();
        var survivingRead = source.SuggestAsync("ord", 5, CancellationToken.None).AsTask();

        try
        {
            Assert.That(client.Calls, Does.Contain("ListTreesAsync"));
            Assert.That(cancelledRead.IsCompleted, Is.False);
            Assert.That(survivingRead.IsCompleted, Is.False);
            cancellation.Cancel();
            var exception = Assert.ThrowsAsync<TaskCanceledException>(async () => await cancelledRead);
            Assert.That(exception!.CancellationToken, Is.EqualTo(cancellation.Token));
            Assert.That(survivingRead.IsCompleted, Is.False);
        }
        finally
        {
            gate.TrySetResult();
        }

        AssertOrders(await survivingRead);
        AssertOrders(await source.SuggestAsync("ord", 5, CancellationToken.None));
        Assert.That(client.Calls.Count(call => call == "ListTreesAsync"), Is.EqualTo(1));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task SuggestAsync_backend_failure_is_unavailable_until_the_directory_is_refreshed(bool backendCancellation)
    {
        var client = UseTrees("t/acme/orders");
        Exception fault = backendCancellation ? new OperationCanceledException() : new TimeoutException();
        client.Fault = call => call == "ListTreesAsync" ? fault : null;
        var source = new TenantTreeSuggestionSource(Services, () => Acme);

        var unavailable = await source.SuggestAsync("", 5, CancellationToken.None);

        Assert.That(unavailable.IsAvailable, Is.False);
        Assert.That(unavailable.Items, Is.Empty);
        Assert.That(unavailable.UnavailableReason, Is.EqualTo(TenantTreeSuggestionSource.UnavailableReason));

        client.Fault = null;
        Services.GetRequiredService<DataDirectory>().Invalidate();
        AssertOrders(await source.SuggestAsync("ord", 5, CancellationToken.None));
    }

    [Test]
    public void SuggestAsync_null_text_is_rejected_before_loading_the_directory()
    {
        var client = UseTrees("t/acme/orders");
        var source = new TenantTreeSuggestionSource(Services, () => Acme);

        Assert.ThrowsAsync<ArgumentNullException>(async () => await source.SuggestAsync(null!, 5, CancellationToken.None));

        Assert.That(client.Calls, Is.Empty);
    }

    private static void AssertOrders(LtSuggestionSet answer)
    {
        Assert.That(answer.IsAvailable, Is.True);
        Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "orders" }));
        Assert.That(answer.Items.Single().Detail, Is.EqualTo(TenantTreeSuggestionSource.Detail));
        Assert.That(answer.UnavailableReason, Is.Null);
    }
}
