using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterProviderKeySuggestionSourceTests : ClusterTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task SuggestAsync_empty_or_default_catalogue_is_unavailable_and_retried(bool isDefault)
    {
        Admin.AuditWalPlacementAsync("orders", Arg.Any<CancellationToken>()).Returns(
            Audit("orders", isDefault ? default : []),
            Audit("orders", ["blob-b"]));
        var source = Source(() => "orders");

        var empty = await source.SuggestAsync("blob-b", 8, CancellationToken.None);
        var recovered = await source.SuggestAsync("blob-b", 8, CancellationToken.None);
        await source.SuggestAsync("blob", 8, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(empty.IsAvailable, Is.False);
            Assert.That(empty.Items, Is.Empty);
            Assert.That(empty.UnavailableReason, Does.Contain("no provider keys").And.Contain("used as typed"));
            Assert.That(recovered.IsAvailable, Is.True);
            Assert.That(recovered.Find("blob-b"), Is.Not.Null);
        });
        await Admin.Received(2).AuditWalPlacementAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SuggestAsync_unknown_key_keeps_a_nonempty_catalogue_available()
    {
        Admin.AuditWalPlacementAsync("orders", Arg.Any<CancellationToken>()).Returns(Audit("orders", ["blob-a"]));

        var answer = await Source(() => "orders").SuggestAsync("remote-only", 8, CancellationToken.None);

        Assert.That(answer.IsAvailable, Is.True);
        Assert.That(answer.Items, Is.Empty);
    }

    [Test]
    public async Task SuggestAsync_without_a_tree_is_unavailable_without_an_audit()
    {
        var answer = await Source(() => " ").SuggestAsync("blob-b", 8, CancellationToken.None);

        Assert.That(answer.UnavailableReason, Is.EqualTo(ClusterProviderKeySuggestionSource.NoTreeReason));
        Assert.That(Admin.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task SuggestAsync_a_failed_audit_is_unavailable_and_can_recover()
    {
        Admin.AuditWalPlacementAsync("orders", Arg.Any<CancellationToken>()).Returns(
            Task.FromException<TreeWalPlacementAudit>(new TimeoutException()),
            Task.FromResult(Audit("orders", ["blob-b"])));
        var source = Source(() => "orders");

        var failed = await source.SuggestAsync("blob-b", 8, CancellationToken.None);
        var recovered = await source.SuggestAsync("blob-b", 8, CancellationToken.None);

        Assert.That(failed.UnavailableReason, Is.EqualTo(ClusterProviderKeySuggestionSource.UnavailableReason));
        Assert.That(recovered.Find("blob-b"), Is.Not.Null);
    }

    [Test]
    public async Task SuggestAsync_overlapping_reads_keep_each_trees_snapshot_together()
    {
        var orders = new TaskCompletionSource<TreeWalPlacementAudit>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ledger = new TaskCompletionSource<TreeWalPlacementAudit>(TaskCreationOptions.RunContinuationsAsynchronously);
        Admin.AuditWalPlacementAsync("orders", Arg.Any<CancellationToken>()).Returns(orders.Task);
        Admin.AuditWalPlacementAsync("ledger", Arg.Any<CancellationToken>()).Returns(ledger.Task);
        var tree = "orders";
        var source = Source(() => tree);
        var first = source.SuggestAsync("", 8, CancellationToken.None).AsTask();
        tree = "ledger";
        var second = source.SuggestAsync("", 8, CancellationToken.None).AsTask();

        ledger.SetResult(Audit("ledger", ["ledger-key"]));
        var secondAnswer = await second;
        orders.SetResult(Audit("orders", ["orders-key"]));
        var firstAnswer = await first;
        var current = await source.SuggestAsync("", 8, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(firstAnswer.Items.Select(item => item.Value), Is.EqualTo(new[] { "orders-key" }));
            Assert.That(secondAnswer.Items.Select(item => item.Value), Is.EqualTo(new[] { "ledger-key" }));
            Assert.That(current.Items.Select(item => item.Value), Is.EqualTo(new[] { "ledger-key" }));
        });
    }

    private ClusterProviderKeySuggestionSource Source(Func<string?> tree) =>
        new(Services.GetRequiredService<ClusterFacades>(), tree);

    private static TreeWalPlacementAudit Audit(string tree, ImmutableArray<string> keys) =>
        new() { TreeId = tree, KnownProviderKeys = keys };
}
