using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the <see cref="ILatticeReplicationDeadLetters"/> public
/// contract: in steady state the dead-letter queue for a replicated
/// tree is empty (count = 0, list = empty), <c>DiscardAsync</c> on a
/// missing entry id returns <see langword="false"/>, and
/// <c>ReplayAsync</c> on a missing entry id returns
/// <see langword="null"/>. The applier-failure population path is
/// covered by dedicated unit tests (
/// <c>DeadLetterTrackingReplicationApplierTests</c>); this contract
/// concern is the steady-state invariant.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public async Task ILatticeReplicationDeadLetters_count_is_zero_in_steady_state()
    {
        var treeId = NextTreeId("dlq-count");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var dlq = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<ILatticeReplicationDeadLetters>();

        Assert.That(await dlq.CountAsync(treeId), Is.Zero);
    }

    [Test]
    public async Task ILatticeReplicationDeadLetters_list_is_empty_in_steady_state()
    {
        var treeId = NextTreeId("dlq-list");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var dlq = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<ILatticeReplicationDeadLetters>();

        var entries = await dlq.ListAsync(treeId);
        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task ILatticeReplicationDeadLetters_discard_on_missing_entry_returns_false()
    {
        var treeId = NextTreeId("dlq-discard");
        await CreateReplicatedTreeAsync(treeId);

        var dlq = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<ILatticeReplicationDeadLetters>();

        var discarded = await dlq.DiscardAsync(treeId, entryId: 9999);
        Assert.That(discarded, Is.False);
    }

    [Test]
    public async Task ILatticeReplicationDeadLetters_replay_on_missing_entry_returns_null()
    {
        var treeId = NextTreeId("dlq-replay");
        await CreateReplicatedTreeAsync(treeId);

        var dlq = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<ILatticeReplicationDeadLetters>();

        var result = await dlq.ReplayAsync(treeId, entryId: 9999);
        Assert.That(result, Is.Null);
    }
}
