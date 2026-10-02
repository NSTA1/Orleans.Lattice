using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The purged-tree abandonment (issue #4271) and the alias-cutover fence
/// (issue #4264) both retire an in-flight split or fold. These pin that they
/// compose: a saga bound to a physical copy whose logical tree was then purged
/// trips the fence first, and whichever path finishes the job, the saga ends
/// retired rather than retrying forever.
/// </summary>
public sealed partial class PurgedTreeSagaAbandonmentTests
{
    private const string RetiredCopyTreeId = "purged-saga-tree-copy";

    private static readonly string[] FencedSagas = ["split", "consolidation"];

    private static Harness CreateBound(string saga) => saga switch
    {
        "split" => SplitSaga(RetiredCopyTreeId),
        "consolidation" => ConsolidationSaga(RetiredCopyTreeId),
        _ => throw new ArgumentOutOfRangeException(nameof(saga), saga, null),
    };

    private static void ArrangeRegistryRowGone(Harness h) =>
        h.Factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(null));

    [TestCaseSource(nameof(FencedSagas))]
    public async Task Fence_abandons_a_saga_bound_to_a_copy_of_a_purged_tree_even_when_the_abort_is_refused(string saga)
    {
        var h = CreateBound(saga);
        ArrangeRegistryRowGone(h);
        h.Factory.GetGrain<IShardRootGrain>("any").AbortSplitAsync()
            .ThrowsAsync(new InvalidOperationException("This tree has been deleted and is no longer accessible."));
        var tick = await ArmAsync(h);

        await tick(CancellationToken.None);

        Assert.That(h.InProgress(), Is.False, "The fence must retire the saga.");
        h.Timer.Received(1).Dispose();
        await h.Reminders.Received(1).UnregisterReminder(h.GrainId, Arg.Any<IGrainReminder>());
    }

    [TestCaseSource(nameof(FencedSagas))]
    public async Task Purged_tree_abandonment_finishes_a_fence_abandonment_that_faulted(string saga)
    {
        var h = CreateBound(saga);
        ArrangeRegistryRowGone(h);
        h.Factory.GetGrain<IShardRootGrain>("any").AbortSplitAsync()
            .ThrowsAsync(new TimeoutException("shard unreachable"));
        var tick = await ArmAsync(h);

        await tick(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(h.InProgress(), Is.False);
            Assert.That(h.RecordExists(), Is.False, "The purged-tree path clears the saga's durable row.");
        });
        h.Timer.Received(1).Dispose();
        await h.Reminders.Received(1).UnregisterReminder(h.GrainId, Arg.Any<IGrainReminder>());
    }
}
