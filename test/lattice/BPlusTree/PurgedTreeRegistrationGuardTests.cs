using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for <see cref="PurgedTreeRegistrationGuard"/> (issue #4219).
/// </summary>
[TestFixture]
public class PurgedTreeRegistrationGuardTests
{
    /// <summary>
    /// A leaf or internal node whose tree id was never set resolves options for
    /// the empty id. No tree can carry that id, so no deletion record can exist
    /// for it, and Orleans refuses an empty string grain key outright: the guard
    /// must answer without addressing the deletion grain.
    /// </summary>
    [TestCase("")]
    [TestCase(" ")]
    public async Task ThrowIfPurgedAsync_unset_tree_id_does_not_address_the_deletion_grain(string treeId)
    {
        var grainFactory = Substitute.For<IGrainFactory>();

        await PurgedTreeRegistrationGuard.ThrowIfPurgedAsync(grainFactory, treeId);

        grainFactory.DidNotReceiveWithAnyArgs().GetGrain<ITreeDeletionGrain>(default(string)!);
    }

    [Test]
    public void ThrowIfPurgedAsync_purged_tree_id_throws()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.HoldsCompletedPurgeAsync().Returns(true);
        grainFactory.GetGrain<ITreeDeletionGrain>("t").Returns(deletion);

        Assert.ThrowsAsync<InvalidOperationException>(
            () => PurgedTreeRegistrationGuard.ThrowIfPurgedAsync(grainFactory, "t"));
    }

    [Test]
    public async Task IsPurgedAsync_reports_the_deletion_record()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var purged = Substitute.For<ITreeDeletionGrain>();
        purged.HoldsCompletedPurgeAsync().Returns(true);
        var live = Substitute.For<ITreeDeletionGrain>();
        live.HoldsCompletedPurgeAsync().Returns(false);
        grainFactory.GetGrain<ITreeDeletionGrain>("purged").Returns(purged);
        grainFactory.GetGrain<ITreeDeletionGrain>("live").Returns(live);

        Assert.That(await PurgedTreeRegistrationGuard.IsPurgedAsync(grainFactory, "purged"), Is.True);
        Assert.That(await PurgedTreeRegistrationGuard.IsPurgedAsync(grainFactory, "live"), Is.False);
        Assert.That(await PurgedTreeRegistrationGuard.IsPurgedAsync(grainFactory, string.Empty), Is.False);
    }
}
