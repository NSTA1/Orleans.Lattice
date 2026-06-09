using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the WAL placement move hooks on
/// <see cref="WalShardGrain"/>: <c>QuiesceForMoveAsync</c> (version-checked
/// fence + stable-tail report) and the in-gate fence that refuses appends while
/// a move is in progress.
/// </summary>
public partial class WalShardGrainTests
{
    [Test]
    public async Task QuiesceForMoveAsync_fences_and_reports_stable_highest_offset()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        var result = await grain.QuiesceForMoveAsync(
            expectedPlacementVersion: 0, lease: TimeSpan.FromSeconds(30), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Quiesced, Is.True);
            Assert.That(result.HighestOffsetInclusive, Is.EqualTo(2L));
            Assert.That(result.ObservedPlacementVersion, Is.EqualTo(0L));
        });
    }

    [Test]
    public async Task QuiesceForMoveAsync_aborts_without_fencing_on_version_mismatch()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        var result = await grain.QuiesceForMoveAsync(
            expectedPlacementVersion: 99, lease: TimeSpan.FromSeconds(30), CancellationToken.None);

        Assert.That(result.Quiesced, Is.False);
        Assert.That(result.ObservedPlacementVersion, Is.EqualTo(0L));

        // Not fenced: appends still succeed after a mismatched quiesce.
        var seq = await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        Assert.That(seq, Is.EqualTo(1L));
    }

    [Test]
    public async Task AppendAsync_throws_quiescing_while_fenced_for_a_move()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        await grain.QuiesceForMoveAsync(0, TimeSpan.FromSeconds(30), CancellationToken.None);

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("b"), CancellationToken.None),
            Throws.TypeOf<LatticeWalQuiescingException>());
    }
}
