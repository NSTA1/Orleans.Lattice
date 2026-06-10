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
    public async Task QuiesceForMoveAsync_aborts_without_fencing_when_activation_is_ahead_of_coordinator()
    {
        // The activation already resolved a *newer* placement (version 5) than
        // the coordinator's expected version (3): the coordinator is stale, so
        // quiescing would fence the wrong provider. Abort without fencing.
        var grain = await CreateGrainAsync(placementVersion: 5);
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        var result = await grain.QuiesceForMoveAsync(
            expectedPlacementVersion: 3, lease: TimeSpan.FromSeconds(30), CancellationToken.None);

        Assert.That(result.Quiesced, Is.False);
        Assert.That(result.ObservedPlacementVersion, Is.EqualTo(5L));

        // Not fenced: appends still succeed after a stale-coordinator quiesce.
        var seq = await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        Assert.That(seq, Is.EqualTo(1L));
    }

    [Test]
    public async Task QuiesceForMoveAsync_quiesces_a_lagging_activation_behind_the_coordinator_version()
    {
        // The activation resolved an *older* placement (version 0) than the
        // coordinator expects (7) - the tree's global version advanced past this
        // shard via a move of some *other* partition. This partition was not
        // remapped (else it would have been deactivated), so its provider is
        // still current and the lagging activation is safe to quiesce.
        var grain = await CreateGrainAsync(placementVersion: 0);
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);

        var result = await grain.QuiesceForMoveAsync(
            expectedPlacementVersion: 7, lease: TimeSpan.FromSeconds(30), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Quiesced, Is.True);
            Assert.That(result.HighestOffsetInclusive, Is.EqualTo(1L));
            Assert.That(result.ObservedPlacementVersion, Is.EqualTo(0L));
        });

        // Fenced now: appends are refused while the move holds the lease.
        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("c"), CancellationToken.None),
            Throws.TypeOf<LatticeWalQuiescingException>());
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
