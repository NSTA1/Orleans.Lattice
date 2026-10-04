using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

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

    /// <summary>
    /// An extreme but validated quiesce lease - the coordinator-supplied lease
    /// carries no upper bound - once overflowed the Stopwatch-tick fence deadline
    /// cast to a negative, past instant, so <c>ThrowIfMoveFenced</c> read the fence
    /// as already expired on the first append: it self-deactivated and re-resolved
    /// placement instead of holding the shard still for the coordinator, defeating
    /// the quiesce entirely. The deadline now saturates, so an extreme lease holds
    /// the fence as an active move (asserted through the active-fence message,
    /// since the lapsed path throws the same exception type) rather than a lapsed
    /// one (siblings of issues 2221 and 2342).
    /// </summary>
    [Test]
    public async Task AppendAsync_reports_an_active_fence_under_an_extreme_quiesce_lease()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        await grain.QuiesceForMoveAsync(0, TimeSpan.MaxValue, CancellationToken.None);

        var thrown = Assert.ThrowsAsync<LatticeWalQuiescingException>(
            async () => await grain.AppendAsync(MakeEntry("b"), CancellationToken.None));
        Assert.That(
            thrown!.Message,
            Does.Contain("is quiesced for a placement move"),
            "an extreme lease must hold the fence as an active move, not read as a lapsed one " +
            "that re-resolves placement");
    }

    /// <summary>
    /// The release of a fence that does not depend on the move coordinator
    /// surviving: once the quiesce lease lapses without a cutover, the next
    /// append is still refused, and the activation asks to be deactivated so the
    /// next one re-resolves placement and comes up unfenced.
    /// </summary>
    [Test]
    public async Task An_expired_quiesce_lease_requests_deactivation_so_the_fence_does_not_hold_for_ever()
    {
        var (grain, context) = await CreateGrainWithContextAsync(new InMemoryWalStorageProvider(), new LatticeOptions());
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        await grain.QuiesceForMoveAsync(0, TimeSpan.FromMilliseconds(1), CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        var thrown = Assert.ThrowsAsync<LatticeWalQuiescingException>(
            async () => await grain.AppendAsync(MakeEntry("b"), CancellationToken.None));
        Assert.That(thrown!.Message, Does.Contain("quiesce lease expired"),
            "a lapsed lease must keep refusing appends until the activation is gone");
        context.Received(1).Deactivate(
            Arg.Is<DeactivationReason>(r => r.ReasonCode == DeactivationReasonCode.ApplicationRequested),
            Arg.Any<CancellationToken>());
    }
}
