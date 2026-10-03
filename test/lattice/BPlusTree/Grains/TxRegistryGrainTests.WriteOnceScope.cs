using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the scope of the registry's write-once terminal rule at each call site
/// of <see cref="TerminalDecisionGuard.Classify"/> (issue #2331).
/// <para>
/// The guard is model-checked and correct over its inputs, but whether a call
/// site enforces "never both commit and abort" depends on what it passes as
/// <c>hasExisting</c>, which no test of the guard in isolation can see. These
/// tests exercise the composition at the registry instead, so the scope each
/// call site's comment claims is the scope a test holds it to.
/// </para>
/// </summary>
public partial class TxRegistryGrainTests
{
    [Test]
    public async Task MarkAbortedAsync_rejects_an_opposite_outcome_while_the_decision_is_live()
    {
        var (grain, _) = CreateGrain(retention: TimeSpan.FromMinutes(1));
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        Assert.That(
            async () => await grain.MarkAbortedAsync(txid),
            Throws.InvalidOperationException,
            "Within the retention window, before any forget, the write-once rule must hold.");
    }

    [Test]
    public async Task MarkAbortedAsync_records_an_opposite_outcome_once_the_tombstone_is_purged()
    {
        // Zero retention purges on forget, so no row is left for Classify to
        // see: hasExisting is false and the opposite outcome is recorded. This
        // is the limit of the scoped claim, pinned so a comment promising more
        // than this would contradict a test.
        var (grain, state) = CreateGrain(retention: TimeSpan.Zero);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        await grain.MarkAbortedAsync(txid);

        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task RecordTerminalArrivalAsync_rejects_an_opposite_outcome_against_a_tombstoned_decision()
    {
        // Unlike the Mark* paths, which re-record over a tombstone, the
        // receiver's mixed-outcome guard classifies against any row still
        // stored, tombstoned or not.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(retention: TimeSpan.FromMinutes(1), timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        Assert.That(state.State.ForgottenAt, Does.ContainKey(txid), "precondition: the decision is tombstoned");

        Assert.That(
            async () => await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: false, expectedShardCount: 2),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task RecordTerminalArrivalAsync_admits_an_opposite_outcome_once_the_decision_is_purged()
    {
        var (grain, state) = CreateGrain(retention: TimeSpan.Zero);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        Assert.That(state.State.Decisions, Does.Not.ContainKey(txid), "precondition: the decision is purged");

        var arrival = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: false, expectedShardCount: 2);

        Assert.That(arrival.IsFinal, Is.False, "the arrival is tallied rather than rejected");
    }
}
