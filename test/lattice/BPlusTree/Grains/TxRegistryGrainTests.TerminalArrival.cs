using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for
/// <see cref="ITxRegistryGrain.RecordTerminalArrivalAsync(System.Guid, int, bool, int)"/>.
/// The method is the receiver-side gate that defers the per-tree
/// linearization mark and the per-shard terminal fan-out until every
/// per-source-shard terminal of a cross-cluster saga has arrived,
/// preserving strict atomic visibility on replicated clusters.
/// </summary>
[TestFixture]
public class TxRegistryGrainTerminalArrivalTests
{
    private static (TxRegistryGrain grain, Orleans.Lattice.Tests.Fakes.FakePersistentState<Orleans.Lattice.BPlusTree.State.TxRegistryState> state) CreateGrain(
        string treeId = "tree-x")
    {
        var context = NSubstitute.Substitute.For<Orleans.Runtime.IGrainContext>();
        context.GrainId.Returns(Orleans.Runtime.GrainId.Create("tx-registry", treeId));
        var state = new Orleans.Lattice.Tests.Fakes.FakePersistentState<Orleans.Lattice.BPlusTree.State.TxRegistryState>();
        var options = new LatticeOptions { TxDecisionRetention = System.TimeSpan.Zero };
        var optionsMonitor = NSubstitute.Substitute.For<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(NSubstitute.Arg.Any<string>()).Returns(options);
        var grain = new TxRegistryGrain(context, optionsMonitor, state);
        return (grain, state);
    }

    [Test]
    public async Task RecordTerminalArrivalAsync_with_zero_expected_count_short_circuits_to_final()
    {
        // Legacy-producer fast path: a 0 expected count means the
        // producer did not stamp the gate, so the gate falls back to
        // "mark on first terminal" semantics and IsFinal must be true
        // immediately. The ObservedSourceShards must contain only the
        // current arrival's source-shard index so the caller's fan-out
        // loop body is uniform between the legacy and gated paths.
        var (grain, _) = CreateGrain();
        var txid = System.Guid.NewGuid();

        var result = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 3, committed: true, expectedShardCount: 0);

        Assert.That(result.IsFinal, Is.True);
        Assert.That(result.FinalOutcome, Is.EqualTo(TxStatus.Committed));
        Assert.That(result.ObservedSourceShards, Is.EqualTo(new[] { 3 }));
    }

    [Test]
    public async Task RecordTerminalArrivalAsync_with_partial_tally_reports_not_final()
    {
        // Two arrivals out of three expected: the tally is incomplete,
        // IsFinal must be false, and ObservedSourceShards must be
        // empty (in-progress arrivals do not ship interim state).
        var (grain, _) = CreateGrain();
        var txid = System.Guid.NewGuid();

        var first = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: true, expectedShardCount: 3);
        var second = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 1, committed: true, expectedShardCount: 3);

        Assert.That(first.IsFinal, Is.False);
        Assert.That(first.ObservedSourceShards, Is.Empty);
        Assert.That(second.IsFinal, Is.False);
        Assert.That(second.ObservedSourceShards, Is.Empty);
    }

    [Test]
    public async Task RecordTerminalArrivalAsync_returns_final_on_last_distinct_arrival()
    {
        // Three arrivals out of three expected: the tally is complete,
        // IsFinal must be true, and ObservedSourceShards must contain
        // every distinct source-shard index that has arrived, sorted.
        var (grain, _) = CreateGrain();
        var txid = System.Guid.NewGuid();

        await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 2, committed: true, expectedShardCount: 3);
        await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: true, expectedShardCount: 3);
        var third = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 1, committed: true, expectedShardCount: 3);

        Assert.That(third.IsFinal, Is.True);
        Assert.That(third.FinalOutcome, Is.EqualTo(TxStatus.Committed));
        Assert.That(third.ObservedSourceShards, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public async Task RecordTerminalArrivalAsync_dedups_duplicate_source_shard_index()
    {
        // A duplicate-delivery retry of the same source-shard terminal
        // must be a no-op for the tally side: re-arriving on shard 0
        // does not advance the tally toward IsFinal.
        var (grain, _) = CreateGrain();
        var txid = System.Guid.NewGuid();

        var first = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: true, expectedShardCount: 2);
        var dup = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: true, expectedShardCount: 2);

        Assert.That(first.IsFinal, Is.False);
        Assert.That(dup.IsFinal, Is.False, "Duplicate arrival must not flip IsFinal when distinct count is still below expected.");
        Assert.That(dup.ObservedSourceShards, Is.Empty);

        // The non-duplicate second source-shard now drives the tally to final.
        var second = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 1, committed: true, expectedShardCount: 2);
        Assert.That(second.IsFinal, Is.True);
        Assert.That(second.ObservedSourceShards, Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public async Task RecordTerminalArrivalAsync_adopts_higher_expected_count_under_drift()
    {
        // Producer-side shadow-forward split mid-saga can grow the
        // touched-shard set between successive per-shard terminals.
        // The registry must adopt max(seen, expected) so the gate is
        // never under-counted by an earlier, smaller expected value.
        var (grain, _) = CreateGrain();
        var txid = System.Guid.NewGuid();

        // First two arrivals report expected=2 - if the gate trusted
        // the first value naively it would flip to IsFinal=true here.
        await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: true, expectedShardCount: 2);
        var maybeFinal = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 1, committed: true, expectedShardCount: 2);
        Assert.That(maybeFinal.IsFinal, Is.True, "Two of two arrivals with expected=2 must be final.");

        // A fresh txid demonstrates the drift case: first arrival says
        // expected=2, second arrival upgrades to expected=3.
        var txid2 = System.Guid.NewGuid();
        await grain.RecordTerminalArrivalAsync(txid2, sourceShardIndex: 0, committed: true, expectedShardCount: 2);
        var notYetFinal = await grain.RecordTerminalArrivalAsync(txid2, sourceShardIndex: 1, committed: true, expectedShardCount: 3);
        Assert.That(notYetFinal.IsFinal, Is.False, "Adopted expected=3 must keep the tally pending after two arrivals.");

        var nowFinal = await grain.RecordTerminalArrivalAsync(txid2, sourceShardIndex: 2, committed: true, expectedShardCount: 3);
        Assert.That(nowFinal.IsFinal, Is.True);
        Assert.That(nowFinal.ObservedSourceShards, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public void RecordTerminalArrivalAsync_rejects_mixed_outcomes()
    {
        // A saga's terminal stream must agree on commit/abort. A mixed
        // sequence is a protocol violation; the registry surfaces it
        // as a hard exception rather than silently corrupting the gate.
        var (grain, _) = CreateGrain();
        var txid = System.Guid.NewGuid();

        Assert.That(async () =>
        {
            await grain.MarkCommittedAsync(txid);
            await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: false, expectedShardCount: 1);
        }, Throws.InvalidOperationException);
    }

    [Test]
    public async Task RecordTerminalArrivalAsync_abort_terminal_returns_aborted_outcome()
    {
        // Symmetric to the commit case: a final abort arrival reports
        // FinalOutcome=Aborted so the caller flips MarkAbortedAsync.
        var (grain, _) = CreateGrain();
        var txid = System.Guid.NewGuid();

        var first = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: false, expectedShardCount: 2);
        var second = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 1, committed: false, expectedShardCount: 2);

        Assert.That(first.IsFinal, Is.False);
        Assert.That(second.IsFinal, Is.True);
        Assert.That(second.FinalOutcome, Is.EqualTo(TxStatus.Aborted));
        Assert.That(second.ObservedSourceShards, Is.EqualTo(new[] { 0, 1 }));
    }
}

