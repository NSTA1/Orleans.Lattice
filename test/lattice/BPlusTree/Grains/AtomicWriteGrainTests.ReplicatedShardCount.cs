using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detector for the <c>OriginBroadcast(k)</c> row of
/// <c>spec/atomic-commit/RefinementCrossCluster.md</c> (issue #4436). A
/// replicated terminal carries the saga's touched-shard count, and the
/// receiver's per-source-shard tally waits for that many source shards before
/// it marks the saga; a terminal carrying no count takes the legacy path and
/// marks the saga on its own (<c>TxRegistryGrain.RecordTerminalArrivalAsync</c>).
/// So the count every terminal of a multi-shard saga is appended under is what
/// keeps the replicated saga all-or-nothing on the receiver. These pin it on both
/// outcomes, through the same harness the <c>NoMixedTerminals</c> detectors use.
/// </summary>
public partial class AtomicWriteGrainTests
{
    [Test]
    public async Task Committing_saga_stamps_its_touched_shard_count_on_every_terminal_it_broadcasts()
    {
        var (grain, state, _, log) = CreateGrainRecordingTerminalVerdicts();
        var entries = EntriesSpanningDistinctShards();

        await grain.ExecuteAsync(TreeId, entries);

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        AssertEveryTerminalCarriesTheTouchedShardCount(state.State.TouchedShards.Count, log.BroadcastShardCounts);
    }

    [Test]
    public async Task Aborting_saga_stamps_its_touched_shard_count_on_every_terminal_it_broadcasts()
    {
        var (grain, state, lattice, log) = CreateGrainRecordingTerminalVerdicts();
        var entries = EntriesSpanningDistinctShards();
        var failingKey = entries[^1].Key;
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(call =>
            {
                foreach (var entry in (List<KeyValuePair<string, byte[]>>)call[0])
                {
                    if (string.Equals(entry.Key, failingKey, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException("simulated mid-batch failure");
                    }
                }
                return Task.CompletedTask;
            });

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ExecuteAsync(TreeId, entries));

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        AssertEveryTerminalCarriesTheTouchedShardCount(state.State.TouchedShards.Count, log.BroadcastShardCounts);
    }

    private static void AssertEveryTerminalCarriesTheTouchedShardCount(int touched, IReadOnlyList<int?> counts)
    {
        // Non-vacuity: a single-shard saga is all-or-nothing whatever its
        // terminal carries, so the count only matters, and is only falsifiable,
        // across more than one shard.
        Assert.That(touched, Is.GreaterThan(1),
            "A single touched shard makes the stamped count irrelevant, so the assertion would be vacuous.");
        Assert.That(counts, Has.Count.EqualTo(touched),
            "Every touched shard must receive exactly one terminal.");
        Assert.That(counts, Is.All.EqualTo(touched),
            $"Every terminal must be appended under the saga's touched-shard count ({touched}); "
            + $"saw [{string.Join(", ", counts.Select(c => c?.ToString() ?? "unset"))}]. An unset count "
            + "sends the replicated terminal down the receiver's legacy mark-on-first-terminal path.");
    }
}
