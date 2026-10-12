using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewMaintainerPhysicalStagingTests
{
    [TestCase(1, false)]
    [TestCase(2, false)]
    [TestCase(1, true)]
    [TestCase(2, true)]
    public async Task Drain_terminal_only_orphan_recovers_same_physical_cardinality_in_bounded_passes(
        int budget, bool earlierTerminalBelowCheckpoint)
    {
        var h = await Harness.CreateAsync(prepareScanLimit: budget);
        for (var offset = 0; offset <= 6; offset++)
        {
            h.Append(Old, offset, offset == 4 ? Prepare(Old, "missed", 11)
                : offset == 5 && earlierTerminalBelowCheckpoint
                    ? Terminal(Old) with { AtomicShardCount = 2 }
                : new LatticeMutation
            {
                TreeId = Old, Kind = MutationKind.Set, Category = MutationCategory.User,
                Key = $"ordinary-{offset}", Value = [2], Timestamp = Clock(20),
            });
        }

        h.Append(Old, 7, earlierTerminalBelowCheckpoint
            ? Terminal(Old) with { AtomicShardCount = 2, ShardIndex = 1 }
            : Terminal(Old));
        var requiredEntries = earlierTerminalBelowCheckpoint ? 6 : 5;
        var passes = (requiredEntries + budget - 1) / budget;
        for (var pass = 0; pass < passes - 1; pass++)
        {
            var before = h.HistoryEntries.Count;
            await h.Grain.DrainAsync();
            Assert.That(h.HistoryEntries.Count - before, Is.EqualTo(budget),
                "the recovery search has one examined-entry budget per drain, not an unbounded lookback");
            Assert.That(h.State.State.ActiveGeneration, Is.Zero,
                "a terminal and a sampled head alone cannot justify rebuilding away its payload obligation");
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(6));
        }

        await h.Grain.DrainAsync();

        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1),
            "an actual prepare below this physical checkpoint supplies the missing cardinality witness");
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(7));
        Assert.That(h.HistoryReads.Take(passes).Select(x => x.From),
            Is.EqualTo(Enumerable.Range(0, passes).Select(x => (long)x * budget - 1)));
        Assert.That(h.HistoryReads.All(x => x.Tree == Old), Is.True);
    }

    [Test]
    public async Task Drain_incomplete_terminal_set_does_not_retire_a_missing_prepare()
    {
        var h = await Harness.CreateAsync(prepareScanLimit: 1);
        h.Append(Old, 0, Prepare(Old, "missed", 11));
        h.Append(Old, 7, Terminal(Old) with { AtomicShardCount = 2 });

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();

        Assert.That(h.HistoryReads, Is.Not.Empty,
            "a real historical prepare alone is not enough without every distinct shard terminal");
        Assert.That(h.State.State.ActiveGeneration, Is.Zero);
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(6));
    }

    [Test]
    public async Task Drain_historical_orphan_search_shares_one_budget_across_batches()
    {
        var h = await Harness.CreateAsync(prepareScanLimit: 1);
        var second = Guid.NewGuid();
        h.Append(Old, 0, Prepare(Old, "second", 11) with { TransactionId = second });
        h.Append(Old, 4, Prepare(Old, "first", 11));
        h.Append(Old, 7, Terminal(Old));
        h.Append(Old, 8, Terminal(Old) with { TransactionId = second });

        await h.Grain.DrainAsync();

        Assert.That(h.HistoryEntries, Has.Count.EqualTo(1),
            "a second unknown batch must not restart the pass's examined-entry budget");
        Assert.That(h.HistoryReads, Has.Count.EqualTo(1));
        Assert.That(h.State.State.ActiveGeneration, Is.Zero);
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(6));
    }
}
