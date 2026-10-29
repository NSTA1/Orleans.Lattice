using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardRootGrainTxTerminalTests
{
    [TestCase(true, true)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(false, false)]
    public async Task AppendTxTerminalAsync_fuses_fallback_clock_reads_and_reuses_the_chain(
        bool committed, bool inlineWalAppend)
    {
        var clocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.AddMinutes(1).Ticks, Counter = 3 },
            new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.AddMinutes(2).Ticks, Counter = 7 },
            HybridLogicalClock.Zero,
        };
        var h = CreateHarness(clocks);
        var txid = Guid.NewGuid();
        foreach (var leaf in h.Leaves)
            leaf.When(l => l.ApplyTxTerminalAsync(txid, committed, null)).Do(_ =>
                Assert.That(h.Writer!.Appended.Count, Is.EqualTo(inlineWalAppend ? 1 : 0)));

        var pending = await h.Grain.AppendTxTerminalAsync(
            txid, committed, inlineWalAppend: inlineWalAppend);

        var terminal = inlineWalAppend ? h.Writer!.Appended.Single() : pending!.Value;
        Assert.That(clocks.All(clock => terminal.Timestamp > clock), Is.True);
        Assert.That(terminal.Op, Is.EqualTo(committed ? MutationKind.TxCommit : MutationKind.TxAbort));
        foreach (var leaf in h.Leaves)
        {
            await leaf.Received(1).GetClockAndNextSiblingAsync();
            await leaf.DidNotReceive().GetClockAsync();
            await leaf.DidNotReceive().GetNextSiblingAsync();
            await leaf.Received(1).ApplyTxTerminalAsync(txid, committed, null);
        }
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task AppendTxTerminalAsync_without_writer_skips_clocks_but_delivers_every_terminal(bool tracked)
    {
        var h = CreateHarness([HybridLogicalClock.Zero, HybridLogicalClock.Zero], registerWriter: false);
        var txid = Guid.NewGuid();
        if (tracked)
        {
            LatticeTransactionContext.Set(txid);
            using (LatticePreparedContext.BeginScope())
                await h.Grain.SetAsync("k", [1]);
        }
        foreach (var leaf in h.Leaves) leaf.ClearReceivedCalls();

        var pending = await h.Grain.AppendTxTerminalAsync(txid, committed: false, inlineWalAppend: false);

        Assert.That(pending, Is.Null);
        foreach (var leaf in h.Leaves)
        {
            await leaf.DidNotReceive().GetClockAsync();
            await leaf.DidNotReceive().GetClockAndNextSiblingAsync();
            await leaf.Received(tracked ? 0 : 1).GetNextSiblingAsync();
        }
        await h.Leaves[0].Received(1).ApplyTxTerminalAsync(txid, false, null);
        await h.Leaves[1].Received(tracked ? 0 : 1).ApplyTxTerminalAsync(txid, false, null);
    }

    [Test]
    public async Task AppendTxTerminalAsync_override_reuses_links_without_reading_clocks()
    {
        var h = CreateHarness([HybridLogicalClock.Zero, HybridLogicalClock.Zero]);
        var stamp = new HybridLogicalClock { WallClockTicks = 42, Counter = 7 };
        using (LatticeHlcOverrideContext.With(stamp))
            await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true);

        Assert.That(h.Writer!.Appended.Single().Timestamp, Is.EqualTo(stamp));
        foreach (var leaf in h.Leaves)
        {
            await leaf.Received(1).GetNextSiblingAsync();
            await leaf.DidNotReceive().GetClockAsync();
            await leaf.DidNotReceive().GetClockAndNextSiblingAsync();
        }
    }

    [Test]
    public async Task AppendTxTerminalAsync_routing_change_during_append_rewalks_before_delivery()
    {
        var h = CreateHarness([HybridLogicalClock.Zero, HybridLogicalClock.Zero]);
        h.Writer!.OnAppend = () =>
        {
            h.Grain.BeginRoutingMutation();
            h.Grain.EndRoutingMutation();
        };

        await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true);

        foreach (var leaf in h.Leaves)
        {
            await leaf.Received(1).GetClockAndNextSiblingAsync();
            await leaf.Received(1).GetNextSiblingAsync();
        }
    }

    [Test]
    public async Task AppendTxTerminalAsync_active_routing_mutation_keeps_separate_walks_and_clocks()
    {
        var h = CreateHarness([HybridLogicalClock.Zero, HybridLogicalClock.Zero]);
        // Production dispatch brackets the terminal itself; the second bracket
        // represents an already-running interleaved batch.
        h.Grain.BeginRoutingMutation();
        h.Grain.BeginRoutingMutation();
        try
        {
            await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true);
        }
        finally
        {
            h.Grain.EndRoutingMutation();
            h.Grain.EndRoutingMutation();
        }

        foreach (var leaf in h.Leaves)
        {
            await leaf.DidNotReceive().GetClockAndNextSiblingAsync();
            await leaf.Received(1).GetClockAsync();
            await leaf.Received(2).GetNextSiblingAsync();
        }
    }

    [Test]
    public async Task AppendTxTerminalAsync_routing_change_during_append_delivers_to_new_sibling()
    {
        var h = CreateHarness([HybridLogicalClock.Zero, HybridLogicalClock.Zero]);
        var txid = Guid.NewGuid();
        h.Leaves[0].GetClockAndNextSiblingAsync().Returns(new LeafClockPage
        {
            Clock = HybridLogicalClock.Zero,
            NextSibling = null,
        });
        h.Writer!.OnAppend = () =>
        {
            h.Grain.BeginRoutingMutation();
            h.Grain.EndRoutingMutation();
        };
        foreach (var leaf in h.Leaves) leaf.ClearReceivedCalls();

        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        await h.Leaves[1].DidNotReceive().GetClockAndNextSiblingAsync();
        await h.Leaves[1].Received(1).GetNextSiblingAsync();
        await h.Leaves[1].Received(1).ApplyTxTerminalAsync(txid, true, null);
    }

    [Test]
    public async Task AppendTxTerminalAsync_overlapping_routing_mutation_resamples_clocks()
    {
        var h = CreateHarness([HybridLogicalClock.Zero, HybridLogicalClock.Zero]);
        var later = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.AddHours(1).Ticks, Counter = 9 };
        h.Leaves[0].GetClockAndNextSiblingAsync().Returns(_ =>
        {
            h.Grain.BeginRoutingMutation();
            h.Grain.EndRoutingMutation();
            return new LeafClockPage
            {
                Clock = HybridLogicalClock.Zero,
                NextSibling = GrainId.Create("leaf", $"{TreeId}-leaf-1"),
            };
        });
        h.Leaves[0].GetClockAsync().Returns(later);
        foreach (var leaf in h.Leaves) leaf.ClearReceivedCalls();

        await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true);

        Assert.That(h.Writer!.Appended.Single().Timestamp, Is.GreaterThan(later));
        foreach (var leaf in h.Leaves)
        {
            await leaf.Received(1).GetClockAsync();
            await leaf.Received(1).GetNextSiblingAsync();
        }
    }

    [Test]
    public async Task AppendTxTerminalAsync_WAL_append_failure_prevents_delivery()
    {
        var h = CreateHarness([HybridLogicalClock.Zero, HybridLogicalClock.Zero]);
        h.Writer!.OnAppend = () => throw new InvalidOperationException("append failed");

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true));

        Assert.That(h.Writer.Appended, Is.Empty);
        foreach (var leaf in h.Leaves)
            await leaf.DidNotReceive().ApplyTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), null);
    }

    [Test]
    public async Task AppendTxTerminalAsync_clock_read_failure_prevents_WAL_and_delivery()
    {
        var h = CreateHarness([HybridLogicalClock.Zero]);
        h.Leaves[0].GetClockAndNextSiblingAsync()
            .Returns(Task.FromException<LeafClockPage>(new InvalidOperationException("replay failed")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true));

        Assert.That(h.Writer!.Appended, Is.Empty);
        await h.Leaves[0].DidNotReceive().ApplyTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), null);
    }
}
