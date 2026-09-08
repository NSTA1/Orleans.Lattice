using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Covers the activation-scoped delta-run buffers the CRDT coalescer folds
/// through: a same-key run is collected once and folded linearly instead of
/// pairwise, its typed deserialisation is deferred until a key actually
/// repeats, and the run buffers are pooled across pumps. None of that is
/// visible in an entry count, so these tests decode the shipped records and
/// assert on the coalesced delta the peer would apply.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    /// <summary>The records behind the most recent batch handed to the transport.</summary>
    private static List<WalRecord> LastShippedRecords(
        IReplicationTransport transport,
        StubReplogShardGrain feed)
    {
        var calls = transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();
        Assert.That(calls, Is.Not.Empty,
            "the shipper must have invoked the transport at least once before this assertion");
        var batch = (ReplicationBatch)calls[^1].GetArguments()[0]!;
        Assert.That(batch.EncodedEnvelope, Is.Not.Null,
            "the shipper must populate EncodedEnvelope on every batch on the framing-only path");
        var entries = batch.EncodedEnvelope!.Value.EncodedEntries.Span;
        var records = new List<WalRecord>(entries.Length);
        for (var i = 0; i < entries.Length; i++)
        {
            records.Add(feed.DecodeShippedEntry(entries[i].AsSpan()));
        }

        return records;
    }

    /// <summary>The single shipped record for <paramref name="key"/>, decoded.</summary>
    private static WalRecord ShippedRecordFor(
        IReplicationTransport transport,
        StubReplogShardGrain feed,
        string key)
    {
        var matches = LastShippedRecords(transport, feed).Where(r => r.Key == key).ToList();
        Assert.That(matches, Has.Count.EqualTo(1), $"exactly one entry must ship for key '{key}'");
        return matches[0];
    }

    /// <summary>The PN-Counter increments carried by a shipped record's coalesced delta.</summary>
    private static Dictionary<string, long> Increments(WalRecord record)
    {
        Assert.That(record.Delta, Is.Not.Null, "the shipped record must carry a typed delta");
        return ((PnCounterDelta)PnShape.DeserializeDelta(record.Delta!)).Increments;
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_does_not_carry_a_run_across_pumps()
    {
        // The run buffers are rented from an activation-scoped pool and reused
        // on the next pump. A buffer that was not cleared on release would fold
        // the first pump's deltas back into the second pump's combined delta.
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        feed.Append(MakeCrdtSet("k", ticks: 1, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 2, PnDelta("A", 2)));
        await grain.PumpForTestingAsync(CancellationToken.None);

        feed.Append(MakeCrdtSet("k", ticks: 3, PnDelta("B", 5)));
        feed.Append(MakeCrdtSet("k", ticks: 4, PnDelta("B", 6)));
        await grain.PumpForTestingAsync(CancellationToken.None);

        var increments = Increments(ShippedRecordFor(transport, feed, "k"));

        Assert.Multiple(() =>
        {
            Assert.That(increments["B"], Is.EqualTo(6));
            Assert.That(increments.ContainsKey("A"), Is.False,
                "the second pump's combined delta must not observe the first pump's run");
        });
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_folds_each_interleaved_key_run_independently()
    {
        // Two interleaved keys each fold a run of their own, so each rents a
        // distinct buffer from the pool. A shared or mis-indexed buffer would
        // bleed one key's replica components into the other's delta.
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        feed.Append(MakeCrdtSet("k", ticks: 1, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("j", ticks: 2, PnDelta("B", 10)));
        feed.Append(MakeCrdtSet("k", ticks: 3, PnDelta("A", 2)));
        feed.Append(MakeCrdtSet("j", ticks: 4, PnDelta("B", 20)));
        feed.Append(MakeCrdtSet("k", ticks: 5, PnDelta("A", 3)));

        await grain.PumpForTestingAsync(CancellationToken.None);

        var k = Increments(ShippedRecordFor(transport, feed, "k"));
        var j = Increments(ShippedRecordFor(transport, feed, "j"));

        Assert.Multiple(() =>
        {
            Assert.That(k, Is.EquivalentTo(new Dictionary<string, long> { ["A"] = 3 }));
            Assert.That(j, Is.EquivalentTo(new Dictionary<string, long> { ["B"] = 20 }));
        });
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_ships_a_singleton_key_delta_verbatim()
    {
        // A key seen once is never folded, so the shipper defers its typed
        // deserialisation entirely and the entry ships with its original delta
        // bytes - even when a folded key sits beside it in the same batch.
        var solo = PnDelta("A", 7);
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        feed.Append(MakeCrdtSet("solo", ticks: 1, solo));
        feed.Append(MakeCrdtSet("k", ticks: 2, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 3, PnDelta("A", 2)));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(
            ShippedRecordFor(transport, feed, "solo").Delta,
            Is.SameAs(solo),
            "an unfolded key's delta is never round-tripped through the codec");
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_long_run_folds_to_the_pairwise_result()
    {
        // Six same-key deltas exercise the linear fold well past the two-delta
        // case, over a replica set that is neither disjoint nor monotone in
        // source order. The expectation is computed by the pairwise
        // CombineDeltas fold the shipper used to run.
        var deltas = new[]
        {
            PnDelta("A", 1), PnDelta("B", 2), PnDelta("A", 3),
            PnDelta("C", 4), PnDelta("B", 5), PnDelta("A", 6),
        };

        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        for (var i = 0; i < deltas.Length; i++)
        {
            feed.Append(MakeCrdtSet("k", ticks: i + 1, deltas[i]));
        }

        await grain.PumpForTestingAsync(CancellationToken.None);

        object pairwise = PnShape.DeserializeDelta(deltas[0]);
        for (var i = 1; i < deltas.Length; i++)
        {
            pairwise = PnShape.CombineDeltas!(pairwise, PnShape.DeserializeDelta(deltas[i]));
        }

        Assert.That(
            Increments(ShippedRecordFor(transport, feed, "k")),
            Is.EquivalentTo(((PnCounterDelta)pairwise).Increments),
            "the linear run fold must produce the delta the pairwise fold produced");
    }
}
