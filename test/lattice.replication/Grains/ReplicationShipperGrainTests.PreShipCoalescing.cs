using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Sender-side coverage for pre-ship coalescing: with the feature off the
/// shipper is byte-identical to today's verbatim drain; with it on, a
/// last-writer-wins tree collapses redundant per-key versions down to the
/// highest-HLC entry the receiver would converge to, CRDT-mode trees are
/// never coalesced, non-point and atomic-batch entries are left verbatim,
/// the per-partition resume cursor still accounts for every elided entry,
/// and the entries-elided / bytes-elided counters fire.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    /// <summary>Builds a coalescing-enabled options instance for the test fixture's single-partition feed.</summary>
    private static LatticeReplicationOptions CoalesceOptions(bool enabled = true, int shipBatchSize = 100) =>
        new()
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            ShipBatchSize = shipBatchSize,
            PreShipCoalescingEnabled = enabled,
        };

    /// <summary>A resolver that reports <paramref name="mode"/> for every tree.</summary>
    private static ILatticeMergeModeResolver ResolverFor(LatticeMergeMode mode)
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(mode);
        return resolver;
    }

    private static WalRecord MakeDelete(string key, long ticks)
        => new()
        {
            TreeId = Tree,
            Op = MutationKind.Delete,
            Key = key,
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
            IsTombstone = true,
            OriginClusterId = LocalCluster,
        };

    private static WalRecord MakeZeroHlcSet(string key)
        => new()
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[] { 7 },
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = LocalCluster,
        };

    private static WalRecord MakeRangeDelete(string startKey, string endExclusive)
        => new()
        {
            TreeId = Tree,
            Op = MutationKind.DeleteRange,
            Key = startKey,
            EndExclusiveKey = endExclusive,
            Timestamp = HybridLogicalClock.Zero,
            IsTombstone = true,
            OriginClusterId = LocalCluster,
        };

    private static WalRecord MakePreparedSet(string key, long ticks, Guid txId, int batchSize, int batchIndex)
        => new()
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[] { 9 },
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
            OriginClusterId = LocalCluster,
            IsPrepared = true,
            TransactionId = txId,
            AtomicBatchSize = batchSize,
            AtomicBatchIndex = batchIndex,
        };

    /// <summary>Captures the pre-ship coalescing counters over the meter for the duration of the test.</summary>
    private sealed class CoalesceMetricRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _entriesElided;
        private long _bytesElided;

        public long EntriesElided => Interlocked.Read(ref _entriesElided);
        public long BytesElided => Interlocked.Read(ref _bytesElided);

        public CoalesceMetricRecorder()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == LatticeReplicationMetrics.MeterName
                    && (instrument.Name == LatticeReplicationMetrics.CoalesceEntriesElidedName
                        || instrument.Name == LatticeReplicationMetrics.CoalesceBytesElidedName))
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
            {
                if (instrument.Name == LatticeReplicationMetrics.CoalesceEntriesElidedName)
                {
                    Interlocked.Add(ref _entriesElided, measurement);
                }
                else if (instrument.Name == LatticeReplicationMetrics.CoalesceBytesElidedName)
                {
                    Interlocked.Add(ref _bytesElided, measurement);
                }
            });
            _listener.Start();
        }

        public void Dispose() => _listener.Dispose();
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_disabled_ships_every_version_verbatim()
    {
        var (grain, _, feed, transport, _, _, _) = Create(CoalesceOptions(enabled: false));
        // Same key rewritten three times in one batch, plus a distinct key.
        feed.Append(MakeEntry("hot", ticks: 1));
        feed.Append(MakeEntry("hot", ticks: 2));
        feed.Append(MakeEntry("hot", ticks: 3));
        feed.Append(MakeEntry("other", ticks: 4));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(4),
            "with coalescing off the shipper ships every drained version verbatim");
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_enabled_keeps_only_highest_hlc_per_key()
    {
        var (grain, _, feed, transport, _, _, _) = Create(CoalesceOptions());
        feed.Append(MakeEntry("hot", ticks: 1));
        feed.Append(MakeEntry("hot", ticks: 2));
        feed.Append(MakeEntry("hot", ticks: 3));
        feed.Append(MakeEntry("other", ticks: 4));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "coalescing collapses the three 'hot' versions to the highest-HLC one, keeping 'other'");
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_enabled_does_not_elide_distinct_keys()
    {
        var (grain, _, feed, transport, _, _, _) = Create(CoalesceOptions());
        feed.Append(MakeEntry("a", ticks: 1));
        feed.Append(MakeEntry("b", ticks: 2));
        feed.Append(MakeEntry("c", ticks: 3));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(3),
            "three distinct keys have nothing to coalesce; all three ship");
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_enabled_later_delete_supersedes_earlier_set()
    {
        var (grain, _, feed, transport, _, _, _) = Create(CoalesceOptions());
        feed.Append(MakeEntry("k", ticks: 1));
        feed.Append(MakeEntry("k", ticks: 2));
        feed.Append(MakeDelete("k", ticks: 3));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(1),
            "the highest-HLC point write for the key is the delete, so only it survives");
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_never_coalesces_even_when_enabled()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.OrSet));
        feed.Append(MakeEntry("hot", ticks: 1));
        feed.Append(MakeEntry("hot", ticks: 2));
        feed.Append(MakeEntry("hot", ticks: 3));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(3),
            "CRDT-mode trees apply via delta merge; coalescing must leave every version verbatim");
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_enabled_never_elides_range_delete()
    {
        var (grain, _, feed, transport, _, _, _) = Create(CoalesceOptions());
        feed.Append(MakeEntry("k", ticks: 1));
        feed.Append(MakeEntry("k", ticks: 2));
        feed.Append(MakeRangeDelete("a", "z"));

        await grain.PumpForTestingAsync(CancellationToken.None);

        // The two 'k' sets collapse to one; the range delete is never a
        // coalescing candidate and ships verbatim.
        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "range deletes are non-point mutations and are never elided");
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_enabled_never_elides_zero_hlc_entries()
    {
        var (grain, _, feed, transport, _, _, _) = Create(CoalesceOptions());
        feed.Append(MakeZeroHlcSet("k"));
        feed.Append(MakeEntry("k", ticks: 2));

        await grain.PumpForTestingAsync(CancellationToken.None);

        // Without the zero-HLC guard the zero entry (lower order) would be
        // elided in favour of the real-HLC one; the guard keeps it verbatim.
        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "entries carrying HybridLogicalClock.Zero are never coalescing candidates");
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_enabled_never_elides_prepared_atomic_entries()
    {
        var (grain, _, feed, transport, _, _, _) = Create(CoalesceOptions());
        var txId = Guid.NewGuid();
        feed.Append(MakePreparedSet("k", ticks: 1, txId, batchSize: 2, batchIndex: 0));
        feed.Append(MakePreparedSet("k", ticks: 2, txId, batchSize: 2, batchIndex: 1));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "saga prepare-phase entries are never coalesced across the atomic-batch boundary");
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_enabled_advances_cursor_past_every_elided_entry()
    {
        var (grain, state, feed, transport, _, _, _) = Create(CoalesceOptions());
        var ackHlc = new HybridLogicalClock { WallClockTicks = 3 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = ackHlc });
        // Four WAL entries appended at sequence 0..3; three collapse to one.
        feed.Append(MakeEntry("hot", ticks: 1));
        feed.Append(MakeEntry("hot", ticks: 2));
        feed.Append(MakeEntry("hot", ticks: 3));
        feed.Append(MakeEntry("other", ticks: 4));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
                "coalescing collapsed the three 'hot' versions to one");
            Assert.That(state.State.PartitionCursors, Contains.Key(0));
            Assert.That(state.State.PartitionCursors[0], Is.EqualTo(4L),
                "the cursor must advance past every consumed sequence - elided entries included - so nothing is re-shipped or stranded");
        });
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_enabled_increments_elided_counters()
    {
        var (grain, _, feed, transport, _, _, _) = Create(CoalesceOptions());
        feed.Append(MakeEntry("hot", ticks: 1));
        feed.Append(MakeEntry("hot", ticks: 2));
        feed.Append(MakeEntry("hot", ticks: 3));
        feed.Append(MakeEntry("other", ticks: 4));

        using var recorder = new CoalesceMetricRecorder();
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(recorder.EntriesElided, Is.EqualTo(2),
                "two redundant 'hot' versions were elided");
            Assert.That(recorder.BytesElided, Is.GreaterThan(0),
                "the bytes-elided counter accumulates the wire-segment lengths of the dropped entries");
        });
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_enabled_no_redundancy_does_not_increment_counter()
    {
        var (grain, _, feed, transport, _, _, _) = Create(CoalesceOptions());
        feed.Append(MakeEntry("a", ticks: 1));
        feed.Append(MakeEntry("b", ticks: 2));

        using var recorder = new CoalesceMetricRecorder();
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(recorder.EntriesElided, Is.EqualTo(0),
            "with no redundant per-key versions the counter never fires");
    }

    [Test]
    public async Task PumpOnceAsync_with_coalescing_disabled_does_not_increment_counter()
    {
        var (grain, _, feed, transport, _, _, _) = Create(CoalesceOptions(enabled: false));
        feed.Append(MakeEntry("hot", ticks: 1));
        feed.Append(MakeEntry("hot", ticks: 2));
        feed.Append(MakeEntry("hot", ticks: 3));

        using var recorder = new CoalesceMetricRecorder();
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(3),
                "the default-off path is byte-identical: every version ships");
            Assert.That(recorder.EntriesElided, Is.EqualTo(0),
                "no coalescing pass runs while the option is off");
        });
    }
}
