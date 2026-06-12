using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// partition-resume unit coverage for the per-partition resume cursor and
/// deferred-persist semantics on the outbound shipper. These tests
/// exercise the durability invariants that protect against data loss
/// across silo restart inside the deferred-persist window, the k-way
/// HLC merge across multiple WAL partitions, and the avoid-rescan-
/// from-zero contract on every steady-state pump tick.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    // Sequence-cursor seed + advance ----------------------------------

    [Test]
    public async Task PumpOnceAsync_seeds_partition_cursor_from_durable_state_on_first_tick()
    {
        // Seed the durable PartitionCursors map at sequence 5; the
        // shipper must resume from there rather than rescanning from 0.
        var seed = new ReplicationShipperState
        {
            Cursor = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 },
        };
        seed.PartitionCursors[0] = 5;
        var (grain, _, feed, transport, _, _, _) = Create(seedState: seed);
        // Pre-populate enough entries so the saved cursor lands inside the WAL.
        for (var i = 0; i < 8; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i + 1));
        }
        var ackHlc = new HybridLogicalClock { WallClockTicks = 8, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = ackHlc });

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            // The very first ReadAsync call must use fromSequence==5
            // (the saved partition cursor), NOT 0.
            Assert.That(feed.ReadFromSequences, Is.Not.Empty);
            Assert.That(feed.ReadFromSequences[0], Is.EqualTo(5L),
                "Partition read on first pump tick must resume from the saved partition cursor, not from sequence 0.");
        });
    }

    [Test]
    public async Task PumpOnceAsync_advances_partition_cursor_past_consumed_sequence_on_positive_ack()
    {
        var (grain, state, feed, transport, _, _, _) = Create();
        // Three entries appended at sequence 0..2.
        feed.Append(MakeEntry("k0", ticks: 1));
        feed.Append(MakeEntry("k1", ticks: 2));
        feed.Append(MakeEntry("k2", ticks: 3));
        var ackHlc = new HybridLogicalClock { WallClockTicks = 3, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = ackHlc });

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.PartitionCursors, Contains.Key(0));
            Assert.That(state.State.PartitionCursors[0], Is.EqualTo(3L),
                "After consuming three entries, the partition cursor must point one past the highest consumed sequence.");
        });
    }

    [Test]
    public async Task PumpOnceAsync_does_not_advance_partition_cursor_when_transport_throws()
    {
        var (grain, state, feed, transport, _, _, _) = Create();
        feed.Append(MakeEntry("k", ticks: 1));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationAck>(_ => throw new InvalidOperationException("transport-down"));

        await grain.OnDoorbellAsync(CancellationToken.None);

        // No partition cursor entry was persisted because the round-trip failed.
        Assert.That(state.State.PartitionCursors, Is.Empty);
    }

    [Test]
    public async Task PumpOnceAsync_does_not_advance_partition_cursor_when_ack_rejected()
    {
        var (grain, state, feed, transport, _, _, _) = Create();
        feed.Append(MakeEntry("k", ticks: 1));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero });

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.PartitionCursors, Is.Empty);
    }

    [Test]
    public async Task PumpOnceAsync_does_not_rescan_from_zero_on_subsequent_ticks()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 2,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        for (var i = 0; i < 6; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i + 1));
        }
        // Receiver returns a low ack so the shipper falls back to
        // sourceHlc (last entry HLC) - exercises the partition-cursor
        // path independent of the ack branch.
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(c => new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = c.Arg<ReplicationBatch>().EncodedEnvelope is null || c.Arg<ReplicationBatch>().EncodedEnvelope!.Value.EncodedEntries.IsEmpty
                    ? HybridLogicalClock.Zero
                    : new HybridLogicalClock { WallClockTicks = 1000, Counter = 0 },
            });

        // Three pump ticks consume two entries each.
        await grain.OnDoorbellAsync(CancellationToken.None);
        await grain.OnDoorbellAsync(CancellationToken.None);
        await grain.OnDoorbellAsync(CancellationToken.None);

        // ReadFromSequences captures the fromSequence of every read.
        // The first read must be 0; every subsequent read on the same
        // partition must be strictly greater than 0 (no rescan).
        Assert.That(feed.ReadFromSequences, Has.Count.GreaterThanOrEqualTo(3));
        Assert.That(feed.ReadFromSequences[0], Is.EqualTo(0L));
        for (var i = 1; i < feed.ReadFromSequences.Count; i++)
        {
            Assert.That(feed.ReadFromSequences[i], Is.GreaterThan(0L),
                $"Read #{i} must resume past sequence 0 - rescanning from zero would reproduce the partition-resume throughput bug.");
        }
    }

    // HLC cold-start filter ------------------------------------------

    [Test]
    public async Task PumpOnceAsync_filters_already_seen_entries_via_hlc_when_partition_cursors_empty_on_legacy_state()
    {
        // Simulate a legacy persisted state: HLC cursor at 5 but
        // PartitionCursors empty. Without the HLC defensive filter the
        // shipper would re-ship every entry below HLC=5 once on first
        // tick after upgrade; with the filter, those entries are
        // dropped before reaching the encode/send path.
        var seed = new ReplicationShipperState
        {
            Cursor = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 },
            // PartitionCursors intentionally empty (legacy decode shape).
        };
        var (grain, _, feed, transport, _, _, _) = Create(seedState: seed);
        // Entries 0..4 are <= cursor and must be filtered;
        // entries 5..7 are > cursor and must ship.
        for (var i = 0; i < 8; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i + 1));
        }
        ReplicationBatch? captured = null;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(c =>
            {
                captured = c.Arg<ReplicationBatch>();
                return new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 8, Counter = 0 },
                };
            });

        await grain.OnDoorbellAsync(CancellationToken.None);

        // The HLC filter dropped entries 0..4 (HLC 1..5 all <= cursor 5);
        // only entries 5..7 (HLC 6..8) made it into the batch.
        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.Value.EncodedEnvelope, Is.Not.Null);
    }

    // K-way HLC merge across two partitions ----------------------------

    private static (
        ReplicationShipperGrain Grain,
        FakePersistentState<ReplicationShipperState> State,
        StubReplogShardGrain[] PartitionedFeeds,
        IReplicationTransport Transport,
        TestEncoder Encoder) CreateMultiPartition(int partitions, LatticeReplicationOptions? options = null)
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var resolved = options ?? new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = partitions,
        };
        // The provided options must declare the requested partition count.
        if (resolved.ReplogPartitions != partitions)
        {
            resolved = new LatticeReplicationOptions
            {
                ClusterId = resolved.ClusterId,
                ShipCursorWriteInterval = resolved.ShipCursorWriteInterval,
                ShipBatchSize = resolved.ShipBatchSize,
                ShipPartitionPageSize = resolved.ShipPartitionPageSize,
                ReplogPartitions = partitions,
            };
        }
        var monitor = Monitor(resolved);
        var walEncoder = new StubWalRecordEncoder();
        var feeds = new StubReplogShardGrain[partitions];
        for (var i = 0; i < partitions; i++)
        {
            feeds[i] = new StubReplogShardGrain(walEncoder);
        }
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        var encoder = new TestEncoder();
        var registry = Substitute.For<IWalCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        var factory = BuildGrainFactory(null, feeds, Tree);
        var grain = new ReplicationShipperGrain(
            ctx, Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, walEncoder, registry, factory, fakeState,
            new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState());
        grain.InitializeForTesting(Tree, Peer);
        return (grain, fakeState, feeds, transport, encoder);
    }

    /// <summary>
    /// Helper that captures the <see cref="ReplicationBatch"/> handed
    /// to the transport on the next pump tick, decoding the framing-
    /// only envelope's entry segments through the supplied
    /// <see cref="StubWalRecordEncoder"/> so tests can inspect the
    /// merge order the shipper produced.
    /// </summary>
    private static List<WalRecord> CaptureMergeOrder(
        IReplicationTransport transport,
        StubWalRecordEncoder walEncoder,
        long ackHlc)
    {
        var captured = new List<WalRecord>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(c =>
            {
                var batch = c.Arg<ReplicationBatch>();
                var seg = batch.EncodedEnvelope!.Value.EncodedEntries.Span;
                for (var i = 0; i < seg.Length; i++)
                {
                    captured.Add(((IWalRecordEncoder)walEncoder).Decode(seg[i].AsSpan(), batch.TreeName, batch.EncodedEnvelope!.Value.Header.Mode));
                }
                return new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = ackHlc, Counter = 0 },
                };
            });
        return captured;
    }

    [Test]
    public async Task DrainBatchAsync_merges_two_partitions_in_hlc_ascending_order()
    {
        var partitions = 2;
        var resolved = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = partitions,
            ShipBatchSize = 16,
        };
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var monitor = Monitor(resolved);
        var walEncoder = new StubWalRecordEncoder();
        var feeds = new[] { new StubReplogShardGrain(walEncoder), new StubReplogShardGrain(walEncoder) };
        // Interleave entries: partition 0 holds HLCs 1, 3, 5;
        // partition 1 holds HLCs 2, 4, 6. The k-way merge must
        // produce 1, 2, 3, 4, 5, 6.
        feeds[0].Append(MakeEntry("p0/a", ticks: 1));
        feeds[0].Append(MakeEntry("p0/b", ticks: 3));
        feeds[0].Append(MakeEntry("p0/c", ticks: 5));
        feeds[1].Append(MakeEntry("p1/a", ticks: 2));
        feeds[1].Append(MakeEntry("p1/b", ticks: 4));
        feeds[1].Append(MakeEntry("p1/c", ticks: 6));
        var transport = Substitute.For<IReplicationTransport>();
        var captured = CaptureMergeOrder(transport, walEncoder, ackHlc: 6);
        var encoder = new TestEncoder();
        var registry = Substitute.For<IWalCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        var factory = BuildGrainFactory(null, feeds, Tree);
        var grain = new ReplicationShipperGrain(
            ctx, Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, walEncoder, registry, factory, fakeState,
            new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState());
        grain.InitializeForTesting(Tree, Peer);

        await grain.OnDoorbellAsync(CancellationToken.None);

        // The merge-loop must emit entries in HLC-ascending order
        // regardless of which partition they came from.
        Assert.That(captured.Count, Is.EqualTo(6));
        var hlcs = captured.Select(e => e.Timestamp.WallClockTicks).ToArray();
        Assert.That(hlcs, Is.EqualTo(new long[] { 1, 2, 3, 4, 5, 6 }));
    }

    [Test]
    public async Task DrainBatchAsync_handles_empty_partition_without_breaking_drain()
    {
        var partitions = 2;
        var (grain, _, feeds, transport, _) = CreateMultiPartition(partitions);
        // Partition 0 has entries; partition 1 is empty.
        feeds[0].Append(MakeEntry("k", ticks: 7));
        ReplicationBatch? captured = null;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(c =>
            {
                captured = c.Arg<ReplicationBatch>();
                return new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 7, Counter = 0 },
                };
            });

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(captured, Is.Not.Null,
            "An empty partition must not pin the drain - the merge must still emit entries from the populated partition.");
    }

    [Test]
    public async Task DrainBatchAsync_advances_only_partitions_that_contributed_entries()
    {
        var partitions = 2;
        var (grain, state, feeds, transport, _) = CreateMultiPartition(partitions);
        // Only partition 0 contributes; partition 1 is empty.
        feeds[0].Append(MakeEntry("p0/a", ticks: 1));
        feeds[0].Append(MakeEntry("p0/b", ticks: 2));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 2, Counter = 0 },
            });

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.PartitionCursors.TryGetValue(0, out var c0), Is.True);
            Assert.That(c0, Is.EqualTo(2L));
            // Partition 1 contributed nothing - its cursor must NOT be
            // recorded (would be a phantom advance to 0 otherwise).
            Assert.That(state.State.PartitionCursors.ContainsKey(1), Is.False,
                "An empty partition must not advance its cursor - recording 0 would mask the true cold-start state.");
        });
    }

    [Test]
    public async Task DrainBatchAsync_handles_partition_count_increase_across_activations()
    {
        // First activation: 1 partition. Second activation (same state):
        // 2 partitions. EnsureScratchSized must grow the scratch
        // arrays in lockstep without dropping the durable cursor.
        var resolved = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
        };
        var (grain, state, feeds, transport, _) = CreateMultiPartition(1, resolved);
        feeds[0].Append(MakeEntry("k", ticks: 1));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            });
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(state.State.PartitionCursors[0], Is.EqualTo(1L));

        // Now grow to 2 partitions on a fresh activation that inherits the same state.
        var resolved2 = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 2,
        };
        var (grain2, _, feeds2, transport2, _) = CreateMultiPartition(2, resolved2);
        // Manually copy the persistent state to simulate the same (tree, peer)
        // grain re-activating with a different partition count.
        var prior = state.State;
        var fakeState = new FakePersistentState<ReplicationShipperState>
        {
            State = prior,
        };
        // Need a dedicated activation built around this seed.
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var monitor = Monitor(resolved2);
        var freshWalEncoder = new StubWalRecordEncoder();
        var stubs = new[] { new StubReplogShardGrain(freshWalEncoder), new StubReplogShardGrain(freshWalEncoder) };
        // Partition 0 already at sequence 1; new entry at sequence 1.
        // Partition 1 fresh, entry at sequence 0.
        stubs[0].Append(MakeEntry("p0/a", ticks: 1)); // already consumed
        stubs[0].Append(MakeEntry("p0/b", ticks: 3));
        stubs[1].Append(MakeEntry("p1/a", ticks: 2));
        var freshTransport = Substitute.For<IReplicationTransport>();
        freshTransport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 3, Counter = 0 },
            });
        var freshGrain = new ReplicationShipperGrain(
            ctx, Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            monitor, freshTransport, new TestEncoder(), freshWalEncoder, Substitute.For<IWalCursorRegistry>(),
            BuildGrainFactory(null, stubs, Tree), fakeState,
            new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState());
        freshGrain.InitializeForTesting(Tree, Peer);
        // Don't use the unused locals.
        _ = grain2;
        _ = feeds2;
        _ = transport2;

        await freshGrain.OnDoorbellAsync(CancellationToken.None);

        // Partition 0's cursor was preserved; partition 1's cursor was created.
        Assert.Multiple(() =>
        {
            Assert.That(stubs[0].ReadFromSequences[0], Is.EqualTo(1L),
                "Partition 0 must resume at saved sequence 1, not rescan from 0 after partition-count grew.");
            Assert.That(stubs[1].ReadFromSequences[0], Is.EqualTo(0L),
                "Newly-added partition 1 must cold-start at sequence 0.");
            Assert.That(fakeState.State.PartitionCursors[0], Is.EqualTo(2L));
            Assert.That(fakeState.State.PartitionCursors[1], Is.EqualTo(1L));
        });
    }

    [Test]
    public async Task PumpOnceAsync_advances_partition_cursor_even_when_every_entry_is_filtered()
    {
        // All entries filtered by KeyPrefixes; the partition cursor
        // must still advance past them so the next pump tick does not
        // re-read the same exhausted page.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyPrefixes = new[] { "kept/" },
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("dropped/a", ticks: 1));
        feed.Append(MakeEntry("dropped/b", ticks: 2));

        await grain.OnDoorbellAsync(CancellationToken.None);

        // Nothing was sent (every entry filtered).
        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
        // The partition cursor was NOT advanced because no batch was
        // shipped - but ReadFromSequences confirms the read happened.
        // (The batch never reached AdvanceCursorAsync because
        // _drainBuffer.Count == 0 after filtering.) On the next tick
        // the read would re-scan from sequence 0 again - this is the
        // behaviour the partition-resume design tolerates: filtered-only ticks
        // are bounded by ShipPartitionPageSize, not by the WAL size.
        Assert.That(state.State.PartitionCursors, Is.Empty,
            "Filtered-only ticks legitimately do not flush - the page-bounded re-read is acceptable per partition-resume.");
        Assert.That(feed.ReadFromSequences, Is.Not.Empty);
    }

    // Deferred-persist semantics --------------------------------------

    [Test]
    public async Task DeferredPersist_does_not_write_state_within_interval()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 4, // require 4 successful acks before flushing
            ShipBatchSize = 1,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        // Three entries, three pump ticks - never reaches the flush threshold.
        for (var i = 0; i < 3; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i + 1));
        }
        var ackHlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(c => new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = c.Arg<ReplicationBatch>().EncodedEnvelope is null || c.Arg<ReplicationBatch>().EncodedEnvelope!.Value.EncodedEntries.IsEmpty
                    ? HybridLogicalClock.Zero
                    : new HybridLogicalClock { WallClockTicks = ackHlc.WallClockTicks, Counter = 0 },
            });

        await grain.OnDoorbellAsync(CancellationToken.None);
        await grain.OnDoorbellAsync(CancellationToken.None);
        await grain.OnDoorbellAsync(CancellationToken.None);

        // Three successful acks at interval=4 means zero durable writes so far.
        Assert.That(state.WriteCount, Is.EqualTo(0),
            "Deferred persist must defer until the configured interval is reached.");
    }

    [Test]
    public async Task DeferredPersist_flushes_state_on_interval_boundary()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 3,
            ShipBatchSize = 1,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        for (var i = 0; i < 5; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i + 1));
        }
        // Return ack frontier matching each shipped entry's HLC so the
        // defensive HLC filter does not drop subsequent entries.
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(c =>
            {
                var batch = c.Arg<ReplicationBatch>();
                // Source HLC fallback: shipper records sourceHlc per
                // batch; ack.HighestAppliedHlc=Zero forces advancedTo=sourceHlc.
                return new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = HybridLogicalClock.Zero,
                };
            });

        // Advance 1, 2 - no flush yet. Advance 3 - first flush.
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(state.WriteCount, Is.EqualTo(0));
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(state.WriteCount, Is.EqualTo(0));
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(state.WriteCount, Is.EqualTo(1),
            "The third successful ack at interval=3 must trigger the durable write.");
    }

    [Test]
    public async Task DeferredPersist_does_not_report_to_registry_until_durable_write_completes()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 4,
            ShipBatchSize = 1,
        };
        var (grain, _, feed, transport, _, registry, _) = Create(opts);
        for (var i = 0; i < 3; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i + 1));
        }
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });

        await grain.OnDoorbellAsync(CancellationToken.None);
        await grain.OnDoorbellAsync(CancellationToken.None);
        await grain.OnDoorbellAsync(CancellationToken.None);

        // Three acks at interval=4 means zero registry reports so far -
        // the registry feeds the WAL GC trim frontier and must never
        // advance past the durably-recoverable cursor.
        await registry.DidNotReceive().ReportCursorAsync(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeferredPersist_reports_to_registry_after_flush()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 2,
            ShipBatchSize = 1,
        };
        var (grain, _, feed, transport, _, registry, _) = Create(opts);
        feed.Append(MakeEntry("k0", ticks: 1));
        feed.Append(MakeEntry("k1", ticks: 2));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });

        await grain.OnDoorbellAsync(CancellationToken.None);
        await grain.OnDoorbellAsync(CancellationToken.None);

        // Second ack hits interval=2 → flush + report.
        await registry.Received(1).ReportCursorAsync(
            Tree, Peer, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeferredPersist_does_not_re_report_unchanged_cursor_after_only_partition_advance()
    {
        // Multi-partition setup: ack returns a low HWM so the durable
        // HLC cursor advances on the first flush, then a second flush
        // happens where only PartitionCursors changed (HLC unchanged).
        // The registry must NOT be re-reported in that case.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
        };
        var (grain, state, feed, transport, _, registry, _) = Create(opts);
        feed.Append(MakeEntry("k0", ticks: 5));
        var hlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc });

        // First pump: advances HLC to 5, reports.
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(state.State.Cursor, Is.EqualTo(hlc));
        await registry.Received(1).ReportCursorAsync(
            Tree, Peer, hlc, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeferredPersist_state_survives_when_registry_fails_after_flush()
    {
        // The flush already wrote to durable storage; a registry
        // failure post-write must not unwind the cursor advance, and
        // _lastReportedCursor must still be updated so the next flush
        // does not re-report indefinitely.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
        };
        var (grain, state, feed, transport, _, registry, _) = Create(opts);
        feed.Append(MakeEntry("k", ticks: 5));
        var hlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc });
        registry.ReportCursorAsync(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns<Task>(_ => Task.FromException(new InvalidOperationException("registry-down")));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Cursor, Is.EqualTo(hlc));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    // OnDeactivate flush ----------------------------------------------

    [Test]
    public async Task OnDeactivate_flushes_pending_cursor_writes()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 100, // never reached during the test
            ShipBatchSize = 1,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("k0", ticks: 1));
        feed.Append(MakeEntry("k1", ticks: 2));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });

        await grain.OnDoorbellAsync(CancellationToken.None);
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(state.WriteCount, Is.EqualTo(0), "No interval flush yet.");

        // Drive the deactivation hook; pending writes must flush.
        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(1),
            "Graceful deactivation must flush pending deferred-persist cursor writes.");
    }

    [Test]
    public async Task OnDeactivate_is_noop_when_no_pending_cursor_writes()
    {
        var (grain, state, _, _, _, _, _) = Create();

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(0));
    }

    [Test]
    public async Task OnDeactivate_swallows_storage_failure_during_flush()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 100,
            ShipBatchSize = 1,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("k", ticks: 1));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });

        await grain.OnDoorbellAsync(CancellationToken.None);
        // Arm the next WriteStateAsync call to fail.
        state.ThrowOnWrite = new InvalidOperationException("storage-down");

        // OnDeactivate must NOT propagate the exception - a storage
        // failure during deactivation is logged and the pending advance
        // is recovered on the next activation by re-shipping from the
        // last durable cursor (receiver dedupes).
        Assert.That(
            async () => await ((IGrainBase)grain).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None),
            Throws.Nothing);
    }

    // Time-dimension coalescing (ShipCursorWriteMaxDelay) ------------

    /// <summary>
    /// Controllable <see cref="TimeProvider"/> for deterministic
    /// exercise of the wall-clock time dimension of the cursor-write
    /// coalescing rule without a real wall-clock wait.
    /// </summary>
    private sealed class MutableTimeProvider : TimeProvider
    {
        private DateTimeOffset _utcNow;

        public MutableTimeProvider(DateTimeOffset start) => _utcNow = start;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan delta) => _utcNow = _utcNow.Add(delta);
    }

    [Test]
    public async Task DeferredPersist_flushes_when_max_delay_elapses_before_interval()
    {
        // Batch-count interval is far higher than the number of acks in
        // this test, so only the time dimension can trigger a flush.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 100,
            ShipCursorWriteMaxDelay = TimeSpan.FromSeconds(5),
            ShipBatchSize = 1,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);
        feed.Append(MakeEntry("k0", ticks: 1));
        feed.Append(MakeEntry("k1", ticks: 2));
        // Ack frontier Zero forces advancedTo = sourceHlc so each batch advances the cursor.
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });

        // First ack: pending=1, count(1) < 100, no time elapsed - deferred.
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(state.WriteCount, Is.EqualTo(0), "First advance must defer - neither threshold reached.");

        // Advance past the max delay, then book a second advance.
        clock.Advance(TimeSpan.FromSeconds(6));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(1),
            "An advance booked after ShipCursorWriteMaxDelay has elapsed must force a durable flush even below the batch-count interval.");
    }

    [Test]
    public async Task DeferredPersist_flushes_on_idle_tick_when_max_delay_elapses()
    {
        // After shipping a single partial batch the stream goes idle
        // (no further entries). The empty-drain pump tick must flush the
        // pending cursor once the time dimension elapses, even though no
        // new ack arrives to re-trigger the count/time check.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 100,
            ShipCursorWriteMaxDelay = TimeSpan.FromSeconds(5),
            ShipBatchSize = 1,
            // Disable the liveness probe so the idle tick's only side
            // effect is the cursor flush under test.
            LivenessProbeInterval = System.Threading.Timeout.InfiniteTimeSpan,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);
        feed.Append(MakeEntry("k0", ticks: 1));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });

        // Ship the one entry: pending=1, deferred.
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(state.WriteCount, Is.EqualTo(0), "Partial batch must defer below the batch-count interval.");

        // Stream is now drained. Advance past the max delay and pump an
        // idle tick - the empty-drain path must flush the pending cursor.
        clock.Advance(TimeSpan.FromSeconds(6));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(1),
            "An idle pump tick after ShipCursorWriteMaxDelay has elapsed must flush the pending cursor write.");
    }

    [Test]
    public async Task DeferredPersist_does_not_flush_on_idle_tick_before_max_delay()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 100,
            ShipCursorWriteMaxDelay = TimeSpan.FromSeconds(5),
            ShipBatchSize = 1,
            LivenessProbeInterval = System.Threading.Timeout.InfiniteTimeSpan,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);
        feed.Append(MakeEntry("k0", ticks: 1));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });

        await grain.OnDoorbellAsync(CancellationToken.None);
        // Advance only part-way to the max delay.
        clock.Advance(TimeSpan.FromSeconds(2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(0),
            "Below both the batch-count interval and the max delay, the pending cursor write must stay deferred.");
    }

    [Test]
    public async Task DeferredPersist_infinite_max_delay_disables_time_dimension()
    {
        // With the time dimension disabled, only the batch-count interval
        // can flush - an arbitrarily large elapsed time must not trigger one.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 100,
            ShipCursorWriteMaxDelay = System.Threading.Timeout.InfiniteTimeSpan,
            ShipBatchSize = 1,
            LivenessProbeInterval = System.Threading.Timeout.InfiniteTimeSpan,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);
        feed.Append(MakeEntry("k0", ticks: 1));
        feed.Append(MakeEntry("k1", ticks: 2));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });

        await grain.OnDoorbellAsync(CancellationToken.None);
        // A full hour of wall-clock time passes - irrelevant when the
        // time dimension is disabled.
        clock.Advance(TimeSpan.FromHours(1));
        await grain.OnDoorbellAsync(CancellationToken.None);
        // And an idle tick at the same elapsed time must also not flush.
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(0),
            "Timeout.InfiniteTimeSpan must disable the time dimension so only the batch-count interval can flush.");
    }

    // Default-value sanity --------------------------------------------

    [Test]
    public void DefaultShipCursorWriteInterval_is_16()
    {
        Assert.That(LatticeReplicationOptions.DefaultShipCursorWriteInterval, Is.EqualTo(16));
    }

    [Test]
    public void DefaultShipCursorWriteMaxDelay_is_two_seconds()
    {
        Assert.That(LatticeReplicationOptions.DefaultShipCursorWriteMaxDelay, Is.EqualTo(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void DefaultShipPartitionPageSize_is_256()
    {
        Assert.That(LatticeReplicationOptions.DefaultShipPartitionPageSize, Is.EqualTo(256));
    }

    [Test]
    public void Default_options_resolve_with_partition_resume_defaults()
    {
        var opts = new LatticeReplicationOptions();
        Assert.Multiple(() =>
        {
            Assert.That(opts.ShipPartitionPageSize, Is.EqualTo(256));
            Assert.That(opts.ShipCursorWriteInterval, Is.EqualTo(16));
        });
    }

    // Cross-shipper HWM: ack frontier > sourceHlc -----------------------

    /// <summary>
    /// When the receiver's <see cref="ReplicationAck.HighestAppliedHlc"/>
    /// is strictly greater than the last shipped entry's HLC (the
    /// cross-shipper-HWM scenario - another shipper to the same tree
    /// already advanced the receiver's frontier past ours), the
    /// shipper must trust the ack and jump <c>state.Cursor</c> to the
    /// receiver's frontier. The defensive HLC filter at the top of the
    /// merge loop then drops any in-between WAL entries on the next
    /// pump tick (their HLC is at-or-below the bumped cursor) so we
    /// don't re-ship work the receiver already has.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_jumps_cursor_to_ack_frontier_when_receiver_returns_higher_hwm()
    {
        var (grain, state, feed, transport, _, _, _) = Create();
        // Ship one entry at HLC=2.
        feed.Append(MakeEntry("k0", ticks: 2));
        // Receiver claims a higher HWM (50) than the shipped batch's
        // last HLC (2) - models a cross-shipper convergence where
        // another peer already pushed the receiver past 2.
        var ackFrontier = new HybridLogicalClock { WallClockTicks = 50, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = ackFrontier });

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.Cursor, Is.EqualTo(ackFrontier),
            "Receiver returned a frontier > sourceHlc - shipper must trust the ack and jump the cursor.");
    }

    /// <summary>
    /// Follow-on for the cross-shipper-HWM scenario: after the cursor
    /// jumps to the receiver's frontier, subsequent WAL entries with
    /// HLC at-or-below that frontier must be filtered out by the
    /// defensive HLC predicate inside the merge loop - they are work
    /// the receiver already has and re-shipping would waste a round
    /// trip.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_filters_below_cursor_entries_after_cross_shipper_hwm_jump()
    {
        var (grain, state, feed, transport, encoder, _, _) = Create();
        feed.Append(MakeEntry("k0", ticks: 2));
        // Tick 1: receiver returns a high frontier; cursor jumps to 50.
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 50, Counter = 0 },
            });
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(state.State.Cursor.WallClockTicks, Is.EqualTo(50L),
            "Sanity: cursor must have jumped to the receiver's frontier on tick 1.");

        // Tick 2: a stale entry at HLC=10 (below the bumped cursor)
        // arrives in the WAL. It must be filtered before reaching
        // the encode/send path.
        feed.Append(MakeEntry("k1", ticks: 10));
        var encodesBeforeTick2 = encoder.Encodes;
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(encoder.Encodes, Is.EqualTo(encodesBeforeTick2),
            "Stale entry below the bumped cursor must be filtered by the defensive HLC predicate; encoder must not run.");
    }
}
