using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Pre-ship CRDT delta-merge coalescing tests: same-key CRDT deltas within a
/// single origin run are combined into one effect-equivalent delta rather than
/// dropped, the per-primitive combine is verified effect-equivalent to
/// sequential apply, and atomic-batch / opaque-delta / default-off fallbacks
/// ship every version verbatim.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    // ── CRDT delta-merge coalescing ──────────────────────────────────────────

    /// <summary>Closed-shape descriptor reused to author typed delta bytes for the CRDT tests.</summary>
    private static readonly CrdtShape PnShape = CrdtShape.ForPnCounter();

    /// <summary>OR-Map descriptor (string key, PN-Counter value) reused to author typed OR-Map delta bytes.</summary>
    private static readonly CrdtShape OrMapShape = CrdtShape.ForOrMap<string, PnCounter>();

    /// <summary>Authors the Orleans-serialised bytes of an OR-Map add delta carrying a single dot-tagged PN-Counter snapshot.</summary>
    private static byte[] OrMapAddDelta(string key, string replicaId, long counter, string incReplica, long amount)
    {
        var value = new PnCounter();
        value.Increment(incReplica, amount);
        return OrMapShape.SerializeDelta!(new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = key, ReplicaId = replicaId, Counter = counter, Value = value } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        });
    }

    /// <summary>A registry with the test tree's OR-Map shape registered so the shipper resolves a combiner for it.</summary>
    private static CrdtShapeRegistry RegisteredOrMapRegistry()
    {
        var registry = new CrdtShapeRegistry();
        registry.Register(Tree, CrdtShape.ForOrMap<string, PnCounter>());
        return registry;
    }

    /// <summary>Authors the Orleans-serialised bytes of a PN-Counter increment delta for one replica.</summary>
    private static byte[] PnDelta(string replica, long cumulativeIncrement)
        => PnShape.SerializeDelta!(new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(StringComparer.Ordinal) { [replica] = cumulativeIncrement },
            Decrements = new Dictionary<string, long>(StringComparer.Ordinal),
        });

    /// <summary>A point CRDT Set carrying a typed delta (and no value, mirroring the stripped wire shape).</summary>
    private static WalRecord MakeCrdtSet(string key, long ticks, byte[]? delta)
        => new()
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
            OriginClusterId = LocalCluster,
            Delta = delta,
        };

    /// <summary>A prepared (atomic-batch) CRDT Set carrying a typed delta.</summary>
    private static WalRecord MakePreparedCrdtSet(string key, long ticks, byte[] delta, Guid txId, int batchSize, int batchIndex)
        => new()
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
            OriginClusterId = LocalCluster,
            Delta = delta,
            IsPrepared = true,
            TransactionId = txId,
            AtomicBatchSize = batchSize,
            AtomicBatchIndex = batchIndex,
        };

    /// <summary>Captures the CRDT delta-merge / elided counters over the meter for the duration of the test.</summary>
    private sealed class CrdtCoalesceMetricRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _deltasMerged;
        private long _entriesElided;

        public long DeltasMerged => Interlocked.Read(ref _deltasMerged);
        public long EntriesElided => Interlocked.Read(ref _entriesElided);

        public CrdtCoalesceMetricRecorder()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == LatticeReplicationMetrics.MeterName
                    && (instrument.Name == LatticeReplicationMetrics.CoalesceDeltasMergedName
                        || instrument.Name == LatticeReplicationMetrics.CoalesceEntriesElidedName))
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
            {
                if (instrument.Name == LatticeReplicationMetrics.CoalesceDeltasMergedName)
                {
                    Interlocked.Add(ref _deltasMerged, measurement);
                }
                else if (instrument.Name == LatticeReplicationMetrics.CoalesceEntriesElidedName)
                {
                    Interlocked.Add(ref _entriesElided, measurement);
                }
            });
            _listener.Start();
        }

        public void Dispose() => _listener.Dispose();
    }

    // --- Per-primitive combine convergence (combined == sequential apply) ---

    [Test]
    public void CrdtShape_pncounter_combine_is_effect_equivalent_to_sequential_apply()
    {
        var shape = CrdtShape.ForPnCounter();
        // PN-Counter components are cumulative per replica; the combine
        // must pointwise-max, never sum (summing would double-count).
        var d1 = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["A"] = 1 },
            Decrements = new Dictionary<string, long>(),
        };
        var d2 = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["A"] = 3, ["B"] = 2 },
            Decrements = new Dictionary<string, long> { ["A"] = 1 },
        };

        var sequential = (PnCounter)shape.CreateEmpty();
        shape.MergeDelta(sequential, d1);
        shape.MergeDelta(sequential, d2);

        var combinedState = (PnCounter)shape.CreateEmpty();
        shape.MergeDelta(combinedState, shape.CombineDeltas!(d1, d2));

        Assert.That(combinedState.Value, Is.EqualTo(sequential.Value),
            "combined PN-Counter delta must converge to the same value as applying the sequence");
    }

    [Test]
    public void CrdtShape_pncounter_combine_over_nonempty_state_matches_sequential_apply()
    {
        var shape = CrdtShape.ForPnCounter();
        var d1 = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["A"] = 2 },
            Decrements = new Dictionary<string, long>(),
        };
        var d2 = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["A"] = 5 },
            Decrements = new Dictionary<string, long>(),
        };

        // Apply against a non-empty receiver state to prove effect-equivalence
        // holds for all S, not just bottom.
        var sequential = (PnCounter)shape.CreateEmpty();
        sequential.Increment("A", 1);
        var combinedState = (PnCounter)shape.CreateEmpty();
        combinedState.Increment("A", 1);

        shape.MergeDelta(sequential, d1);
        shape.MergeDelta(sequential, d2);
        shape.MergeDelta(combinedState, shape.CombineDeltas!(d1, d2));

        Assert.That(combinedState.Value, Is.EqualTo(sequential.Value));
    }

    [Test]
    public void CrdtShape_pncounter_combine_is_idempotent_under_duplicate_delivery()
    {
        var shape = CrdtShape.ForPnCounter();
        var d = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["A"] = 4 },
            Decrements = new Dictionary<string, long>(),
        };

        var once = (PnCounter)shape.CreateEmpty();
        shape.MergeDelta(once, d);

        var combinedState = (PnCounter)shape.CreateEmpty();
        shape.MergeDelta(combinedState, shape.CombineDeltas!(d, d));

        Assert.That(combinedState.Value, Is.EqualTo(once.Value),
            "combining a delta with itself is idempotent for a cumulative counter");
    }

    [Test]
    public void CrdtShape_orset_combine_is_effect_equivalent_to_sequential_apply()
    {
        var shape = CrdtShape.ForOrSet();
        var elem1 = new byte[] { 1 };
        var elem2 = new byte[] { 2 };
        var d1 = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = elem1, ReplicaId = "A", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var d2 = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = elem2, ReplicaId = "A", Counter = 2 } },
            Removes = new[] { new OrSetDeltaDot { Element = elem1, ReplicaId = "A", Counter = 1 } },
        };

        var sequential = (OrSet)shape.CreateEmpty();
        shape.MergeDelta(sequential, d1);
        shape.MergeDelta(sequential, d2);

        var combinedState = (OrSet)shape.CreateEmpty();
        shape.MergeDelta(combinedState, shape.CombineDeltas!(d1, d2));

        Assert.Multiple(() =>
        {
            Assert.That(combinedState.Contains(elem1), Is.EqualTo(sequential.Contains(elem1)));
            Assert.That(combinedState.Contains(elem2), Is.EqualTo(sequential.Contains(elem2)));
            Assert.That(combinedState.Count, Is.EqualTo(sequential.Count));
        });
    }

    [Test]
    public void CrdtShape_versionvector_combine_is_pointwise_max()
    {
        var shape = CrdtShape.ForVersionVector();
        var d1 = new VersionVectorDelta
        {
            Entries = new Dictionary<string, HybridLogicalClock>
            {
                ["A"] = new HybridLogicalClock { WallClockTicks = 5 },
            },
        };
        var d2 = new VersionVectorDelta
        {
            Entries = new Dictionary<string, HybridLogicalClock>
            {
                ["A"] = new HybridLogicalClock { WallClockTicks = 3 },
                ["B"] = new HybridLogicalClock { WallClockTicks = 7 },
            },
        };

        var sequential = (VersionVector)shape.CreateEmpty();
        shape.MergeDelta(sequential, d1);
        shape.MergeDelta(sequential, d2);

        var combinedState = (VersionVector)shape.CreateEmpty();
        shape.MergeDelta(combinedState, shape.CombineDeltas!(d1, d2));

        Assert.Multiple(() =>
        {
            Assert.That(combinedState.GetClock("A"), Is.EqualTo(sequential.GetClock("A")));
            Assert.That(combinedState.GetClock("B"), Is.EqualTo(sequential.GetClock("B")));
        });
    }

    [Test]
    public void CrdtShape_mvregister_combine_resolves_dot_dominance_not_naive_concat()
    {
        var shape = CrdtShape.ForMvRegister();
        // A second write from the same replica that observed the first
        // supersedes it. A naive entry-concat would wrongly keep both;
        // the dominance-aware combine keeps only the survivor.
        var d1 = new MvRegisterDelta
        {
            Entries = new[] { new MvRegisterEntry { ReplicaId = "A", Counter = 1, Value = new byte[] { 1 } } },
            Context = new Dictionary<string, long> { ["A"] = 1 },
        };
        var d2 = new MvRegisterDelta
        {
            Entries = new[] { new MvRegisterEntry { ReplicaId = "A", Counter = 2, Value = new byte[] { 2 } } },
            Context = new Dictionary<string, long> { ["A"] = 2 },
        };

        var sequential = (MvRegister)shape.CreateEmpty();
        shape.MergeDelta(sequential, d1);
        shape.MergeDelta(sequential, d2);

        var combinedState = (MvRegister)shape.CreateEmpty();
        shape.MergeDelta(combinedState, shape.CombineDeltas!(d1, d2));

        Assert.Multiple(() =>
        {
            Assert.That(sequential.Count, Is.EqualTo(1), "the superseding write collapses the register to one value");
            Assert.That(combinedState.Count, Is.EqualTo(sequential.Count),
                "combined MV-Register delta must resolve dominance the same way the sequence does");
            Assert.That(combinedState.Values().Single(), Is.EqualTo(sequential.Values().Single()));
        });
    }

    [Test]
    public void CrdtShape_rga_combine_is_effect_equivalent_to_sequential_apply()
    {
        var shape = CrdtShape.ForRga();
        var n1 = new RgaDeltaNode { ReplicaId = "A", Counter = 1, ParentDot = Rga.Root, Value = new byte[] { 1 } };
        var n2 = new RgaDeltaNode { ReplicaId = "A", Counter = 2, ParentDot = n1.Dot, Value = new byte[] { 2 } };
        var d1 = new RgaDelta { Inserts = new[] { n1 }, Tombstones = Array.Empty<OrSetDot>() };
        var d2 = new RgaDelta { Inserts = new[] { n2 }, Tombstones = Array.Empty<OrSetDot>() };

        var sequential = (Rga)shape.CreateEmpty();
        shape.MergeDelta(sequential, d1);
        shape.MergeDelta(sequential, d2);

        var combinedState = (Rga)shape.CreateEmpty();
        shape.MergeDelta(combinedState, shape.CombineDeltas!(d1, d2));

        var sequentialValues = sequential.ToList().Select(static t => t.Value).ToArray();
        var combinedValues = combinedState.ToList().Select(static t => t.Value).ToArray();
        Assert.That(combinedValues, Is.EqualTo(sequentialValues),
            "combined RGA delta must materialise the identical ordered sequence");
    }

    [Test]
    public void CrdtShape_ormap_exposes_a_combine_that_folds_through_the_value_crdt()
    {
        var shape = CrdtShape.ForOrMap<string, PnCounter>();
        Assert.That(shape.CombineDeltas, Is.Not.Null,
            "the registered OR-Map shape now combines same-key delta runs via the value CRDT join");

        var value1 = new PnCounter();
        value1.Increment("X", 2);
        var value2 = new PnCounter();
        value2.Increment("Y", 3);
        var d1 = new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "A", Counter = 1, Value = value1 } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        };
        var d2 = new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "A", Counter = 2, Value = value2 } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        };

        // Smoke: the combine folds distinct dots into one delta whose live
        // value equals the sequential apply (5 = 2 + 3 across the two dots).
        var combined = (OrMapDelta<string, PnCounter>)shape.CombineDeltas!(d1, d2);
        var fromCombined = (OrMap<string, PnCounter>)shape.CreateEmpty();
        shape.MergeDelta(fromCombined, combined);
        var fromSequence = (OrMap<string, PnCounter>)shape.CreateEmpty();
        shape.MergeDelta(fromSequence, d1);
        shape.MergeDelta(fromSequence, d2);
        Assert.That(fromCombined.Get("k")!.Value, Is.EqualTo(fromSequence.Get("k")!.Value));
    }

    // --- Shipper-level CRDT coalescing behaviour ---

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_merges_same_key_deltas_to_one_entry()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        feed.Append(MakeCrdtSet("k", ticks: 1, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 2, PnDelta("A", 2)));
        feed.Append(MakeCrdtSet("k", ticks: 3, PnDelta("A", 3)));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(1),
            "the three same-key CRDT deltas merge into a single combined-delta entry");
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_keeps_distinct_keys_and_merges_per_key()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        feed.Append(MakeCrdtSet("k", ticks: 1, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("j", ticks: 2, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 3, PnDelta("A", 2)));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "the two 'k' deltas merge to one; 'j' (single delta) ships verbatim");
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_records_deltas_merged_and_entries_elided_counters()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        feed.Append(MakeCrdtSet("k", ticks: 1, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 2, PnDelta("A", 2)));
        feed.Append(MakeCrdtSet("k", ticks: 3, PnDelta("A", 3)));

        using var recorder = new CrdtCoalesceMetricRecorder();
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(recorder.DeltasMerged, Is.EqualTo(3),
                "three source deltas were folded into the combined delta");
            Assert.That(recorder.EntriesElided, Is.EqualTo(2),
                "two of the three source entries were dropped from the wire");
        });
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_single_delta_per_key_does_not_record_merge_counter()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        feed.Append(MakeCrdtSet("k", ticks: 1, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("j", ticks: 2, PnDelta("A", 1)));

        using var recorder = new CrdtCoalesceMetricRecorder();
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
                "two distinct keys with one delta each have nothing to merge");
            Assert.That(recorder.DeltasMerged, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_null_delta_ships_verbatim()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        // Opaque CRDT entries (no typed delta) cannot be combined safely;
        // they must ship verbatim with no data loss.
        feed.Append(MakeCrdtSet("k", ticks: 1, delta: null));
        feed.Append(MakeCrdtSet("k", ticks: 2, delta: null));
        feed.Append(MakeCrdtSet("k", ticks: 3, delta: null));

        using var recorder = new CrdtCoalesceMetricRecorder();
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(3),
                "null-delta CRDT entries fall back to ship-individually");
            Assert.That(recorder.DeltasMerged, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_mixed_null_and_typed_delta_ships_key_verbatim()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        // A key with any opaque entry is non-combinable end-to-end: all of
        // its entries ship verbatim so ordering and effect are preserved.
        feed.Append(MakeCrdtSet("k", ticks: 1, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 2, delta: null));
        feed.Append(MakeCrdtSet("k", ticks: 3, PnDelta("A", 2)));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(3),
            "an opaque entry forces the whole key to ship verbatim");
    }

    [Test]
    public async Task PumpOnceAsync_with_unregistered_ormap_mode_ships_verbatim()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.OrMap));
        // No OR-Map shape is registered for the test tree, so TryGet returns
        // null and the batch ships verbatim.
        feed.Append(MakeCrdtSet("k", ticks: 1, new byte[] { 1 }));
        feed.Append(MakeCrdtSet("k", ticks: 2, new byte[] { 2 }));
        feed.Append(MakeCrdtSet("k", ticks: 3, new byte[] { 3 }));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(3),
            "an unregistered OR-Map tree has no combiner; entries ship individually");
    }

    [Test]
    public async Task PumpOnceAsync_with_registered_ormap_mode_merges_same_key_deltas_to_one_entry()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.OrMap),
            crdtShapeRegistry: RegisteredOrMapRegistry());
        feed.Append(MakeCrdtSet("k", ticks: 1, OrMapAddDelta("k", "A", 1, "X", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 2, OrMapAddDelta("k", "A", 2, "X", 2)));
        feed.Append(MakeCrdtSet("k", ticks: 3, OrMapAddDelta("k", "A", 3, "X", 3)));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(1),
            "a registered OR-Map tree folds the three same-key deltas into a single combined-delta entry");
    }

    [Test]
    public async Task PumpOnceAsync_with_registered_ormap_mode_records_deltas_merged_and_entries_elided_counters()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.OrMap),
            crdtShapeRegistry: RegisteredOrMapRegistry());
        feed.Append(MakeCrdtSet("k", ticks: 1, OrMapAddDelta("k", "A", 1, "X", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 2, OrMapAddDelta("k", "A", 2, "X", 2)));
        feed.Append(MakeCrdtSet("k", ticks: 3, OrMapAddDelta("k", "A", 3, "X", 3)));

        using var recorder = new CrdtCoalesceMetricRecorder();
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(recorder.DeltasMerged, Is.EqualTo(3),
                "three OR-Map source deltas were folded into the combined delta");
            Assert.That(recorder.EntriesElided, Is.EqualTo(2),
                "two of the three OR-Map source entries were dropped from the wire");
        });
    }

    [Test]
    public async Task PumpOnceAsync_with_registered_ormap_mode_keeps_distinct_keys_and_merges_per_key()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.OrMap),
            crdtShapeRegistry: RegisteredOrMapRegistry());
        feed.Append(MakeCrdtSet("k", ticks: 1, OrMapAddDelta("k", "A", 1, "X", 1)));
        feed.Append(MakeCrdtSet("j", ticks: 2, OrMapAddDelta("j", "A", 1, "X", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 3, OrMapAddDelta("k", "A", 2, "X", 2)));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "the two 'k' OR-Map deltas merge to one; 'j' (single delta) ships verbatim");
    }

    [Test]
    public async Task PumpOnceAsync_with_registered_ormap_mode_null_delta_ships_verbatim()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.OrMap),
            crdtShapeRegistry: RegisteredOrMapRegistry());
        // Opaque OR-Map entries (no typed delta) cannot be combined safely;
        // they fall back to ship-individually even though a combiner exists.
        feed.Append(MakeCrdtSet("k", ticks: 1, delta: null));
        feed.Append(MakeCrdtSet("k", ticks: 2, delta: null));

        using var recorder = new CrdtCoalesceMetricRecorder();
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
                "null-delta OR-Map entries fall back to ship-individually");
            Assert.That(recorder.DeltasMerged, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task PumpOnceAsync_with_registered_ormap_mode_coalescing_disabled_ships_every_delta_verbatim()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(enabled: false),
            modeResolver: ResolverFor(LatticeMergeMode.OrMap),
            crdtShapeRegistry: RegisteredOrMapRegistry());
        feed.Append(MakeCrdtSet("k", ticks: 1, OrMapAddDelta("k", "A", 1, "X", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 2, OrMapAddDelta("k", "A", 2, "X", 2)));
        feed.Append(MakeCrdtSet("k", ticks: 3, OrMapAddDelta("k", "A", 3, "X", 3)));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(3),
            "the default-off path is byte-identical even for a registered OR-Map tree: every delta ships");
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_coalescing_disabled_ships_every_delta_verbatim()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(enabled: false),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        feed.Append(MakeCrdtSet("k", ticks: 1, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 2, PnDelta("A", 2)));
        feed.Append(MakeCrdtSet("k", ticks: 3, PnDelta("A", 3)));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(3),
            "the default-off path is byte-identical: every CRDT delta ships");
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_never_merges_across_prepared_atomic_boundary()
    {
        var (grain, _, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        var txId = Guid.NewGuid();
        feed.Append(MakePreparedCrdtSet("k", ticks: 1, PnDelta("A", 1), txId, batchSize: 2, batchIndex: 0));
        feed.Append(MakePreparedCrdtSet("k", ticks: 2, PnDelta("A", 2), txId, batchSize: 2, batchIndex: 1));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "saga prepare-phase CRDT entries are never coalesced across the atomic-batch boundary");
    }

    [Test]
    public async Task PumpOnceAsync_with_crdt_mode_advances_cursor_past_every_merged_entry()
    {
        var (grain, state, feed, transport, _, _, _) = Create(
            CoalesceOptions(),
            modeResolver: ResolverFor(LatticeMergeMode.PnCounter));
        var ackHlc = new HybridLogicalClock { WallClockTicks = 4 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = ackHlc });
        feed.Append(MakeCrdtSet("k", ticks: 1, PnDelta("A", 1)));
        feed.Append(MakeCrdtSet("k", ticks: 2, PnDelta("A", 2)));
        feed.Append(MakeCrdtSet("k", ticks: 3, PnDelta("A", 3)));
        feed.Append(MakeCrdtSet("other", ticks: 4, PnDelta("A", 1)));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
                "the three 'k' deltas merged to one; 'other' ships verbatim");
            Assert.That(state.State.PartitionCursors, Contains.Key(0));
            Assert.That(state.State.PartitionCursors[0], Is.EqualTo(4L),
                "the cursor advances past every consumed sequence - merged-away entries included");
        });
    }
}