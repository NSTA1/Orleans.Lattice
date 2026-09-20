using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The tree-count-independent bound on cold-start registry fan-in (issue #3240).
/// <para>
/// <b>The law this fixture exists to break.</b> Every registered tree births its
/// own reminder-driven background services, and each of them resolves through the
/// one cluster-singleton <see cref="ILatticeRegistry"/> activation. Measured on a
/// live estate, that produced ~0.41 registry calls/s/tree with no decay across a
/// 4x range of tree counts: the cold-start cost was the product
/// <c>(trees) x (per-tree background services)</c>, and no bound anywhere in the
/// path was independent of the tree count.
/// </para>
/// <para>
/// <b>Why the pre-existing single-flight coalescer did not close this</b>
/// (<see cref="LatticeOptionsResolverRegistryCoalescingTests"/>): that mechanism
/// collapses the concurrent readers of <em>one</em> tree into one round trip,
/// which removes the second factor. K distinct trees resolving at once still
/// started K flights, so peak fan-in stayed proportional to K. The two compose,
/// and only the gate bounds the first factor.
/// </para>
/// <para>
/// <b>What makes this a scalability gate and not a health gate.</b> The load-bearing
/// assertion is not that a fixed tree count behaves well - a faster registry would
/// satisfy that while leaving the law untouched. It is that the observed peak is
/// the same constant at two tree counts a 4x apart, and that per-tree round trips
/// fall rather than hold as the estate grows. A fix that merely made the call
/// cheaper would fail both.
/// </para>
/// </summary>
[TestFixture]
public class RegistryFanInGateTests
{
    private static TreeRegistryEntry PinnedEntry(string? physical = null, int[]? slots = null) => new()
    {
        MaxLeafKeys = 128,
        MaxInternalChildren = 128,
        ShardCount = 1,
        PhysicalTreeId = physical,
        ShardMap = slots is null ? null : new ShardMap { Slots = slots, Version = 7 },
    };

    /// <summary>
    /// Records every registry round trip the gate issues and the peak number that
    /// were ever inside the registry at the same instant.
    /// </summary>
    private sealed class RegistryProbe
    {
        private int _inFlight;
        private int _peak;
        private int _inFlightKeys;
        private int _peakKeys;

        internal int RoundTrips;
        internal int SingleKeyReads;
        internal int BatchedReads;
        internal int WidestBatch;
        internal int Peak => Volatile.Read(ref _peak);

        /// <summary>
        /// High-water mark of concurrently in-flight <em>keys</em>, not round
        /// trips. A batched read expands to one downstream key read per id, so
        /// this is the quantity that actually reaches the backing tree and the
        /// one the permit count multiplies with the batch size to produce.
        /// </summary>
        internal int PeakKeys => Volatile.Read(ref _peakKeys);

        internal IDisposable Enter(int keys = 1)
        {
            Interlocked.Increment(ref RoundTrips);
            var now = Interlocked.Increment(ref _inFlight);
            var nowKeys = Interlocked.Add(ref _inFlightKeys, keys);

            // Lift the high-water marks without losing a concurrent raise.
            int seen;
            while (now > (seen = Volatile.Read(ref _peak)) &&
                   Interlocked.CompareExchange(ref _peak, now, seen) != seen)
            {
            }

            while (nowKeys > (seen = Volatile.Read(ref _peakKeys)) &&
                   Interlocked.CompareExchange(ref _peakKeys, nowKeys, seen) != seen)
            {
            }

            return new Exit(this, keys);
        }

        private sealed class Exit(RegistryProbe owner, int keys) : IDisposable
        {
            public void Dispose()
            {
                Interlocked.Decrement(ref owner._inFlight);
                Interlocked.Add(ref owner._inFlightKeys, -keys);
            }
        }
    }

    /// <summary>
    /// Builds a registry whose reads hold themselves inside the call for
    /// <paramref name="hold"/>, so concurrent round trips genuinely overlap and the
    /// probe's high-water mark means something.
    /// </summary>
    private static (IGrainFactory Factory, RegistryProbe Probe) BuildRegistry(
        TimeSpan hold,
        Func<string, TreeRegistryEntry?>? entryFor = null,
        Exception? failWith = null)
    {
        var probe = new RegistryProbe();
        entryFor ??= _ => PinnedEntry();

        var registry = Substitute.For<ILatticeRegistry>();

        registry.GetEntryAsync(Arg.Any<string>()).Returns(call =>
        {
            Interlocked.Increment(ref probe.SingleKeyReads);
            return ServeOneAsync((string)call[0]);
        });

        registry.GetEntriesAsync(Arg.Any<IReadOnlyList<string>>()).Returns(call =>
        {
            var ids = (IReadOnlyList<string>)call[0];
            Interlocked.Increment(ref probe.BatchedReads);
            InterlockedRaise(ref probe.WidestBatch, ids.Count);
            return ServeManyAsync(ids);
        });

        async Task<TreeRegistryEntry?> ServeOneAsync(string treeId)
        {
            using (probe.Enter())
            {
                await Task.Delay(hold);
                if (failWith is not null)
                    throw failWith;
                return entryFor(treeId);
            }
        }

        async Task<Dictionary<string, TreeRegistryEntry>> ServeManyAsync(IReadOnlyList<string> ids)
        {
            // Charged as ids.Count keys, because the real GetEntriesAsync issues
            // one concurrent backing read per id rather than one per batch.
            using (probe.Enter(ids.Count))
            {
                await Task.Delay(hold);
                if (failWith is not null)
                    throw failWith;

                var result = new Dictionary<string, TreeRegistryEntry>(StringComparer.Ordinal);
                foreach (var id in ids)
                {
                    if (entryFor(id) is { } entry)
                        result[id] = entry;
                }

                return result;
            }
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return (factory, probe);
    }

    private static void InterlockedRaise(ref int target, int candidate)
    {
        int seen;
        while (candidate > (seen = Volatile.Read(ref target)) &&
               Interlocked.CompareExchange(ref target, candidate, seen) != seen)
        {
        }
    }

    /// <summary>
    /// THE gate. Peak concurrent registry fan-in must be the same constant at two
    /// tree counts a 4x apart, and per-tree round trips must not rise with the
    /// estate. Before the bound existed, the peak equalled the tree count exactly
    /// and this failed at both arms.
    /// </summary>
    [Test]
    public async Task Peak_fan_in_and_per_tree_round_trips_do_not_rise_with_tree_count()
    {
        var (smallPeak, smallTrips) = await DrainAsync(trees: 40);
        var (largePeak, largeTrips) = await DrainAsync(trees: 160);

        Assert.Multiple(() =>
        {
            Assert.That(smallPeak, Is.LessThanOrEqualTo(RegistryFanInGate.GlobalMaxConcurrentReads),
                "40 trees resolving at once must not put more than the gate's constant into the registry.");
            Assert.That(largePeak, Is.LessThanOrEqualTo(RegistryFanInGate.GlobalMaxConcurrentReads),
                "4x the trees must not widen the fan-in: a bound that grows with the estate is not a bound.");
            Assert.That(largePeak, Is.LessThanOrEqualTo(smallPeak),
                "the peak is the scaling signal - it must be flat across tree counts, not merely small at one.");

            // Per-tree round trips, which is the acceptance criterion the issue
            // states. Batching means a wider estate costs wider messages rather
            // than more calls, so this strictly falls rather than holding.
            var smallPerTree = smallTrips / 40.0;
            var largePerTree = largeTrips / 160.0;
            Assert.That(largePerTree, Is.LessThanOrEqualTo(smallPerTree),
                $"per-tree registry round trips must be flat or sub-linear: "
                + $"{smallTrips} trips for 40 trees vs {largeTrips} for 160.");
            Assert.That(largeTrips, Is.LessThan(160 / 2),
                "160 trees must not cost anything like 160 round trips.");
        });

        static async Task<(int Peak, int RoundTrips)> DrainAsync(int trees)
        {
            var (factory, probe) = BuildRegistry(TimeSpan.FromMilliseconds(60));
            var gate = new RegistryFanInGate(factory);

            var reads = new Task<TreeRegistryEntry?>[trees];
            for (var i = 0; i < trees; i++)
                reads[i] = gate.GetEntryAsync($"tree-{i:D4}");

            await Task.WhenAll(reads);
            Assert.That(reads.All(r => r.Result is not null), Is.True, "every tree must resolve");
            return (probe.Peak, probe.RoundTrips);
        }
    }

    /// <summary>
    /// The two constants are one budget expressed as a product, and this test is
    /// the tripwire that says so.
    /// <para>
    /// <see cref="ILatticeRegistry.GetEntriesAsync"/> issues one concurrent
    /// backing read per id in the batch rather than one per batch, so a batch of
    /// B ids becomes B concurrent key reads downstream. The concurrency that
    /// actually reaches the backing tree is therefore permits x batch size, not
    /// the permit count. Anyone widening
    /// <see cref="RegistryFanInGate.MaxBatchSize"/> to buy fewer round trips is
    /// widening downstream fan-out one-for-one, which is the opposite of what
    /// "batching reduces load" suggests, so it must be a deliberate act rather
    /// than an optimisation.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_downstream_ceiling_is_the_product_of_the_two_constants()
    {
        const int Product =
            RegistryFanInGate.GlobalMaxConcurrentReads * RegistryFanInGate.MaxBatchSize;

        // Both arms sit well above the product, so the product is what binds
        // rather than the caller count. Below it the gate is indistinguishable
        // from the ungated path by construction, which is the point of the
        // comparative claim in MaxBatchSize's remarks.
        var small = await DrainKeysAsync(trees: 1_400);
        var large = await DrainKeysAsync(trees: 2_800);

        Assert.Multiple(() =>
        {
            Assert.That(small, Is.LessThanOrEqualTo(Product),
                $"downstream key concurrency must not exceed permits x batch ({Product}).");
            Assert.That(large, Is.LessThanOrEqualTo(Product),
                "doubling the estate must not widen downstream fan-out past the product.");
            Assert.That(large, Is.LessThanOrEqualTo(small),
                "the downstream ceiling is the scaling signal too - it must be flat, not merely capped.");

            // Stated rather than derived, so that changing either constant fails
            // here and forces the product to be reconsidered deliberately.
            Assert.That(Product, Is.EqualTo(1024),
                "permits x batch is the concurrency reaching the backing tree. "
                + "If you changed either constant, read the remarks on MaxBatchSize: "
                + "batching relocates and re-expands concurrent work rather than reducing it.");
        });

        static async Task<int> DrainKeysAsync(int trees)
        {
            var (factory, probe) = BuildRegistry(TimeSpan.FromMilliseconds(25));
            var gate = new RegistryFanInGate(factory);

            var reads = new Task<TreeRegistryEntry?>[trees];
            for (var i = 0; i < trees; i++)
                reads[i] = gate.GetEntryAsync($"tree-{i:D5}");

            await Task.WhenAll(reads);
            Assert.That(reads.All(r => r.Result is not null), Is.True, "every tree must resolve");
            return probe.PeakKeys;
        }
    }

    /// <summary>
    /// The bound must survive the real caller path, not only a direct call: every
    /// per-tree background service reaches the registry through
    /// <see cref="LatticeOptionsResolver"/>, so that is where the unbounded fan-in
    /// actually lived. This is the arm that failed before the gate was wired in.
    /// </summary>
    [Test]
    public async Task Options_resolution_across_many_trees_holds_the_bound()
    {
        LatticeOptionsResolver.ResetWarnedLatchedTreesForTests();
        var (factory, probe) = BuildRegistry(TimeSpan.FromMilliseconds(60));

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var resolver = new LatticeOptionsResolver(factory, monitor);

        var resolves = Enumerable.Range(0, 120)
            .Select(i => resolver.ResolveAsync($"tree-resolver-{i:D4}"))
            .ToArray();

        await Task.WhenAll(resolves);

        Assert.That(probe.Peak, Is.LessThanOrEqualTo(RegistryFanInGate.GlobalMaxConcurrentReads),
            "120 trees resolving options at once is the cold-start shape, and it must "
            + "present a constant fan-in to the registry singleton.");
    }

    /// <summary>
    /// A batched read must hand every caller its own tree's entry. Getting this
    /// wrong would be far worse than the fan-in it fixes, so it is pinned with
    /// distinguishable per-tree values rather than a shared stub.
    /// </summary>
    [Test]
    public async Task Batched_read_gives_each_caller_its_own_entry()
    {
        var (factory, probe) = BuildRegistry(
            TimeSpan.FromMilliseconds(60),
            entryFor: id => PinnedEntry(physical: $"physical-of-{id}"));
        var gate = new RegistryFanInGate(factory);

        var ids = Enumerable.Range(0, 64).Select(i => $"tree-{i:D3}").ToArray();
        var reads = ids.Select(gate.ResolveAsync).ToArray();
        var resolved = await Task.WhenAll(reads);

        Assert.Multiple(() =>
        {
            for (var i = 0; i < ids.Length; i++)
            {
                Assert.That(resolved[i], Is.EqualTo($"physical-of-{ids[i]}"),
                    $"{ids[i]} must receive its own entry, not a neighbour's");
            }

            Assert.That(probe.BatchedReads, Is.GreaterThan(0), "the fan-in must have formed a batch");
        });
    }

    /// <summary>
    /// A tree absent from the batched result reads back as <c>null</c>, which is
    /// what the single-key member returns for an unregistered tree. The batched
    /// member simply omits unregistered ids, so this demultiplexing step is where
    /// "absent" has to become "null".
    /// </summary>
    [Test]
    public async Task Unregistered_trees_read_back_null_from_a_batch()
    {
        var (factory, probe) = BuildRegistry(
            TimeSpan.FromMilliseconds(60),
            entryFor: id => id.EndsWith('3') ? null : PinnedEntry());
        var gate = new RegistryFanInGate(factory);

        var ids = Enumerable.Range(0, 40).Select(i => $"tree-{i:D3}").ToArray();
        var entries = await Task.WhenAll(ids.Select(gate.GetEntryAsync));

        Assert.Multiple(() =>
        {
            for (var i = 0; i < ids.Length; i++)
            {
                if (ids[i].EndsWith('3'))
                    Assert.That(entries[i], Is.Null, $"{ids[i]} is unregistered and must read back null");
                else
                    Assert.That(entries[i], Is.Not.Null, $"{ids[i]} is registered and must read back an entry");
            }

            Assert.That(probe.BatchedReads, Is.GreaterThan(0), "the fan-in must have formed a batch");
        });
    }

    /// <summary>
    /// An un-contended silo must issue byte-for-byte the traffic it issued before
    /// the gate existed. A batch of one is not a batch, and reaching for the
    /// batched member here would change the call shape of every small deployment
    /// to buy a bound it never needed.
    /// </summary>
    [Test]
    public async Task A_lone_read_uses_the_single_key_member()
    {
        var (factory, probe) = BuildRegistry(TimeSpan.Zero);
        var gate = new RegistryFanInGate(factory);

        await gate.GetEntryAsync("tree-alone");

        Assert.Multiple(() =>
        {
            Assert.That(probe.SingleKeyReads, Is.EqualTo(1));
            Assert.That(probe.BatchedReads, Is.Zero,
                "an uncontended read must not change shape just because a bound exists");
            Assert.That(probe.RoundTrips, Is.EqualTo(1));
        });
    }

    /// <summary>
    /// Duplicate ids arriving together share one round trip rather than occupying
    /// two slots of a bound that is deliberately small.
    /// </summary>
    [Test]
    public async Task Concurrent_reads_of_one_tree_share_one_round_trip()
    {
        var (factory, probe) = BuildRegistry(TimeSpan.FromMilliseconds(80));
        var gate = new RegistryFanInGate(factory);

        var reads = Enumerable.Range(0, 32).Select(_ => gate.GetEntryAsync("tree-same")).ToArray();
        await Task.WhenAll(reads);

        Assert.That(probe.RoundTrips, Is.EqualTo(1),
            "32 readers of one tree must not consume 32 of the gate's 4 slots");
    }

    /// <summary>
    /// A failing round trip must fault every caller that joined it. Sharing one
    /// failure is not new exposure - the alternative is N identical failures
    /// against a registry that is already not answering - but silently returning
    /// <c>null</c> to the joiners would be.
    /// </summary>
    [Test]
    public void A_failed_batch_faults_every_caller_in_it()
    {
        var boom = new InvalidOperationException("registry is not answering");
        var (factory, _) = BuildRegistry(TimeSpan.FromMilliseconds(40), failWith: boom);
        var gate = new RegistryFanInGate(factory);

        var reads = Enumerable.Range(0, 32).Select(i => gate.GetEntryAsync($"tree-{i:D3}")).ToArray();

        Assert.Multiple(() =>
        {
            foreach (var read in reads)
            {
                var thrown = Assert.ThrowsAsync<InvalidOperationException>(async () => await read);
                Assert.That(thrown!.Message, Is.EqualTo(boom.Message));
            }
        });
    }

    /// <summary>
    /// <see cref="ShardMap"/> is a mutable class, and an un-gated grain call handed
    /// every caller its own deep copy. Two callers that join one gated read must
    /// therefore still get separate instances, or the gate would silently begin
    /// aliasing state that used to be private to each caller - a correctness
    /// regression introduced in the name of a performance bound.
    /// </summary>
    [Test]
    public async Task Joined_shard_map_readers_do_not_alias_one_instance()
    {
        var (factory, _) = BuildRegistry(
            TimeSpan.FromMilliseconds(80),
            entryFor: _ => PinnedEntry(slots: [0, 1, 2, 3]));
        var gate = new RegistryFanInGate(factory);

        var first = gate.GetShardMapAsync("tree-shared");
        var second = gate.GetShardMapAsync("tree-shared");
        var maps = await Task.WhenAll(first, second);

        Assert.Multiple(() =>
        {
            Assert.That(maps[0], Is.Not.Null);
            Assert.That(maps[1], Is.Not.Null);
            Assert.That(maps[0], Is.Not.SameAs(maps[1]), "joined callers must not share one map instance");
            Assert.That(maps[0]!.Slots, Is.Not.SameAs(maps[1]!.Slots), "nor one slot array");
            Assert.That(maps[0]!.Slots, Is.EqualTo(new[] { 0, 1, 2, 3 }));
            Assert.That(maps[0]!.Version, Is.EqualTo(7));
        });

        maps[0]!.Slots[0] = 99;
        Assert.That(maps[1]!.Slots[0], Is.Zero, "mutating one caller's map must not be visible to the other");
    }

    /// <summary>
    /// The gate shares round trips in progress and retains nothing. A read issued
    /// after a previous one completed must reach the registry afresh, or the bound
    /// would have quietly become a cache with an unstated staleness window - the
    /// same property <see cref="LatticeOptionsResolverRegistryCoalescingTests"/>
    /// pins for the per-tree coalescer.
    /// </summary>
    [Test]
    public async Task Sequential_reads_each_reach_the_registry()
    {
        var (factory, probe) = BuildRegistry(TimeSpan.Zero);
        var gate = new RegistryFanInGate(factory);

        await gate.GetEntryAsync("tree-sequential");
        await gate.GetEntryAsync("tree-sequential");
        await gate.GetEntryAsync("tree-sequential");

        Assert.That(probe.RoundTrips, Is.EqualTo(3),
            "the gate coalesces reads in flight; it must not retain a completed one");
    }

    /// <summary>
    /// No batch may exceed the size cap, so a very large estate widens the number
    /// of batches rather than producing one unboundedly large registry message.
    /// </summary>
    [Test]
    public async Task No_batch_exceeds_the_size_cap()
    {
        var (factory, probe) = BuildRegistry(TimeSpan.FromMilliseconds(60));
        var gate = new RegistryFanInGate(factory);

        var reads = Enumerable.Range(0, 400).Select(i => gate.GetEntryAsync($"tree-{i:D4}")).ToArray();
        await Task.WhenAll(reads);

        Assert.That(probe.WidestBatch, Is.LessThanOrEqualTo(RegistryFanInGate.MaxBatchSize),
            "an estate large enough to queue must widen the batch count, not the batch");
    }

    /// <summary>
    /// <b>The bound must hold at the target, not at each caller.</b> There is
    /// exactly one registry activation cluster-wide, so M silos each holding a
    /// private bound of C would deliver <c>M x C</c> round trips to that single
    /// activation - a large constant factor, not a bound, and one that grows with
    /// the estate because a cluster adds silos precisely because it added trees.
    /// <para>
    /// This drives M independent gates, each seeing a membership of M, at one
    /// shared registry, and asserts the total in flight at that registry never
    /// exceeds the one cluster-wide ceiling. Run at two silo counts a 4x apart so
    /// a pass cannot come from M being small.
    /// </para>
    /// </summary>
    [TestCase(4)]
    [TestCase(16)]
    public async Task The_bound_is_global_across_silos_not_per_silo(int silos)
    {
        var (factory, probe) = BuildRegistry(TimeSpan.FromMilliseconds(60));

        // One registry, M gates, each told the cluster has M live silos.
        var gates = Enumerable.Range(0, silos)
            .Select(_ => new RegistryFanInGate(factory, OracleFor(silos)))
            .ToArray();

        // Every silo cold-starts 60 trees at once: the storm shape.
        var reads = new List<Task<TreeRegistryEntry?>>();
        for (var s = 0; s < silos; s++)
        {
            for (var t = 0; t < 60; t++)
                reads.Add(gates[s].GetEntryAsync($"silo{s}-tree-{t:D3}"));
        }

        await Task.WhenAll(reads);

        Assert.That(probe.Peak, Is.LessThanOrEqualTo(RegistryFanInGate.GlobalMaxConcurrentReads),
            $"{silos} silos x 60 trees must present at most the one cluster-wide ceiling "
            + $"to the single registry activation, not {silos} private budgets.");
    }

    /// <summary>
    /// The admission wait the bound imposes must be observable. Bounding fan-in
    /// queues work rather than removing it, and every other registry signal is
    /// scoped to the registry grain, so a stall that relocated to caller-side
    /// admission would drive them all to zero and read as a clean recovery. This
    /// pins that the relocated wait is actually recorded.
    /// </summary>
    [Test]
    public async Task The_admission_wait_the_bound_imposes_is_recorded()
    {
        var recorded = new List<double>();
        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (ReferenceEquals(instrument, LatticeMetrics.RegistryAdmissionWait))
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<double>((_, value, _, _) =>
        {
            lock (recorded) recorded.Add(value);
        });
        listener.Start();

        var (factory, _) = BuildRegistry(TimeSpan.FromMilliseconds(60));
        var gate = new RegistryFanInGate(factory);

        // Far more trees than the budget, so most must genuinely queue.
        var reads = Enumerable.Range(0, 200).Select(i => gate.GetEntryAsync($"tree-{i:D4}")).ToArray();
        await Task.WhenAll(reads);
        listener.Dispose();

        lock (recorded)
        {
            Assert.Multiple(() =>
            {
                Assert.That(recorded, Has.Count.EqualTo(200),
                    "every admitted read must record its wait, including the ones that waited zero");
                Assert.That(recorded.Max(), Is.GreaterThan(0),
                    "a storm past the bound must show a non-zero queueing tail, or the "
                    + "instrument is not actually observing the relocated wait");
            });
        }
    }

    /// <summary>
    /// The width the bound caps must be observable, and observable <em>at</em> the
    /// bound.
    /// <para>
    /// <b>Why a separate instrument was needed at all.</b> The obvious candidate,
    /// <c>orleans.lattice.registry.call.in_flight</c>, counts calls executing
    /// inside the registry singleton's body summed over every caller in the
    /// cluster, including callers that never pass through a gate. That is a
    /// different population with a different ceiling from the permit count
    /// <see cref="RegistryFanInGate.GlobalMaxConcurrentReads"/> caps, so reading
    /// it against that constant compares two quantities that were never the same
    /// number - and a comfortable-looking reading there says nothing about
    /// whether the bound has headroom.
    /// </para>
    /// <para>
    /// <b>Why the recorded value counts the dispatch being recorded.</b> Its
    /// neighbours exclude the arrival. Under that convention a fully saturated
    /// gate tops out at 15 against a bound of 16, so saturation and headroom are
    /// indistinguishable by inspection: exactly the off-by-one that would let an
    /// experiment which did reach the ceiling still report that it had not. This
    /// test pins the inclusive convention so that equality with the constant
    /// remains the readable saturation signal.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_gate_width_the_bound_caps_is_recorded_and_reaches_the_bound()
    {
        var widths = new List<int>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.RegistryAdmissionInFlight,
            l => l.SetMeasurementEventCallback<int>((_, value, _, _) =>
            {
                lock (widths) widths.Add(value);
            }));

        // A hold long enough that the first GlobalMaxConcurrentReads dispatches
        // are all still in flight when the rest arrive, which is the only
        // condition under which the permits can actually be exhausted.
        var (factory, _) = BuildRegistry(TimeSpan.FromMilliseconds(60));
        var gate = new RegistryFanInGate(factory);

        var reads = Enumerable.Range(0, 400).Select(i => gate.GetEntryAsync($"tree-{i:D4}")).ToArray();
        await Task.WhenAll(reads);
        listener.Dispose();

        lock (widths)
        {
            Assert.Multiple(() =>
            {
                Assert.That(widths, Is.Not.Empty, "every gated dispatch must record its width");
                Assert.That(widths.Min(), Is.GreaterThanOrEqualTo(1),
                    "the value counts the dispatch being recorded, so it is never zero");
                Assert.That(widths.Max(), Is.EqualTo(RegistryFanInGate.GlobalMaxConcurrentReads),
                    $"a storm far past the bound must record width exactly "
                    + $"{RegistryFanInGate.GlobalMaxConcurrentReads}. A max of "
                    + $"{RegistryFanInGate.GlobalMaxConcurrentReads - 1} means the arrival was "
                    + "excluded from the count, which would make a saturated gate "
                    + "indistinguishable from one with a spare permit.");
                Assert.That(widths.Max(), Is.LessThanOrEqualTo(RegistryFanInGate.GlobalMaxConcurrentReads),
                    "the instrument must never record above the bound it reports on");
            });
        }
    }

    /// <summary>
    /// The batching half of the gate must be observable as a share, not inferred.
    /// <para>
    /// A dispatch of one id takes the single-key <c>GetEntryAsync</c> path, which
    /// is byte-for-byte the traffic the silo had before the gate existed; two or
    /// more takes the batched <c>GetEntriesAsync</c> path. So the share of
    /// recorded sizes above one is the share of reads the bound actually
    /// coalesced, and it is the only direct evidence that the batching path ran
    /// at all. Both arms are asserted because a batching share is only meaningful
    /// against a demonstrated floor: without the unsaturated arm, a low share is
    /// equally consistent with "batching is broken" and with "nothing queued".
    /// </para>
    /// </summary>
    [Test]
    public async Task The_batched_share_is_recorded_and_moves_only_when_the_gate_queues()
    {
        var saturated = await BatchSizesAsync(trees: 400);
        var idle = await BatchSizesAsync(trees: 1);

        Assert.Multiple(() =>
        {
            Assert.That(idle, Has.Count.EqualTo(1), "one arrival is one dispatch");
            Assert.That(idle[0], Is.EqualTo(1),
                "an uncontended read must dispatch alone - a batch of one is the ungated shape, "
                + "and if this recorded more the instrument would overstate coalescing");

            Assert.That(saturated, Is.Not.Empty);
            Assert.That(saturated.Count(s => s > 1), Is.GreaterThan(0),
                "a storm far past the bound must actually form batches, or the gate is "
                + "queueing without coalescing and the batching constant is dead weight");
            Assert.That(saturated.Max(), Is.LessThanOrEqualTo(RegistryFanInGate.MaxBatchSize),
                "no dispatch may carry more ids than the batch constant permits");
        });

        static async Task<List<int>> BatchSizesAsync(int trees)
        {
            var sizes = new List<int>();
            using var listener = MeterListening.StartForInstrument(
                LatticeMetrics.RegistryAdmissionBatchSize,
                l => l.SetMeasurementEventCallback<int>((_, value, _, _) =>
                {
                    lock (sizes) sizes.Add(value);
                }));

            var (factory, _) = BuildRegistry(TimeSpan.FromMilliseconds(60));
            var gate = new RegistryFanInGate(factory);

            var reads = Enumerable.Range(0, trees).Select(i => gate.GetEntryAsync($"tree-{i:D4}")).ToArray();
            await Task.WhenAll(reads);
            listener.Dispose();

            lock (sizes) return [.. sizes];
        }
    }

    /// <summary>
    /// The instrument that tells an idle gate apart from a comfortable one.
    /// <para>
    /// This is the fixture's answer to the false-green failure mode. Admission
    /// dispatches synchronously on the arriving thread whenever a permit is free,
    /// so an unsaturated gate drives every other gate signal to its structural
    /// floor - a wait of microseconds, a width of one, a batch of one. Those
    /// floors are not a weak measurement of headroom; they are what absent demand
    /// looks like, and they are visually identical to a bound with room to spare.
    /// An experiment reading only those three cannot tell the two apart, and will
    /// report "the bound is comfortable" when what happened is that nothing ever
    /// asked for it.
    /// </para>
    /// <para>
    /// Queue depth is recorded at enqueue rather than at dispatch, so it measures
    /// <em>offered</em> fan-in and moves whether or not the bound binds. The two
    /// arms here are the discrimination itself: the idle arm pins the floor, the
    /// saturated arm pins that the signal leaves it. A depth stuck at one is the
    /// evidence that a green run means nothing.
    /// </para>
    /// </summary>
    [Test]
    public async Task Queue_depth_separates_absent_demand_from_a_bound_with_headroom()
    {
        var saturated = await DepthsAsync(trees: 400);
        var idle = await DepthsAsync(trees: 1);

        Assert.Multiple(() =>
        {
            Assert.That(idle, Has.Count.EqualTo(1));
            Assert.That(idle[0], Is.EqualTo(1),
                "a lone arrival offers a fan-in of exactly itself - this is the floor every "
                + "other gate instrument also sits at when nothing is asking, which is why "
                + "none of them can be read as evidence on its own");

            Assert.That(saturated.Max(), Is.GreaterThan(RegistryFanInGate.GlobalMaxConcurrentReads),
                $"offered fan-in must be shown to exceed the bound of "
                + $"{RegistryFanInGate.GlobalMaxConcurrentReads} before any reading from the "
                + "wait, width or batch-size instruments is evidence about the bound at all");
            Assert.That(saturated.Count(d => d > 1), Is.GreaterThan(saturated.Count / 2),
                "under a storm most arrivals must find others already waiting, or the "
                + "arrivals are dispersed in time and the run sampled the wrong regime");
        });

        static async Task<List<int>> DepthsAsync(int trees)
        {
            var depths = new List<int>();
            using var listener = MeterListening.StartForInstrument(
                LatticeMetrics.RegistryAdmissionQueueDepth,
                l => l.SetMeasurementEventCallback<int>((_, value, _, _) =>
                {
                    lock (depths) depths.Add(value);
                }));

            var (factory, _) = BuildRegistry(TimeSpan.FromMilliseconds(60));
            var gate = new RegistryFanInGate(factory);

            var reads = Enumerable.Range(0, trees).Select(i => gate.GetEntryAsync($"tree-{i:D4}")).ToArray();
            await Task.WhenAll(reads);
            listener.Dispose();

            lock (depths) return [.. depths];
        }
    }

    /// <summary>
    /// A membership snapshot reporting <paramref name="silos"/> active hosts.
    /// </summary>
    private static ISiloStatusOracle OracleFor(int silos)
    {
        var statuses = new Dictionary<SiloAddress, SiloStatus>();
        for (var i = 0; i < silos; i++)
        {
            statuses[SiloAddress.New(
                new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 11111 + i), 0)] = SiloStatus.Active;
        }

        var oracle = Substitute.For<ISiloStatusOracle>();
        oracle.GetApproximateSiloStatuses(true).Returns(statuses);
        return oracle;
    }
}
