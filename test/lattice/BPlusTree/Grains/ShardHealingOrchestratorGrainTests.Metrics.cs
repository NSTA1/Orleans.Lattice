using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the two instruments automatic over-split healing publishes.
/// <para>
/// The epic's adoption proof reads these to show healing actually happened, so
/// they have to answer three questions an operator will really ask: how much
/// damage is left (<c>healing.backlog</c>), what the healer is currently doing
/// about it (<c>healing.decisions</c>), and how much has been reclaimed - which
/// is deliberately <em>not</em> a third instrument, but the existing
/// <c>shard.consolidations_committed</c> counter the consolidation coordinator
/// already fires once per durably committed fold. Publishing a second counter
/// for the same event would double-count it.
/// </para>
/// </summary>
public partial class ShardHealingOrchestratorGrainTests
{
    private const string HealingBacklogInstrument = "orleans.lattice.shard.healing.backlog";
    private const string HealingDecisionsInstrument = "orleans.lattice.shard.healing.decisions";

    /// <summary>
    /// Captures measurements on the Lattice meter for the duration of one test,
    /// so emission is observed directly rather than inferred.
    /// </summary>
    private sealed class HealingMetricRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)> _records = [];
        private readonly Lock _gate = new();

        public HealingMetricRecorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (ReferenceEquals(instrument.Meter, LatticeMetrics.Meter))
                        listener.EnableMeasurementEvents(instrument);
                },
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                lock (_gate) _records.Add((instrument.Name, value, tags.ToArray()));
            });
            _listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) =>
            {
                lock (_gate) _records.Add((instrument.Name, value, tags.ToArray()));
            });
            _listener.Start();
        }

        private static bool HasTag(KeyValuePair<string, object?>[] tags, string key, string value)
        {
            foreach (var tag in tags)
            {
                if (tag.Key == key && (tag.Value as string) == value) return true;
            }
            return false;
        }

        /// <summary>Every value recorded for an instrument against this test's tree.</summary>
        public IReadOnlyList<long> ValuesFor(string instrumentName)
        {
            lock (_gate)
            {
                return _records
                    .Where(r => r.Name == instrumentName && HasTag(r.Tags, LatticeMetrics.TagTree, TreeId))
                    .Select(r => r.Value)
                    .ToArray();
            }
        }

        /// <summary>Total counted for an instrument carrying a specific decision tag.</summary>
        public long TotalForDecision(string instrumentName, string decision)
        {
            lock (_gate)
            {
                return _records
                    .Where(r => r.Name == instrumentName
                        && HasTag(r.Tags, LatticeMetrics.TagTree, TreeId)
                        && HasTag(r.Tags, LatticeMetrics.TagDecision, decision))
                    .Sum(r => r.Value);
            }
        }

        /// <summary>Whether every record for an instrument carries a tenant dimension.</summary>
        public bool EveryRecordCarriesTenant(string instrumentName)
        {
            lock (_gate)
            {
                var matching = _records.Where(r => r.Name == instrumentName).ToArray();
                return matching.Length > 0
                    && matching.All(r => r.Tags.Any(t => t.Key == LatticeTenantLabel.TagTenant));
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    [Test]
    public async Task Healing_backlog_reports_the_shards_above_the_base_count()
    {
        using var metrics = new HealingMetricRecorder();
        var h = CreateGrain(physicalShardCount: 16, baseShardCount: 4);

        await h.Grain.RunHealingPassAsync();

        Assert.That(metrics.ValuesFor(HealingBacklogInstrument), Is.EqualTo(new long[] { 12 }),
            "16 physical shards against a base of 4 is 12 shards of healing work outstanding");
    }

    [Test]
    public async Task Healing_backlog_reaches_zero_when_the_tree_is_healed()
    {
        // "Trees healed" is read straight off this instrument, so a healed tree
        // must actually publish a zero rather than simply stop reporting.
        using var metrics = new HealingMetricRecorder();
        var h = CreateGrain(physicalShardCount: 4, baseShardCount: 4);

        await h.Grain.RunHealingPassAsync();

        Assert.That(metrics.ValuesFor(HealingBacklogInstrument), Is.EqualTo(new long[] { 0 }));
    }

    [Test]
    public async Task Healing_backlog_is_published_on_every_sweep()
    {
        using var metrics = new HealingMetricRecorder();
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        LoadForeground(h, 8, opsPerShard: 5_000);

        await h.Grain.RunHealingPassAsync();
        await h.Grain.RunHealingPassAsync();
        await h.Grain.RunHealingPassAsync();

        Assert.That(metrics.ValuesFor(HealingBacklogInstrument), Is.EqualTo(new long[] { 6, 6, 6 }),
            "a tree held back by backpressure must still report its outstanding damage");
    }

    [Test]
    public async Task Healing_backlog_is_published_even_when_the_kill_switch_is_off()
    {
        // An operator who disabled healing still needs to see what disabling it
        // is costing them.
        using var metrics = new HealingMetricRecorder();
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { ShardHealingEnabled = false });

        await h.Grain.RunHealingPassAsync();

        Assert.That(metrics.ValuesFor(HealingBacklogInstrument), Is.EqualTo(new long[] { 6 }));
    }

    [Test]
    public async Task Every_sweep_publishes_exactly_one_decision()
    {
        using var metrics = new HealingMetricRecorder();
        var h = CreateGrain(physicalShardCount: 4, baseShardCount: 4);

        for (var i = 0; i < 5; i++) await h.Grain.RunHealingPassAsync();

        Assert.That(metrics.ValuesFor(HealingDecisionsInstrument), Is.EqualTo(new long[] { 1, 1, 1, 1, 1 }));
    }

    [TestCase("not_over_split")]
    [TestCase("disabled")]
    [TestCase("admission_closed")]
    [TestCase("skewed_load")]
    [TestCase("split_in_flight")]
    [TestCase("tree_maintenance")]
    [TestCase("backpressure")]
    [TestCase("admitted")]
    public async Task Each_decision_is_published_under_its_own_tag(string decision)
    {
        using var metrics = new HealingMetricRecorder();
        var options = new LatticeOptions();
        Harness h;

        switch (decision)
        {
            case "not_over_split":
                h = CreateGrain(physicalShardCount: 4, baseShardCount: 4, options: options);
                break;
            case "disabled":
                options.ShardHealingEnabled = false;
                h = CreateGrain(physicalShardCount: 8, baseShardCount: 2, options: options);
                break;
            case "admission_closed":
                options.MaxConcurrentShardConsolidations = 0;
                h = CreateGrain(physicalShardCount: 8, baseShardCount: 2, options: options);
                break;
            case "skewed_load":
                h = CreateGrain(physicalShardCount: 8, baseShardCount: 2, options: options);
                LoadUniformly(h, 8, opsPerShard: 10);
                h.ShardOf(0).GetHotnessAsync().Returns(new ShardHotness
                {
                    Reads = 400, Writes = 0, Window = TimeSpan.FromSeconds(1),
                });
                break;
            case "split_in_flight":
                h = CreateGrain(physicalShardCount: 8, baseShardCount: 2, options: options);
                h.ShardOf(2).IsSplittingAsync().Returns(true);
                break;
            case "tree_maintenance":
                h = CreateGrain(physicalShardCount: 8, baseShardCount: 2, options: options);
                h.Lattice.IsMergeCompleteAsync().Returns(false);
                break;
            case "backpressure":
                h = CreateGrain(physicalShardCount: 8, baseShardCount: 2, options: options);
                LoadForeground(h, 8, opsPerShard: 5_000);
                break;
            default:
                h = CreateGrain(physicalShardCount: 8, baseShardCount: 2, options: options);
                break;
        }

        await h.Grain.RunHealingPassAsync();

        Assert.That(metrics.TotalForDecision(HealingDecisionsInstrument, decision), Is.EqualTo(1),
            $"the sweep did not publish decision '{decision}'; it reported {h.State.State.LastDecision}");
    }

    [Test]
    public async Task At_capacity_and_no_foldable_pair_are_published_under_their_own_tags()
    {
        using var metrics = new HealingMetricRecorder();

        var atCapacity = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });
        MarkInFlight(atCapacity, donor: 7, survivor: 6);
        await atCapacity.Grain.RunHealingPassAsync();

        var noPair = CreateGrain(physicalShardCount: 8, baseShardCount: 2,
            options: new LatticeOptions { MaxConcurrentShardConsolidations = 2 },
            existingState: new ShardHealingOrchestratorState { InFlightDonorShardIndices = [7] });
        noPair.ConsolidationOf(7).GetProgressAsync()
            .Returns<Task<ShardConsolidationProgress>>(_ => throw new TimeoutException("silo unreachable"));
        await noPair.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(metrics.TotalForDecision(HealingDecisionsInstrument, "at_capacity"), Is.EqualTo(1));
            Assert.That(metrics.TotalForDecision(HealingDecisionsInstrument, "no_foldable_pair"), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Cooldown_is_published_under_its_own_tag()
    {
        using var metrics = new HealingMetricRecorder();
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        h.ShardOf(3).IsSplittingAsync().Returns(true);
        await h.Grain.RunHealingPassAsync();

        h.ShardOf(3).IsSplittingAsync().Returns(false);
        await h.Grain.RunHealingPassAsync();

        Assert.That(metrics.TotalForDecision(HealingDecisionsInstrument, "cooldown"), Is.EqualTo(1));
    }

    [Test]
    public async Task Both_healing_instruments_carry_a_derived_tenant_dimension()
    {
        // Neither instrument is a platform sentinel: healing is a property of a
        // specific tenant's tree, so both must be visible to a tenant-scoped
        // telemetry query rather than being attributed to the platform.
        using var metrics = new HealingMetricRecorder();
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);

        await h.Grain.RunHealingPassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(metrics.EveryRecordCarriesTenant(HealingBacklogInstrument), Is.True);
            Assert.That(metrics.EveryRecordCarriesTenant(HealingDecisionsInstrument), Is.True);
        });
    }

    [Test]
    public async Task Healing_publishes_no_second_reclaim_counter()
    {
        // Shards reclaimed is derived from the consolidation coordinator's own
        // commit counter, which fires once per durably committed fold. The
        // orchestrator must not publish a competing count of the same event:
        // it starts folds, and a started fold is not a reclaimed shard.
        using var metrics = new HealingMetricRecorder();
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);

        await h.Grain.RunHealingPassAsync();

        Assert.That(metrics.ValuesFor("orleans.lattice.shard.consolidations_committed"), Is.Empty,
            "admitting a fold must not increment the commit counter; only a durable commit may");
    }
}
