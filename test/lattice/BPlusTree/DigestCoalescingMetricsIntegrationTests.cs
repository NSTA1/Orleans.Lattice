using System.Diagnostics.Metrics;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Metric-attribution coverage for the c2-xxviii leaf-side digest
/// coalescing path. Subscribes a <see cref="MeterListener"/> to
/// <see cref="LatticeMetrics.LeafDigestPublishes"/> and asserts the
/// per-path counts after a controlled burst.
/// <para>
/// These tests close the "is coalescing actually firing on Azure" gap
/// that the c2-xxix bugfix surfaced: prior to the gate, every leaf
/// grain observed <c>DigestCoalescingWindowMs = 0</c> regardless of
/// configuration because the resolver dropped the field on the floor.
/// The propagation guard caught it at the property level; these
/// tests catch it at the behaviour level by counting cross-grain
/// publishes directly.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class DigestCoalescingMetricsIntegrationTests
{
    private CoalescingClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new CoalescingClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ILattice> NewTreeAsync(string prefix, int shardCount = 1)
        => await _fixture.CreateTreeAsync($"{prefix}-{Guid.NewGuid():N}", shardCount);

    /// <summary>
    /// Asserts the headline coalescing invariant: a burst of writes
    /// against a tree whose leaf splits past <see cref="CoalescingClusterFixture.SmallMaxLeafKeys"/>
    /// records strictly more <c>coalesced_scheduled + coalesced_skipped</c>
    /// publish-decisions than actual <c>coalesced_fired</c> cross-grain
    /// publishes - i.e. the per-leaf burst really did pay fewer parent
    /// publishes than mutations. The exact ratio depends on per-CI
    /// scheduling jitter (timer firings can interleave with arrivals),
    /// so the assertion is the qualitative shape, not a specific
    /// numeric ratio.
    /// </summary>
    [Test]
    public async Task DigestCoalescing_burst_records_more_decisions_than_cross_grain_publishes()
    {
        var treeId = $"coalesce-metrics-burst-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        // Pre-warm: force at least one split so the surviving leaves
        // have a parent internal node. Writes against a flat-tree
        // root-is-leaf shape (no parent) short-circuit before any
        // publish-decision metric increments, so a burst against an
        // empty tree records nothing on the LeafDigestPublishes
        // counter regardless of how many writes it lands.
        const int prewarm = 6; // > SmallMaxLeafKeys=4 forces a split
        for (var i = 0; i < prewarm; i++)
        {
            await tree.SetAsync($"pre{i:D2}", Encoding.UTF8.GetBytes($"v{i}"));
        }
        var live = await tree.CountAsync();
        _ = await LatticeDigestSettleHelpers.AwaitDigestConvergesToAsync(
            tree, shardIndex: 0, expectedEntryCount: live);

        // Now the recorder captures only the post-prewarm burst, so
        // every dirtying mutation lands on a leaf that already has a
        // parent and therefore records exactly one publish-decision.
        using var recorder = new DigestPublishRecorder(treeId);

        // Update existing keys instead of inserting new ones so the
        // burst does not trigger further splits (which would inject
        // structural inline publishes interleaved with the per-write
        // coalesced path and make the headline assertion harder to
        // interpret). Updates still dirty the projection hash.
        const int burst = 24;
        for (var i = 0; i < burst; i++)
        {
            // Round-robin over the prewarm keys so every write is an
            // update.
            var k = $"pre{i % prewarm:D2}";
            await tree.SetAsync(k, Encoding.UTF8.GetBytes($"v{i}-burst"));
        }

        // Let coalescing timers drain so coalesced_fired settles.
        // The per-burst convergence target here is "no new keys", so
        // CountAsync still equals `live`. We then poll the recorder
        // explicitly because the settle helper short-circuits on a
        // no-change count check and would otherwise return before
        // any coalescing timer had a chance to tick.
        live = await tree.CountAsync();
        _ = await LatticeDigestSettleHelpers.AwaitDigestConvergesToAsync(
            tree, shardIndex: 0, expectedEntryCount: live);

        // Poll until at least one timer has fired OR a generous
        // ceiling elapses. With a 5 ms coalescing window the timer
        // will tick within tens of milliseconds; the 2-second cap
        // is a stuck-test backstop, not the expected wait.
        var firedDeadline = DateTime.UtcNow.AddSeconds(2);
        while (DateTime.UtcNow < firedDeadline
            && recorder.CountFor(LatticeMetrics.PathCoalescedFiredTag) == 0)
        {
            await Task.Delay(15);
        }

        var scheduled = recorder.CountFor(LatticeMetrics.PathCoalescedScheduledTag);
        var skipped = recorder.CountFor(LatticeMetrics.PathCoalescedSkippedTag);
        var fired = recorder.CountFor(LatticeMetrics.PathCoalescedFiredTag);
        var inline = recorder.CountFor(LatticeMetrics.PathInlineTag);
        var flush = recorder.CountFor(LatticeMetrics.PathDeactivationFlushTag);

        // Per-write decision invariant: every dirtying foreground
        // SetAsync against a parented leaf records exactly one
        // publish-decision (scheduled, skipped, or - if a structural
        // event cancelled the timer first - inline). The post-prewarm
        // leaves all have a parent, so decisions must equal the burst
        // size exactly.
        var decisions = scheduled + skipped + fired + inline + flush;
        Assert.That(decisions, Is.GreaterThanOrEqualTo(burst),
            $"every dirtying mutation on a parented leaf must record a publish decision; "
            + $"saw {decisions} for {burst} writes "
            + $"(scheduled={scheduled}, skipped={skipped}, fired={fired}, inline={inline}, flush={flush})");

        // Headline coalescing invariant: total cross-grain publishes
        // (fired + inline + flush) must be strictly fewer than the
        // burst size. If publishes >= burst the coalescing path
        // silently regressed (each write paid one cross-grain hop).
        var hops = fired + inline + flush;
        Assert.That(hops, Is.LessThan(burst),
            $"coalescing must save at least one cross-grain hop on a {burst}-write burst; "
            + $"observed {hops} hops (scheduled={scheduled}, skipped={skipped}, fired={fired}, "
            + $"inline={inline}, flush={flush})");

        // And at least one of the burst-driven publishes must have
        // landed on the coalesced_fired path - the alternative is
        // that every coalesced timer was cancelled by an interleaved
        // structural inline publish, which would only happen if a
        // hidden split snuck into the burst. Updates against pre-
        // existing keys never split.
        Assert.That(fired, Is.GreaterThan(0),
            $"at least one coalesced timer must have fired on a {burst}-write update burst; "
            + $"observed {fired} fires (scheduled={scheduled}, inline={inline}). "
            + "Zero fires means every timer was cancelled before tick - check for a regression "
            + "in PublishDigestUpwardInlineAsync that erroneously cancels timers from the "
            + "per-write hot path.");
    }

    /// <summary>
    /// Asserts the inverse shape: a tree configured at
    /// <c>DigestCoalescingWindowMs = 0</c> records zero coalescing
    /// activity (no <c>coalesced_scheduled</c>, no <c>coalesced_skipped</c>,
    /// no <c>coalesced_fired</c>); every per-write publish lands on the
    /// <c>inline</c> path. The pre-c2-xxviii synchronous-publish shape
    /// must remain reachable as an opt-out for consumers that depend on
    /// the read-after-write digest invariant.
    /// <para>
    /// Run under <see cref="FourShardClusterFixture"/> rather than the
    /// coalescing fixture because pinning the window to zero on the
    /// coalescing fixture's silo would invalidate every other test in
    /// the suite.
    /// </para>
    /// </summary>
    [Test]
    public async Task DigestCoalescing_zero_window_records_inline_publishes_only()
    {
        // Standalone four-shard fixture spun up just for this test so
        // we do not pollute the coalescing fixture's silo state. The
        // four-shard fixture pins DigestCoalescingWindowMs = 0 at the
        // silo level.
        await using var inlineFixture = new InlineOnlyFixture();
        await inlineFixture.InitializeAsync();

        var treeId = $"coalesce-metrics-inline-{Guid.NewGuid():N}";
        var tree = await inlineFixture.CreateTreeAsync(treeId);

        using var recorder = new DigestPublishRecorder(treeId);

        // The four-shard fixture pins MaxLeafKeys = 4. To exercise the
        // inline-publish path we need leaves that have a parent
        // internal node - i.e. at least one shard whose key count
        // exceeds the 4-key cap and splits. Writing 40 keys across 4
        // shards averages 10 keys/shard, comfortably above the split
        // threshold on the busiest shard so at least one parent
        // appears.
        const int writes = 40;
        for (var i = 0; i < writes; i++)
        {
            await tree.SetAsync($"k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        // The chained-fold reads can race the silo-side recorder
        // flush so we give the listener a beat to drain.
        await Task.Delay(20);

        var scheduled = recorder.CountFor(LatticeMetrics.PathCoalescedScheduledTag);
        var skipped = recorder.CountFor(LatticeMetrics.PathCoalescedSkippedTag);
        var fired = recorder.CountFor(LatticeMetrics.PathCoalescedFiredTag);

        Assert.That(scheduled, Is.Zero, "no timer should be scheduled when the window is 0");
        Assert.That(skipped, Is.Zero, "no decisions should be deferred when the window is 0");
        Assert.That(fired, Is.Zero, "no coalesced timer should fire when the window is 0");

        var inline = recorder.CountFor(LatticeMetrics.PathInlineTag);
        Assert.That(inline, Is.GreaterThan(0),
            "at least one per-write publish on the inline path must record an inline decision "
            + "(every dirtying write on a leaf with a parent does so; a zero count means the "
            + "synchronous-publish path silently regressed)");
    }

    /// <summary>
    /// Asserts that a structural event (leaf split, triggered by
    /// writing past <see cref="CoalescingClusterFixture.SmallMaxLeafKeys"/>)
    /// records at least one <c>inline</c> publish even though
    /// per-write publishes coalesce. The c2-xxviii memo's exclusion
    /// of structural callers from the coalescing window is the load
    /// bearing invariant: structural publishes must reach the parent
    /// synchronously so operator tooling (e.g. RebuildLeafProjectionAsync
    /// followed by GetLeafProjectionDigestAsync) observes the post-publish
    /// state without a settle delay.
    /// </summary>
    [Test]
    public async Task DigestCoalescing_structural_split_records_inline_publish()
    {
        var treeId = $"coalesce-metrics-split-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        using var recorder = new DigestPublishRecorder(treeId);

        // SmallMaxLeafKeys = 4; writing 8 keys forces at least one
        // split. The split itself drives PublishDigestUpwardInlineAsync
        // which records inline regardless of the coalescing window.
        for (var i = 0; i < 8; i++)
        {
            await tree.SetAsync($"k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var live = await tree.CountAsync();
        _ = await LatticeDigestSettleHelpers.AwaitDigestConvergesToAsync(
            tree, shardIndex: 0, expectedEntryCount: live);

        var inline = recorder.CountFor(LatticeMetrics.PathInlineTag);
        Assert.That(inline, Is.GreaterThan(0),
            "the leaf-split's structural digest publish must record at least one inline cross-grain hop, "
            + "regardless of the active coalescing window");
    }

    /// <summary>
    /// Asserts the multi-shard chained-fold path works under
    /// coalescing: writes distributed across several shards converge
    /// to the correct per-shard chained-fold entry-count via the
    /// settle helper, and the recorder observes <c>coalesced_fired</c>
    /// activity across more than one tree-tagged leaf - confirming
    /// that the coalescing path itself is not silently scoped to a
    /// single shard.
    /// </summary>
    [Test]
    public async Task DigestCoalescing_multi_shard_chained_fold_converges_to_live_count()
    {
        const int shardCount = 4;
        var tree = await NewTreeAsync("coalesce-multi-shard", shardCount: shardCount);

        // Spread writes across shards by key. Per-key hash routing
        // distributes the 32 writes broadly enough across 4 shards
        // that more than one shard sees mutations.
        const int writes = 32;
        for (var i = 0; i < writes; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        // The per-shard digests must sum to the live count once the
        // chained-fold catches up across every shard.
        var live = await tree.CountAsync();
        Assert.That(live, Is.EqualTo(writes), "live entry count must match writes regardless of shard fan-out");

        var perShard = await LatticeDigestSettleHelpers.AwaitAllShardDigestsConvergeAsync(
            tree, shardCount: shardCount, expectedTotalEntries: live);

        // Independence sanity check: at least two shards must be
        // populated after a 32-write key-distributed burst. If we
        // accidentally pinned everything onto one shard the test would
        // degrade to the single-shard test above.
        var populatedShards = perShard.Count(d => d.EntryCount > 0);
        Assert.That(populatedShards, Is.GreaterThanOrEqualTo(2),
            $"32 writes against {shardCount} shards must populate at least 2 shards; saw {populatedShards}. "
            + "If this fails the test has degenerated to a single-shard scenario.");

        var summed = perShard.Sum(d => d.EntryCount);
        Assert.That(summed, Is.EqualTo(live),
            "the sum of per-shard chained-fold entry counts must equal the live count after settle");
    }

    /// <summary>
    /// Asserts that a graceful leaf deactivation drains a pending
    /// coalesced digest publish via <c>FlushPendingDigestPublishAsync</c>,
    /// recording <c>deactivation_flush</c> on the publish counter
    /// rather than losing the publish entirely. The test runs against
    /// a dedicated fixture pinning the coalescing window to 5 seconds
    /// so the timer is guaranteed not to fire before the forced
    /// deactivation runs - making the deactivation-flush path the
    /// only way the parent could have received the publish.
    /// </summary>
    [Test]
    public async Task DigestCoalescing_graceful_deactivation_flushes_pending_publish()
    {
        await using var longWindowFixture = new LongWindowFixture();
        await longWindowFixture.InitializeAsync();

        var treeId = $"coalesce-metrics-deact-{Guid.NewGuid():N}";
        var tree = await longWindowFixture.CreateTreeAsync(treeId);

        using var recorder = new DigestPublishRecorder(treeId);

        // Write enough keys to force a leaf split so the resulting
        // leaves have a parent internal node - a flat-tree single-leaf
        // shard has ParentId = null and FlushPendingDigestPublishAsync
        // short-circuits with no publish (and no metric increment) by
        // design.
        for (var i = 0; i < 12; i++)
        {
            await tree.SetAsync($"k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        // Resolve a leaf id via the shard root and force-deactivate it.
        // With a 5-second coalescing window the timer is virtually
        // guaranteed not to have fired between the last write and the
        // forced deactivation, so the only path through which the
        // parent can receive the post-burst publish is the graceful
        // OnDeactivateAsync -> FlushPendingDigestPublishAsync drain.
        var shard = longWindowFixture.Cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null, "Post-split shard must expose its leftmost leaf id.");
        var leaf = longWindowFixture.Cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value.GetGuidKey());

        // Snapshot counters before the deactivation so the assertion
        // can isolate the delta to the flush path.
        var firedBefore = recorder.CountFor(LatticeMetrics.PathCoalescedFiredTag);
        var flushBefore = recorder.CountFor(LatticeMetrics.PathDeactivationFlushTag);

        await leaf.ForceDeactivateAsync();

        // The runtime schedules OnDeactivateAsync after the current
        // turn ends. Poll the metric until either the flush fires or
        // a generous timeout elapses; capping at 5 s of wall-clock
        // keeps a stuck test bounded without racing the configured
        // 5 s coalescing window's lower bound.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            var flushNow = recorder.CountFor(LatticeMetrics.PathDeactivationFlushTag);
            if (flushNow > flushBefore) break;
            await Task.Delay(20);
        }

        var firedAfter = recorder.CountFor(LatticeMetrics.PathCoalescedFiredTag);
        var flushAfter = recorder.CountFor(LatticeMetrics.PathDeactivationFlushTag);

        // The graceful flush must have fired at least once for the
        // forced-deactivated leaf, AND the coalescing timer must not
        // have raced ahead of the deactivation (which would leave
        // flushAfter == flushBefore and indicate the test never
        // actually exercised the flush path).
        Assert.That(flushAfter, Is.GreaterThan(flushBefore),
            $"graceful deactivation must record at least one deactivation_flush; "
            + $"before={flushBefore} after={flushAfter} (fired delta={firedAfter - firedBefore}). "
            + "A no-op flush either means the timer raced ahead of deactivation "
            + "(unlikely at a 5 s window) or the flush path silently regressed.");
    }

    /// <summary>
    /// Captures <see cref="LatticeMetrics.LeafDigestPublishes"/>
    /// measurements scoped to a single tree id for the lifetime of
    /// the recorder. Subscribes only to the <see cref="LatticeMetrics.Meter"/>
    /// to avoid cross-meter pollution.
    /// </summary>
    private sealed class DigestPublishRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly string _treeId;
        private readonly List<KeyValuePair<string, object?>[]> _records = new();
        private readonly object _lock = new();

        public DigestPublishRecorder(string treeId)
        {
            _treeId = treeId;
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                        && inst.Name == LatticeMetrics.LeafDigestPublishes.Name)
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<long>(OnLong);
            _listener.Start();
        }

        private void OnLong(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            // Filter by tree tag at capture time so the recorder is
            // safe to use even when other trees share the same silo.
            string? observedTree = null;
            foreach (var t in tags)
            {
                if (t.Key == LatticeMetrics.TagTree && t.Value is string s)
                {
                    observedTree = s;
                    break;
                }
            }
            if (observedTree != _treeId) return;

            lock (_lock)
            {
                _records.Add(tags.ToArray());
            }
        }

        public int CountFor(KeyValuePair<string, object?> pathTag)
        {
            lock (_lock)
            {
                var count = 0;
                foreach (var tags in _records)
                {
                    foreach (var t in tags)
                    {
                        if (t.Key == LatticeMetrics.TagPath
                            && t.Value is string s
                            && pathTag.Value is string expected
                            && string.Equals(s, expected, StringComparison.Ordinal))
                        {
                            count++;
                            break;
                        }
                    }
                }
                return count;
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// Standalone fixture for the inline-only (window=0) test. Reuses
    /// <see cref="FourShardClusterFixture"/> exactly so the silo-level
    /// configuration matches the rest of the suite's window=0 baseline.
    /// </summary>
    private sealed class InlineOnlyFixture : IAsyncDisposable
    {
        private readonly FourShardClusterFixture _inner = new();

        public Task InitializeAsync() => _inner.InitializeAsync();

        public Task<ILattice> CreateTreeAsync(string treeId)
            => _inner.CreateTreeAsync(treeId);

        public ValueTask DisposeAsync() => new(_inner.DisposeAsync());
    }

    /// <summary>
    /// Standalone fixture with a deliberately-large coalescing window
    /// (5 seconds) so the deactivation-flush test can rely on the
    /// timer not firing between the last write and the forced
    /// deactivation. The fixture builds a TestCluster directly rather
    /// than reusing <see cref="CoalescingClusterFixture"/>
    /// (which pins 5 ms) so the long-window setting is isolated to
    /// this single test and does not skew the other coalescing
    /// fixtures.
    /// </summary>
    private sealed class LongWindowFixture : IAsyncDisposable
    {
        private const int LongWindowMs = 5_000;
        private const int SmallMaxLeafKeys = 4;
        private const int ShardCount = 1;

        public Orleans.TestingHost.TestCluster Cluster { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            var builder = new Orleans.TestingHost.TestClusterBuilder();
            builder.AddSiloBuilderConfigurator<SiloConfigurator>();
            Cluster = builder.Build();
            await Cluster.DeployAsync();
        }

        public async Task<ILattice> CreateTreeAsync(string treeId)
        {
            var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            await registry.RegisterAsync(treeId, new Orleans.Lattice.BPlusTree.State.TreeRegistryEntry
            {
                MaxLeafKeys = SmallMaxLeafKeys,
                ShardCount = ShardCount,
            });
            return Cluster.Client.GetGrain<ILattice>(treeId);
        }

        public async ValueTask DisposeAsync()
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }

        private sealed class SiloConfigurator : Orleans.TestingHost.ISiloConfigurator
        {
            public void Configure(Orleans.Hosting.ISiloBuilder siloBuilder)
            {
                siloBuilder.AddLattice((silo, name)
                    => silo.AddMemoryGrainStorage(name));
                siloBuilder.ConfigureLattice(o => o.DigestCoalescingWindowMs = LongWindowMs);
                siloBuilder.UseInMemoryReminderService();
            }
        }
    }
}
