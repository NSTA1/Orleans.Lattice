using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Receiver-side causal-plus dependency check + bounded-buffer tests
/// for the canonical <see cref="ReplicationApplier"/>.
/// </summary>
public partial class ReplicationApplierTests
{
    private const string OriginC = "site-c";

    private static VersionVector Vector(params (string Origin, HybridLogicalClock Clock)[] entries)
    {
        var v = new VersionVector();
        foreach (var (o, c) in entries)
        {
            v.Entries[o] = c;
        }
        return v;
    }

    private sealed class CausalHarness
    {
        public required ReplicationApplier Applier { get; init; }
        public required IGrainFactory Factory { get; init; }
        public required IReplicationApplyGrain Apply { get; init; }
        public required IReplicationHighWaterMarkGrain Hwm { get; init; }
        public required IReplicationDeadLetterGrain Dlq { get; init; }
        public required Dictionary<string, HybridLogicalClock> HwmRows { get; init; }
        public required VersionVector Vc { get; init; }
    }

    private static CausalHarness CreateCausalHarness(LatticeReplicationOptions? options = null)
    {
        var rows = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        var vc = new VersionVector();

        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();

        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree).Returns(dlq);

        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var origin = (string)call[0];
                return Task.FromResult(
                    rows.TryGetValue(origin, out var v) ? v : HybridLogicalClock.Zero);
            });
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var origin = (string)call[0];
                var candidate = (HybridLogicalClock)call[1];
                var current = rows.TryGetValue(origin, out var v) ? v : HybridLogicalClock.Zero;
                if (candidate > current)
                {
                    rows[origin] = candidate;
                    if (!vc.Entries.TryGetValue(origin, out var existing) || candidate > existing)
                    {
                        vc.Entries[origin] = candidate;
                    }
                    return Task.FromResult(true);
                }
                return Task.FromResult(false);
            });
        hwm.GetVectorAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                // Return a defensive copy each call (matches grain contract).
                var clone = new VersionVector();
                foreach (var (k, v) in vc.Entries)
                {
                    clone.Entries[k] = v;
                }
                return Task.FromResult(clone);
            });

        var resolved = options ?? new LatticeReplicationOptions { ClusterId = LocalCluster };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(resolved);
        monitor.Get(Arg.Any<string>()).Returns(resolved);

        return new CausalHarness
        {
            Applier = new ReplicationApplier(factory, monitor, new LocalVectorClockCache(factory)),
            Factory = factory,
            Apply = apply,
            Hwm = hwm,
            Dlq = dlq,
            HwmRows = rows,
            Vc = vc,
        };
    }

    [Test]
    public async Task ApplyAsync_applies_when_vector_clock_dependencies_satisfied()
    {
        var h = CreateCausalHarness();
        // Local already has site-c at tick 50.
        h.HwmRows[OriginC] = Hlc(50);
        h.Vc.Entries[OriginC] = Hlc(50);

        var entry = SetEntry("k", Hlc(100)) with
        {
            VectorClock = Vector((OriginC, Hlc(50))),
        };

        var result = await h.Applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await h.Apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>());
        await h.Dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
    }

    [Test]
    public async Task ApplyAsync_parks_entry_when_vector_clock_dependency_missing()
    {
        var h = CreateCausalHarness();

        var entry = SetEntry("k", Hlc(100)) with
        {
            VectorClock = Vector((OriginC, Hlc(50))),
        };

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await h.Apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await h.Hwm.DidNotReceive().TryAdvanceAsync(RemoteCluster, Hlc(100), Arg.Any<CancellationToken>());
        // No DLQ enqueue under the cap.
        await h.Dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
    }

    [Test]
    public async Task ApplyAsync_skips_dependency_check_when_vector_clock_is_null()
    {
        // Legacy peers and pre-causal-plus entries decode VectorClock as
        // null and must continue applying on the existing HWM-only path.
        var h = CreateCausalHarness();

        var entry = SetEntry("k", Hlc(7)) with { VectorClock = null };

        var result = await h.Applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await h.Hwm.DidNotReceiveWithAnyArgs().GetVectorAsync(default);
    }

    [Test]
    public async Task ApplyAsync_skips_dependency_check_when_vector_clock_is_empty()
    {
        var h = CreateCausalHarness();

        var entry = SetEntry("k", Hlc(7)) with { VectorClock = new VersionVector() };

        var result = await h.Applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await h.Hwm.DidNotReceiveWithAnyArgs().GetVectorAsync(default);
    }

    [Test]
    public async Task ApplyAsync_drains_buffered_entries_when_dependency_arrives()
    {
        var h = CreateCausalHarness();

        // Step 1: deliver an entry that depends on site-c@50. It parks.
        var blocked = SetEntry("k1", Hlc(100)) with
        {
            VectorClock = Vector((OriginC, Hlc(50))),
        };
        var first = await h.Applier.ApplyAsync(blocked);
        Assert.That(first.Applied, Is.False);

        // Step 2: deliver the satisfying site-c@50 entry. Its apply
        // advances the local VC and the drain unblocks k1.
        var satisfier = SetEntry("k2", Hlc(50), origin: OriginC);
        var second = await h.Applier.ApplyAsync(satisfier);

        Assert.That(second.Applied, Is.True);
        // Both entries must have applied through the apply grain.
        await h.Apply.Received(1).ApplySetAsync("k1", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>());
        await h.Apply.Received(1).ApplySetAsync("k2", Arg.Any<byte[]>(), Hlc(50), OriginC, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_routes_overflow_eviction_to_dead_letter_queue()
    {
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            CausalBufferMaxEntries = 2,
            CausalBufferMaxBytes = 1 << 20,
        };
        var h = CreateCausalHarness(options);

        // All three entries are blocked on site-c@99 — they all park.
        for (var i = 0; i < 3; i++)
        {
            var entry = SetEntry($"k{i}", Hlc(100 + i)) with
            {
                VectorClock = Vector((OriginC, Hlc(99))),
            };
            await h.Applier.ApplyAsync(entry);
        }

        // The third park evicted the first parked entry (k0) and routed
        // it to the DLQ with reason hlc_skew.
        await h.Dlq.Received(1).EnqueueAsync(
            Arg.Is<WalRecord>(e => e.Key == "k0"),
            Arg.Any<string>(),
            Arg.Any<int>(),
            LatticeReplicationMetrics.ReasonHlcSkew,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_drains_chain_of_dependent_entries_in_one_call()
    {
        // Park two entries chained on site-c: k1 needs site-c@30 and
        // k2 needs site-c@30 too. When the satisfier (site-c@30) lands
        // the drain pass must release both entries in FIFO order.
        var h = CreateCausalHarness();

        var first = SetEntry("k1", Hlc(100)) with
        {
            VectorClock = Vector((OriginC, Hlc(30))),
        };
        var second = SetEntry("k2", Hlc(101)) with
        {
            VectorClock = Vector((OriginC, Hlc(30))),
        };

        await h.Applier.ApplyAsync(first);
        await h.Applier.ApplyAsync(second);

        var satisfier = SetEntry("k0", Hlc(30), origin: OriginC);
        var result = await h.Applier.ApplyAsync(satisfier);

        Assert.That(result.Applied, Is.True);
        await h.Apply.Received(1).ApplySetAsync("k0", Arg.Any<byte[]>(), Hlc(30), OriginC, null, Arg.Any<long>());
        await h.Apply.Received(1).ApplySetAsync("k1", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>());
        await h.Apply.Received(1).ApplySetAsync("k2", Arg.Any<byte[]>(), Hlc(101), RemoteCluster, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_routes_drained_apply_failure_to_dead_letter_queue()
    {
        // Park an entry that depends on site-c@30. When site-c@30
        // arrives, the drain attempts to apply the parked entry. We
        // force ApplySetAsync to throw an InvalidOperationException for
        // that key and assert the failure routes to the DLQ with the
        // schema reason tag (the catch-block in DrainBufferAsync).
        var h = CreateCausalHarness();

        h.Apply.ApplySetAsync("k1", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(),
                Arg.Any<string>(), Arg.Any<VersionVector?>(), Arg.Any<long>())
            .Returns(_ => Task.FromException(new InvalidOperationException("schema mismatch")));

        var blocked = SetEntry("k1", Hlc(100)) with
        {
            VectorClock = Vector((OriginC, Hlc(30))),
        };
        await h.Applier.ApplyAsync(blocked);

        var satisfier = SetEntry("k0", Hlc(30), origin: OriginC);
        await h.Applier.ApplyAsync(satisfier);

        await h.Dlq.Received(1).EnqueueAsync(
            Arg.Is<WalRecord>(e => e.Key == "k1"),
            Arg.Any<string>(),
            Arg.Any<int>(),
            LatticeReplicationMetrics.ReasonSchema,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_records_apply_duration_with_parked_causal_buffer_outcome()
    {
        // A park on the causal-apply buffer is a terminal apply
        // outcome and must record into apply.duration with
        // outcome=parked-causal-buffer.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var h = CreateCausalHarness();

        var entry = SetEntry("k", Hlc(100)) with
        {
            VectorClock = Vector((OriginC, Hlc(50))),
        };
        var result = await h.Applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.False);
        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
            Assert.That(
                only.Tags.Any(t => t.Key == LatticeReplicationMetrics.TagTree
                    && (string?)t.Value == Tree),
                Is.True);
            Assert.That(
                only.Tags.Any(t => t.Key == LatticeReplicationMetrics.TagOutcome
                    && (string?)t.Value == LatticeReplicationMetrics.OutcomeParkedCausalBuffer),
                Is.True);
        });
    }

    [Test]
    public async Task ApplyAsync_records_exactly_one_apply_duration_sample_per_invocation_under_drain_cascade()
    {
        // Pins the doc invariant: a drain cascade triggered by an
        // arriving satisfier rolls the drained-entry work into the
        // satisfier's single success sample. The originally parked
        // entries do not generate additional samples on drain because
        // CausalApplyBuffer.DrainBufferAsync calls ApplyPointAsync
        // directly, NOT ApplyAsync, so the per-invocation try/finally
        // in ApplyAsync only fires once per outer call.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var h = CreateCausalHarness();

        // Park an entry that depends on site-c@50.
        var blocked = SetEntry("k1", Hlc(100)) with
        {
            VectorClock = Vector((OriginC, Hlc(50))),
        };
        var parkResult = await h.Applier.ApplyAsync(blocked);
        Assert.That(parkResult.Applied, Is.False);

        // Deliver the satisfier — its successful apply advances the
        // local VC past site-c@50 and triggers the drain of k1. Both
        // the satisfier and the drained k1 land within the satisfier's
        // single ApplyAsync call, so the histogram observes exactly
        // ONE additional sample (the satisfier's), not two.
        var satisfier = SetEntry("k0", Hlc(50), origin: OriginC);
        var drainResult = await h.Applier.ApplyAsync(satisfier);
        Assert.That(drainResult.Applied, Is.True);

        // Total expected samples: 1 (parked-causal-buffer for the
        // initial park) + 1 (success for the satisfier+drain) = 2.
        // A regression that lifted DrainBufferAsync to call ApplyAsync
        // would produce 3 samples (extra success for the drained k1).
        Assert.That(collector.Measurements, Has.Count.EqualTo(2));
        var outcomes = collector.Measurements
            .Select(m => m.Tags
                .First(t => t.Key == LatticeReplicationMetrics.TagOutcome).Value)
            .ToArray();
        Assert.That(outcomes, Is.EquivalentTo(new object?[]
        {
            LatticeReplicationMetrics.OutcomeParkedCausalBuffer,
            LatticeReplicationMetrics.OutcomeSuccess,
        }));
    }
}
