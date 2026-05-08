using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Verifies the bootstrap → incremental causal handoff contract that
/// the receiver-side bootstrap state machine (snapshot pin via
/// <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>),
/// the per-origin high-water-mark dedupe, and the causal-plus
/// dependency check collectively define but no single component
/// asserts end-to-end.
/// <para>
/// The mechanism itself is shipped by:
/// </para>
/// <list type="bullet">
/// <item><description>
/// the bootstrap coordinator grain calling
/// <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/> on
/// transition <c>ApplyingSnapshot → IncrementalHandoff</c>
/// (see <c>LatticeBootstrapCoordinatorGrainTests</c>);
/// </description></item>
/// <item><description>
/// the canonical applier consulting the per-origin diagonal first and
/// short-circuiting re-deliveries that are dominated by the pinned
/// frontier on the diagonal (see <c>ReplicationApplierTests</c>);
/// </description></item>
/// <item><description>
/// the canonical applier consulting the full local vector clock for
/// any entry carrying a non-empty <see cref="WalRecord.VectorClock"/>
/// and parking unsatisfied entries in a per-tree FIFO buffer (see
/// <c>ReplicationApplierTests.Causal</c>);
/// </description></item>
/// <item><description>
/// the snapshot stream carrying the producer's causal-stable frontier
/// alongside the as-of HLC so the receiver pins both
/// (see <c>SnapshotStream.CausalStableFrontier</c>).
/// </description></item>
/// </list>
/// <para>
/// These tests pin the post-handoff contract: with a frontier already
/// pinned (simulating the bootstrap state machine's transition into
/// <c>LiveIncremental</c>), incremental entries follow one of four
/// paths — HWM-dedup for entries whose VC is dominated by the
/// frontier, direct apply for entries whose declared dependencies are
/// already satisfied by the pinned frontier, park-then-unblock for
/// entries whose dependencies are not yet satisfied, and DLQ-with-tag
/// <see cref="LatticeReplicationMetrics.ReasonHlcSkew"/> when the
/// transient catch-up window saturates the bounded buffer.
/// </para>
/// </summary>
[TestFixture]
public partial class BootstrapCausalHandoffTests
{
    private const string Tree = "boot-handoff";
    private const string LocalCluster = "site-c";
    private const string OriginA = "site-a";
    private const string OriginB = "site-b";
    private const string OriginD = "site-d";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static VersionVector Vector(params (string Origin, HybridLogicalClock Clock)[] entries)
    {
        var v = new VersionVector();
        foreach (var (origin, clock) in entries)
        {
            v.Entries[origin] = clock;
        }
        return v;
    }

    private static WalRecord SetEntry(
        string key,
        HybridLogicalClock ts,
        string origin,
        VersionVector? vc = null,
        byte[]? value = null) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = key,
        Value = value ?? new byte[] { 1, 2, 3 },
        Timestamp = ts,
        OriginClusterId = origin,
        VectorClock = vc,
    };

    private static WalRecord DeleteEntry(
        string key,
        HybridLogicalClock ts,
        string origin,
        VersionVector? vc = null) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Delete,
        Key = key,
        Timestamp = ts,
        IsTombstone = true,
        OriginClusterId = origin,
        VectorClock = vc,
    };

    private static WalRecord RangeDeleteEntry(string startInclusive, string endExclusive, string origin) => new()
    {
        TreeId = Tree,
        Op = MutationKind.DeleteRange,
        Key = startInclusive,
        EndExclusiveKey = endExclusive,
        Timestamp = HybridLogicalClock.Zero,
        IsTombstone = true,
        OriginClusterId = origin,
    };

    private sealed class HandoffHarness
    {
        public required ReplicationApplier Applier { get; init; }
        public required IGrainFactory Factory { get; init; }
        public required IOptionsMonitor<LatticeReplicationOptions> Monitor { get; init; }
        public required IReplicationApplyGrain Apply { get; init; }
        public required IReplicationHighWaterMarkGrain Hwm { get; init; }
        public required IReplicationDeadLetterGrain Dlq { get; init; }
        public required Dictionary<string, HybridLogicalClock> HwmRows { get; init; }
        public required VersionVector LocalVc { get; init; }
        public required List<(WalRecord Entry, string ReasonTag)> Parked { get; init; }
    }

    private static HandoffHarness CreateHarness(LatticeReplicationOptions? options = null)
    {
        var rows = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        var localVc = new VersionVector();
        var parked = new List<(WalRecord, string)>();

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
                return Task.FromResult(rows.TryGetValue(origin, out var v) ? v : HybridLogicalClock.Zero);
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
                    if (!localVc.Entries.TryGetValue(origin, out var existing) || candidate > existing)
                    {
                        localVc.Entries[origin] = candidate;
                    }
                    return Task.FromResult(true);
                }
                return Task.FromResult(false);
            });

        hwm.GetVectorAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var clone = new VersionVector();
                foreach (var (k, v) in localVc.Entries)
                {
                    clone.Entries[k] = v;
                }
                return Task.FromResult(clone);
            });

        // PinSnapshotAsync overwrites both the per-origin diagonal rows
        // and the local vector clock — exactly the persistence behaviour
        // R-081's IReplicationHighWaterMarkGrain.PinSnapshotAsync does
        // against real grain state.
        hwm.PinSnapshotAsync(
                Arg.Any<HybridLogicalClock>(),
                Arg.Any<VersionVector>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var frontier = (VersionVector)call[1];
                rows.Clear();
                localVc.Entries.Clear();
                foreach (var (origin, clock) in frontier.Entries)
                {
                    rows[origin] = clock;
                    localVc.Entries[origin] = clock;
                }
                return Task.CompletedTask;
            });

        dlq.EnqueueAsync(
                Arg.Any<WalRecord>(),
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                parked.Add(((WalRecord)call[0], (string)call[3]));
                return Task.FromResult((long)parked.Count);
            });

        var resolved = options ?? new LatticeReplicationOptions { ClusterId = LocalCluster };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(resolved);
        monitor.Get(Arg.Any<string>()).Returns(resolved);

        return new HandoffHarness
        {
            Applier = new ReplicationApplier(factory, monitor, new LocalVectorClockCache(factory)),
            Factory = factory,
            Monitor = monitor,
            Apply = apply,
            Hwm = hwm,
            Dlq = dlq,
            HwmRows = rows,
            LocalVc = localVc,
            Parked = parked,
        };
    }

    /// <summary>
    /// Behaviour 1 (spec): incremental entries whose
    /// <see cref="WalRecord.VectorClock"/> is dominated by the
    /// pinned frontier are HWM-deduplicated as
    /// already-applied-via-snapshot — no buffering, no re-merge. Pins
    /// the cross-origin VC-dominated case routes through the same fast
    /// path the per-origin HWM check already provides for the
    /// diagonal.
    /// </summary>
    [Test]
    public async Task After_pin_incremental_entry_below_frontier_is_dedup_via_hwm_without_buffering()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)), (OriginB, Hlc(200)));

        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // Entry from origin-A at HLC 50 (below the pinned diagonal of
        // 100) carrying a VC slot at or below the frontier on every
        // origin — i.e. the snapshot already covers it.
        var entry = SetEntry("k1", Hlc(50), OriginA, Vector((OriginA, Hlc(50)), (OriginB, Hlc(150))));

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False, "Entry below pinned frontier must not re-apply.");
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(100)),
                "HWM dedup must report the pinned diagonal as the HWM, not the entry's own HLC.");
            Assert.That(h.Parked, Is.Empty, "HWM-dedup must not park the entry.");
        });

        // The applier must short-circuit before reaching the apply
        // grain — otherwise the cross-origin VC-dominated path would
        // re-merge work the snapshot already covered.
        await h.Apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }

    /// <summary>
    /// Behaviour 2 (spec): incremental entries with at least one origin
    /// component above the pinned frontier whose dependencies are
    /// satisfied by the frontier apply directly — no buffering, no
    /// DLQ. This is the steady-state path immediately after handoff.
    /// </summary>
    [Test]
    public async Task After_pin_incremental_entry_above_frontier_with_satisfied_deps_applies_directly()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)), (OriginB, Hlc(200)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // Entry from origin-A at HLC 150 (above origin-A's diagonal of
        // 100) whose declared dep on origin-B (200) is exactly the
        // pinned diagonal — satisfied.
        var entry = SetEntry("k2", Hlc(150), OriginA, Vector((OriginA, Hlc(150)), (OriginB, Hlc(200))));

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True, "Above-frontier entry with satisfied deps must apply.");
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(150)), "HWM must advance to the applied entry's HLC.");
            Assert.That(h.Parked, Is.Empty, "Direct apply must not enqueue the DLQ.");
        });

        await h.Apply.Received(1).ApplySetAsync("k2", Arg.Any<byte[]>(), Hlc(150), OriginA, null, 0);
    }

    /// <summary>
    /// Behaviour 3 (spec): incremental entries with at least one origin
    /// component above the pinned frontier whose dependencies are not
    /// yet satisfied park in the bounded causal-apply buffer and
    /// unblock as the missing predecessor lands. Pins the second half
    /// of the contract: a bootstrap completing into a peer with
    /// concurrent in-flight writes elsewhere on the topology can
    /// observe entries whose VC carries dependencies the snapshot did
    /// not yet cover; those entries must park rather than apply
    /// out-of-order.
    /// </summary>
    [Test]
    public async Task After_pin_incremental_entry_above_frontier_with_unsatisfied_deps_parks_then_unblocks()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)), (OriginB, Hlc(200)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // First entry: from origin-A at HLC 150 with a forward
        // dependency on origin-B(500). The pinned frontier has
        // origin-B at 200, so the dependency is unsatisfied — entry
        // must park.
        var blocked = SetEntry("k3", Hlc(150), OriginA, Vector((OriginA, Hlc(150)), (OriginB, Hlc(500))));

        var blockedResult = await h.Applier.ApplyAsync(blocked);

        Assert.Multiple(() =>
        {
            Assert.That(blockedResult.Applied, Is.False, "Unsatisfied-deps entry must not apply on first delivery.");
            Assert.That(h.Parked, Is.Empty, "Parked-in-buffer is not the DLQ; the DLQ must be untouched.");
        });
        await h.Apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);

        // Now the missing predecessor lands: an origin-B entry at
        // HLC 500 with no forward deps (origin-B's diagonal climb).
        // This advance unblocks the parked entry, which must drain
        // and apply in the same call.
        var satisfier = SetEntry("k3-dep", Hlc(500), OriginB, Vector((OriginB, Hlc(500))));

        var satResult = await h.Applier.ApplyAsync(satisfier);

        Assert.That(satResult.Applied, Is.True, "Predecessor entry must apply directly.");

        // Both entries must now have flowed through the apply grain:
        // the predecessor first, the unblocked drain immediately after.
        await h.Apply.Received(1).ApplySetAsync("k3-dep", Arg.Any<byte[]>(), Hlc(500), OriginB, null, 0);
        await h.Apply.Received(1).ApplySetAsync("k3", Arg.Any<byte[]>(), Hlc(150), OriginA, null, 0);
        Assert.That(h.Parked, Is.Empty, "A clean unblock must not engage the DLQ.");
    }

    /// <summary>
    /// Behaviour 4 (spec): overflow during the transient catch-up
    /// window — the bounded buffer hits
    /// <see cref="LatticeReplicationOptions.CausalBufferMaxEntries"/>
    /// while the peer is still draining the gap between snapshot and
    /// live — routes the oldest parked entry through the DLQ with
    /// reason tag <see cref="LatticeReplicationMetrics.ReasonHlcSkew"/>.
    /// Pins the last contract bullet: the operator playbook for
    /// "I bootstrapped under heavy concurrent write load" is to
    /// inspect the DLQ and replay any entries that landed there once
    /// the gap closes.
    /// </summary>
    [Test]
    public async Task After_pin_buffer_overflow_routes_oldest_blocked_entry_to_dlq_with_hlc_skew_reason()
    {
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            CausalBufferMaxEntries = 2,
        };
        var h = CreateHarness(options);
        var frontier = Vector((OriginA, Hlc(100)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // Park three entries from origin-B (so the per-origin HWM
        // diagonal stays at zero for origin-B; HWM-dedup never fires)
        // each declaring a forward dep on origin-A above the pinned
        // diagonal — all three are unsatisfied. The third forces an
        // eviction of the oldest blocked entry to the DLQ.
        var e1 = SetEntry("k-overflow-1", Hlc(10), OriginB, Vector((OriginA, Hlc(999, 1)), (OriginB, Hlc(10))));
        var e2 = SetEntry("k-overflow-2", Hlc(20), OriginB, Vector((OriginA, Hlc(999, 2)), (OriginB, Hlc(20))));
        var e3 = SetEntry("k-overflow-3", Hlc(30), OriginB, Vector((OriginA, Hlc(999, 3)), (OriginB, Hlc(30))));

        await h.Applier.ApplyAsync(e1);
        await h.Applier.ApplyAsync(e2);
        await h.Applier.ApplyAsync(e3);

        Assert.Multiple(() =>
        {
            Assert.That(h.Parked, Has.Count.EqualTo(1),
                "Exactly one overflow eviction must reach the DLQ when capacity is 2 and three entries park.");
            Assert.That(h.Parked[0].Entry.Key, Is.EqualTo("k-overflow-1"),
                "FIFO eviction must drop the oldest parked entry.");
            Assert.That(h.Parked[0].ReasonTag, Is.EqualTo(LatticeReplicationMetrics.ReasonHlcSkew),
                "Overflow reason tag must be hlc_skew so operators can alert on causal-skew DLQ growth distinctly from schema faults.");
        });

        // The two surviving parked entries (k-overflow-2, k-overflow-3)
        // are still buffered — none of the three has reached the apply
        // grain.
        await h.Apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }
}