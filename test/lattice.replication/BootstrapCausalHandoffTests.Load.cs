using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Load and throughput gaps on the post-pin causal handoff path:
/// cascading multi-hop drains, bursty single-satisfier unblocks of
/// many parked entries, byte-cap eviction independent of entry-cap,
/// and mixed below+above+park interleavings within one batch.
/// </summary>
public partial class BootstrapCausalHandoffTests
{
    /// <summary>
    /// Load 10: a three-origin cascading drain where X depends on Y
    /// and Y depends on Z. The arrival of Z must transitively unblock
    /// Y (in the first drain pass) and then X (in the second pass) in
    /// causal order. Pins the fixed-point loop in
    /// <c>ReplicationApplier.DrainBufferAsync</c>.
    /// </summary>
    [Test]
    public async Task After_pin_cascading_dependency_chain_drains_in_causal_order()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)), (OriginB, Hlc(100)), (OriginD, Hlc(100)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // X (origin-A) depends on Y's HLC: origin-B(500).
        var x = SetEntry("k-x", Hlc(150), OriginA, Vector((OriginA, Hlc(150)), (OriginB, Hlc(500))));
        // Y (origin-B) depends on Z's HLC: origin-D(700).
        var y = SetEntry("k-y", Hlc(500), OriginB, Vector((OriginB, Hlc(500)), (OriginD, Hlc(700))));
        // Z (origin-D) has no forward deps.
        var z = SetEntry("k-z", Hlc(700), OriginD, Vector((OriginD, Hlc(700))));

        // X arrives first and parks (origin-B@500 not satisfied).
        var xResult = await h.Applier.ApplyAsync(x);
        Assert.That(xResult.Applied, Is.False);

        // Y arrives next and parks (origin-D@700 not satisfied).
        var yResult = await h.Applier.ApplyAsync(y);
        Assert.That(yResult.Applied, Is.False);

        // Z arrives - applies directly, then triggers a fixed-point
        // drain that releases Y (origin-D now at 700) and then X
        // (origin-B now at 500).
        var zResult = await h.Applier.ApplyAsync(z);

        Assert.That(zResult.Applied, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(h.Parked, Is.Empty, "A clean cascading unblock must not engage the DLQ.");
            Assert.That(h.HwmRows[OriginA], Is.EqualTo(Hlc(150)));
            Assert.That(h.HwmRows[OriginB], Is.EqualTo(Hlc(500)));
            Assert.That(h.HwmRows[OriginD], Is.EqualTo(Hlc(700)));
        });

        await h.Apply.Received(1).ApplySetAsync("k-z", Arg.Any<byte[]>(), Hlc(700), OriginD, null, 0);
        await h.Apply.Received(1).ApplySetAsync("k-y", Arg.Any<byte[]>(), Hlc(500), OriginB, null, 0);
        await h.Apply.Received(1).ApplySetAsync("k-x", Arg.Any<byte[]>(), Hlc(150), OriginA, null, 0);
    }

    /// <summary>
    /// Load 11: a single satisfier unblocks many parked entries that
    /// share the same forward dependency. Pins the buffer's bulk-drain
    /// path and confirms the apply grain is invoked once per parked
    /// entry on a single drain pass.
    /// </summary>
    [Test]
    public async Task After_pin_bursty_drain_unblocks_many_parked_entries_from_one_satisfier()
    {
        const int parkedCount = 100;

        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)), (OriginB, Hlc(100)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // Park 100 origin-B entries each declaring a forward dep on
        // origin-A(500). Default CausalBufferMaxEntries (1024) easily
        // accommodates.
        for (var i = 0; i < parkedCount; i++)
        {
            var entry = SetEntry(
                $"k-burst-{i:D3}",
                Hlc(200 + i),
                OriginB,
                Vector((OriginA, Hlc(500)), (OriginB, Hlc(200 + i))));
            var result = await h.Applier.ApplyAsync(entry);
            Assert.That(result.Applied, Is.False, $"Parked entry {i} must not apply on first delivery.");
        }

        Assert.That(h.Parked, Is.Empty, "Parking must not have engaged the DLQ.");

        // Single satisfier from origin-A unblocks all 100 parked entries.
        var satisfier = SetEntry("k-burst-sat", Hlc(500), OriginA, Vector((OriginA, Hlc(500))));
        var satResult = await h.Applier.ApplyAsync(satisfier);

        Assert.That(satResult.Applied, Is.True, "Satisfier entry must apply.");
        // Apply grain must have been invoked exactly parkedCount + 1 times.
        await h.Apply.Received(parkedCount + 1)
            .ApplySetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<string>(), Arg.Any<VersionVector?>(), Arg.Any<long>());
        Assert.That(h.Parked, Is.Empty, "A clean bulk drain must not engage the DLQ.");
    }

    /// <summary>
    /// Load 12: the byte cap evicts the oldest parked entry
    /// independently of the entry cap. Two ~33 KB-payload entries
    /// fit a 64 KB byte cap with margin (per-entry size estimate is
    /// ~payload + 148 bytes); the third entry triggers a byte-cap
    /// eviction of the first.
    /// </summary>
    [Test]
    public async Task After_pin_buffer_overflow_on_bytes_cap_evicts_oldest_with_hlc_skew_reason()
    {
        // Set entry cap well above the test's three entries so the
        // observed eviction is byte-cap-driven, not entry-cap-driven.
        // CausalBufferMaxBytes minimum is 64 KB.
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            CausalBufferMaxEntries = 100,
            CausalBufferMaxBytes = 65_536,
        };
        var h = CreateHarness(options);
        var frontier = Vector((OriginA, Hlc(100)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // Three ~25 KB-payload entries from origin-B with unsatisfied
        // forward deps on origin-A. EstimateSize per entry ~= 25_746
        // (value + key*2 + 128 overhead); two fit under 65_536, three
        // do not - so adding the third evicts exactly the oldest.
        var payload = new byte[25 * 1024];
        var e1 = SetEntry("k-bytes-1", Hlc(10), OriginB, Vector((OriginA, Hlc(999, 1)), (OriginB, Hlc(10))), payload);
        var e2 = SetEntry("k-bytes-2", Hlc(20), OriginB, Vector((OriginA, Hlc(999, 2)), (OriginB, Hlc(20))), payload);
        var e3 = SetEntry("k-bytes-3", Hlc(30), OriginB, Vector((OriginA, Hlc(999, 3)), (OriginB, Hlc(30))), payload);

        await h.Applier.ApplyAsync(e1);
        await h.Applier.ApplyAsync(e2);
        await h.Applier.ApplyAsync(e3);

        Assert.Multiple(() =>
        {
            Assert.That(h.Parked, Has.Count.EqualTo(1),
                "Byte-cap overflow must evict exactly one entry to the DLQ.");
            Assert.That(h.Parked[0].Entry.Key, Is.EqualTo("k-bytes-1"),
                "FIFO eviction must drop the oldest entry first.");
            Assert.That(h.Parked[0].ReasonTag, Is.EqualTo(LatticeReplicationMetrics.ReasonHlcSkew),
                "Byte-cap overflow uses the same hlc_skew reason as entry-cap overflow.");
        });
    }

    /// <summary>
    /// Load 13: a mixed sequence interleaving (a) below-frontier
    /// HWM-deduped entries, (b) above-frontier-with-satisfied-deps
    /// directly-applied entries, (c) above-frontier-with-unsatisfied-
    /// deps parked entries, and (d) the satisfier that drains the
    /// parked tail - all on a single applier instance - must produce
    /// the per-category outcomes for every entry without cross-talk.
    /// </summary>
    [Test]
    public async Task After_pin_mixed_below_above_park_sequence_classifies_each_entry_independently()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)), (OriginB, Hlc(200)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // Below-frontier dedup (origin-A @ 50, dominated by diagonal 100).
        var below = SetEntry("k-below", Hlc(50), OriginA, Vector((OriginA, Hlc(50))));
        var belowResult = await h.Applier.ApplyAsync(below);

        // Above-frontier with satisfied deps (origin-A @ 150, dep on origin-B@200 == diagonal).
        var directApply = SetEntry("k-direct", Hlc(150), OriginA, Vector((OriginA, Hlc(150)), (OriginB, Hlc(200))));
        var directResult = await h.Applier.ApplyAsync(directApply);

        // Above-frontier with unsatisfied deps (origin-A @ 160, dep on origin-B@500).
        var park = SetEntry("k-park", Hlc(160), OriginA, Vector((OriginA, Hlc(160)), (OriginB, Hlc(500))));
        var parkResult = await h.Applier.ApplyAsync(park);

        // Satisfier (origin-B @ 500, no deps) - drains "k-park".
        var satisfier = SetEntry("k-sat", Hlc(500), OriginB, Vector((OriginB, Hlc(500))));
        var satResult = await h.Applier.ApplyAsync(satisfier);

        Assert.Multiple(() =>
        {
            Assert.That(belowResult.Applied, Is.False, "Below-frontier entry must dedup.");
            Assert.That(belowResult.HighWaterMark, Is.EqualTo(Hlc(100)), "Dedup reports the pinned diagonal.");

            Assert.That(directResult.Applied, Is.True, "Direct-apply entry must apply.");
            Assert.That(directResult.HighWaterMark, Is.EqualTo(Hlc(150)));

            Assert.That(parkResult.Applied, Is.False, "Parked entry must not apply on first delivery.");

            Assert.That(satResult.Applied, Is.True, "Satisfier must apply directly.");
            Assert.That(satResult.HighWaterMark, Is.EqualTo(Hlc(500)));

            Assert.That(h.Parked, Is.Empty, "Mixed clean sequence must not engage the DLQ.");
        });

        // Apply grain receives exactly three calls: direct, satisfier, and the drained park.
        await h.Apply.Received(1).ApplySetAsync("k-direct", Arg.Any<byte[]>(), Hlc(150), OriginA, null, 0);
        await h.Apply.Received(1).ApplySetAsync("k-sat", Arg.Any<byte[]>(), Hlc(500), OriginB, null, 0);
        await h.Apply.Received(1).ApplySetAsync("k-park", Arg.Any<byte[]>(), Hlc(160), OriginA, null, 0);
        await h.Apply.DidNotReceive().ApplySetAsync("k-below", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<string>(), Arg.Any<VersionVector?>(), Arg.Any<long>());
    }
}
