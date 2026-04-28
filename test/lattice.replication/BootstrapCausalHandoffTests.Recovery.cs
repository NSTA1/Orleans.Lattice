using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Recovery gaps on the post-pin causal handoff path: idempotent
/// re-pin, frontier-lowering re-pin, fresh applier reconstruction
/// (the per-applier in-memory buffer is intentionally not durable),
/// and pin-overwrites-prior-VC.
/// </summary>
public partial class BootstrapCausalHandoffTests
{
    /// <summary>
    /// Recovery 6: pinning the same frontier twice is idempotent —
    /// the second pin neither admits previously-deduped entries nor
    /// changes the dedup verdict for incremental traffic.
    /// </summary>
    [Test]
    public async Task After_pin_re_pinning_same_frontier_is_idempotent_for_dedup_verdict()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)));

        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // Below-diagonal entry must still be HWM-deduped after the
        // second pin — the second pin is a no-op for dedup behaviour.
        var below = SetEntry("k-below", Hlc(50), OriginA);
        var belowResult = await h.Applier.ApplyAsync(below);

        Assert.Multiple(() =>
        {
            Assert.That(belowResult.Applied, Is.False);
            Assert.That(belowResult.HighWaterMark, Is.EqualTo(Hlc(100)));
            Assert.That(h.HwmRows[OriginA], Is.EqualTo(Hlc(100)),
                "Per-origin diagonal must remain at the pinned value after redundant re-pin.");
        });
    }

    /// <summary>
    /// Recovery 7: re-pinning a lower frontier (operator-driven
    /// rollback / restore-from-older-snapshot) overwrites the
    /// per-origin diagonal unconditionally. An entry that was
    /// previously HWM-deduped against the higher frontier must apply
    /// after the lower re-pin.
    /// </summary>
    [Test]
    public async Task After_pin_re_pinning_lower_frontier_admits_previously_deduped_entries()
    {
        var h = CreateHarness();

        // First pin at HLC 200 — a subsequent entry at HLC 50 dedups.
        await h.Hwm.PinSnapshotAsync(Hlc(200), Vector((OriginA, Hlc(200))), CancellationToken.None);
        var firstResult = await h.Applier.ApplyAsync(SetEntry("k-replay", Hlc(50), OriginA));
        Assert.That(firstResult.Applied, Is.False, "Below-diagonal entry dedups under the higher pin.");

        // Operator rolls back: re-pin at HLC 25.
        await h.Hwm.PinSnapshotAsync(Hlc(25), Vector((OriginA, Hlc(25))), CancellationToken.None);

        // The same HLC 50 entry now applies — it is above the new
        // diagonal of 25.
        var secondResult = await h.Applier.ApplyAsync(SetEntry("k-replay", Hlc(50), OriginA));

        Assert.Multiple(() =>
        {
            Assert.That(secondResult.Applied, Is.True, "Entry above the lower re-pinned diagonal must apply.");
            Assert.That(secondResult.HighWaterMark, Is.EqualTo(Hlc(50)));
            Assert.That(h.HwmRows[OriginA], Is.EqualTo(Hlc(50)),
                "Per-origin diagonal must advance from the lowered pin to the applied entry's HLC.");
        });
    }

    /// <summary>
    /// Recovery 8: the per-applier causal-apply buffer is in-memory
    /// and per-instance. A fresh <see cref="ReplicationApplier"/>
    /// constructed over the same factory + options + already-pinned
    /// HWM grain has an empty buffer — a previously-parked entry
    /// re-delivered to the new applier re-parks (no apply, no DLQ).
    /// This pins the recovery contract for a silo restart that
    /// re-activates the applier singleton from clean state.
    /// </summary>
    [Test]
    public async Task After_pin_fresh_applier_instance_starts_with_empty_buffer_and_re_parks_redeliveries()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        var blocked = SetEntry("k-restart", Hlc(150), OriginA, Vector((OriginA, Hlc(150)), (OriginB, Hlc(500))));
        var firstResult = await h.Applier.ApplyAsync(blocked);
        Assert.That(firstResult.Applied, Is.False, "First applier instance parks the unsatisfied entry.");

        // A fresh applier over the same factory + monitor models the
        // post-restart state: the pinned HWM grain state survives, but
        // the in-memory buffer does not. Re-delivering the same entry
        // re-parks it; the apply grain still must not be touched.
        var freshApplier = new ReplicationApplier(h.Factory, h.Monitor);

        var redelivered = await freshApplier.ApplyAsync(blocked);

        Assert.Multiple(() =>
        {
            Assert.That(redelivered.Applied, Is.False, "Re-delivered blocked entry must re-park on the fresh applier.");
            Assert.That(redelivered.HighWaterMark, Is.EqualTo(Hlc(100)),
                "Reported HWM must be the pinned diagonal observed on the surviving HWM grain state.");
            Assert.That(h.Parked, Is.Empty, "Re-parking on a fresh buffer must not engage the DLQ.");
        });
        await h.Apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }

    /// <summary>
    /// Recovery 9: a snapshot pin overwrites the prior incrementally-
    /// built local vector clock — origins that were previously tracked
    /// by incremental traffic but are absent from the new frontier
    /// disappear from both the per-origin diagonal and the local VC.
    /// This is the "snapshot is authoritative" contract.
    /// </summary>
    [Test]
    public async Task After_pin_snapshot_clears_prior_origins_not_present_in_frontier()
    {
        var h = CreateHarness();

        // Build incremental state across two origins via normal apply.
        await h.Applier.ApplyAsync(SetEntry("k-a", Hlc(10), OriginA));
        await h.Applier.ApplyAsync(SetEntry("k-b", Hlc(20), OriginB));

        Assert.That(h.HwmRows.ContainsKey(OriginA), Is.True);
        Assert.That(h.HwmRows.ContainsKey(OriginB), Is.True);

        // Pin a snapshot whose frontier mentions only origin-A: origin-B
        // must drop out of the local VC and the diagonal.
        var frontier = Vector((OriginA, Hlc(100)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(h.HwmRows.ContainsKey(OriginA), Is.True);
            Assert.That(h.HwmRows[OriginA], Is.EqualTo(Hlc(100)));
            Assert.That(h.HwmRows.ContainsKey(OriginB), Is.False,
                "Origin-B was not in the pinned frontier and must no longer appear in the diagonal table.");
            Assert.That(h.LocalVc.Entries.ContainsKey(OriginB), Is.False,
                "Origin-B must also drop from the local vector clock — the snapshot is authoritative.");
        });
    }

    /// <summary>
    /// Recovery: <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
    /// mutates the per-origin diagonal and the local vector clock but does
    /// NOT drain the causal-apply buffer — drain is triggered solely by a
    /// successful <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/>.
    /// Pre-pin parked entries survive the pin overwrite and drain on the
    /// next post-pin successful apply when the pinned frontier satisfies
    /// their declared dependencies. Pins the contract that lets the
    /// bootstrap coordinator's <c>ApplyingSnapshot → LiveIncremental</c>
    /// transition safely interleave with concurrent pre-pin live deliveries.
    /// </summary>
    [Test]
    public async Task After_pre_pin_park_pin_does_not_drain_buffer_and_post_pin_advance_releases_entry()
    {
        var h = CreateHarness();

        // Pre-pin park: with empty rows + empty localVc, an entry from
        // origin-A at HLC 150 with a forward dep on origin-B@500 parks
        // (HWM check passes against Zero; dep check fails against the
        // empty localVc).
        var blocked = SetEntry("k-pre-pin", Hlc(150), OriginA, Vector((OriginA, Hlc(150)), (OriginB, Hlc(500))));
        var parkResult = await h.Applier.ApplyAsync(blocked);

        Assert.That(parkResult.Applied, Is.False, "Pre-pin entry with unsatisfied deps must park.");
        await h.Apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);

        // Pin a frontier that already satisfies the parked entry's dep
        // on origin-B@500. The pin overwrites rows + localVc but must
        // NOT, by itself, trigger a drain — drain is gated on
        // TryAdvanceAsync returning advanced=true.
        var frontier = Vector((OriginA, Hlc(100)), (OriginB, Hlc(500)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        await h.Apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        Assert.That(h.Parked, Is.Empty, "Pin must not route the parked entry to the DLQ either.");

        // Post-pin advance via an unrelated satisfier from a third
        // origin (no VC, takes direct-apply path). The TryAdvance this
        // triggers fires the drain that finally releases the pre-pin
        // parked entry — the pinned frontier already covers its
        // origin-B@500 dep, and origin-A is its own origin so the dep
        // check skips it.
        var advancer = SetEntry("k-advance", Hlc(700), OriginD);
        var advanceResult = await h.Applier.ApplyAsync(advancer);

        Assert.That(advanceResult.Applied, Is.True, "Post-pin advancer must apply directly.");
        await h.Apply.Received(1).ApplySetAsync("k-advance", Arg.Any<byte[]>(), Hlc(700), OriginD, null, 0);
        await h.Apply.Received(1).ApplySetAsync("k-pre-pin", Arg.Any<byte[]>(), Hlc(150), OriginA, null, 0);
        Assert.That(h.Parked, Is.Empty, "Clean post-pin drain of a pre-pin parked entry must not engage the DLQ.");
    }
}
