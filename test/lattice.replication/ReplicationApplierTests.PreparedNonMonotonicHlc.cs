using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Phase D1c regression coverage for the producer-side batched saga
/// shape (parallel cross-leaf <c>lattice.SetManyAsync</c> fan-out).
/// Under D1c each touched leaf advances its own independent
/// <see cref="HybridLogicalClock"/> while authoring its slice of the
/// saga's prepared writes, so two leaves participating in the same
/// saga can produce prepared <see cref="WalRecord"/>s with
/// non-monotonic <see cref="WalRecord.Timestamp"/> values that
/// interleave on the shared per-tree WAL partition. The replication
/// pipeline must deliver and apply every such prepared write
/// regardless of HLC ordering - the per-tx pending bucket and the
/// matching terminal mark are the authoritative atomic-visibility
/// gate; dropping a prepared write by HLC dedup, HLC cursor filter,
/// or causal-park gate would leave the receiver's per-tx pending
/// bucket with a strict subset of the saga's keys and produce a
/// partial-saga view when the terminal flips.
/// <para>
/// These tests pin the four bypasses Phase D1c added against
/// regression:
/// <list type="bullet">
///   <item><description>
///   <see cref="ReplicationApplier.ApplyAsync(WalRecord, CancellationToken)"/>
///   bypasses the per-origin HWM dedup for
///   <c>IsPrepared &amp;&amp; AtomicBatchSize &gt; 0</c> entries even
///   when their <see cref="WalRecord.Timestamp"/> is at or below
///   the per-origin HWM.
///   </description></item>
///   <item><description>
///   The same applier bypasses the causal-park gate for the same
///   class of entries (parking them would deadlock with
///   not-yet-arrived siblings whose VC dependencies refer to the
///   parked entry's own per-leaf clock).
///   </description></item>
///   <item><description>
///   <see cref="ReplicationApplier.ApplyBatchAsync"/>'s batched
///   per-entry pass mirrors both bypasses (the batched path is the
///   shipper's hot path; see
///   <see cref="ReplicationApplier.Batch"/>).
///   </description></item>
///   <item><description>
///   The producer-side <c>ReplicationShipperGrain</c> bypasses its
///   <c>state.Cursor</c> HLC filter for the same class of entries
///   (covered by the integration suite under
///   <c>PublicReplicationApiContractTests</c>; this test file pins
///   the applier-side contract).
///   </description></item>
/// </list>
/// </para>
/// </summary>
public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyAsync_does_not_dedup_prepared_atomic_batch_entry_with_hlc_at_or_below_hwm()
    {
        // Regression: D1c bug where the receiver-side per-origin HWM
        // dedup dropped a prepared+atomic-batch entry whose
        // (per-leaf-stamped) HLC sat at or below the HWM advanced by
        // an earlier-applied sibling from a different leaf in the
        // same saga. Pre-D1c the saga's per-key SetAsync calls
        // serialised at the leaf, so per-origin HLCs were roughly
        // monotonic across the saga; D1c's batched parallel
        // cross-leaf fan-out breaks that assumption.
        var (applier, _, apply, hwm) = CreateApplier();
        // Receiver's HWM is already at the higher-HLC sibling's
        // timestamp because that sibling applied first.
        var siblingHlc = Hlc(50, 1);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(siblingHlc);
        var txid = Guid.NewGuid();
        // Lower-HLC sibling arriving second: would be dedup'd by the
        // legacy HWM gate without the D1c bypass.
        var lowerHlcSibling = PreparedSetEntry(
            "atom-k0",
            Hlc(20, 0),
            txid,
            atomicBatchSize: 4,
            atomicBatchIndex: 0);

        var result = await applier.ApplyAsync(lowerHlcSibling);

        Assert.That(result.Applied, Is.True,
            "Prepared+atomic-batch entries must NOT be HWM-deduped even when their per-leaf HLC sits below the per-origin HWM - the per-tx pending bucket + terminal mark is the atomic-visibility gate, not HWM.");
        await apply.Received(1).ApplyPreparedSetAsync(
            "atom-k0",
            Arg.Any<byte[]>(),
            Hlc(20, 0),
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            0L,
            txid,
            4,
            0);
    }

    [Test]
    public async Task ApplyAsync_does_not_dedup_prepared_atomic_batch_entry_with_hlc_strictly_below_hwm()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));
        var txid = Guid.NewGuid();
        var entry = PreparedDeleteEntry(
            "atom-k0",
            Hlc(10),
            txid,
            atomicBatchSize: 4,
            atomicBatchIndex: 0);

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyPreparedDeleteAsync(
            "atom-k0",
            Hlc(10),
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            txid,
            4,
            0);
    }

    [Test]
    public async Task ApplyAsync_still_dedups_non_prepared_entry_with_hlc_at_or_below_hwm()
    {
        // Counter-check: the bypass is gated on
        // (IsPrepared && AtomicBatchSize > 0). A plain (non-prepared)
        // entry with HLC at or below HWM must still be dedup'd -
        // otherwise the bypass would silently disable HWM dedup for
        // every entry.
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(50, 1));

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(50, 1)));

        Assert.That(result.Applied, Is.False,
            "Non-prepared entry at or below HWM must still be dedup'd; the D1c bypass applies only to IsPrepared+AtomicBatchSize>0.");
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_still_dedups_prepared_entry_without_atomic_batch_size()
    {
        // The bypass requires BOTH IsPrepared AND AtomicBatchSize > 0.
        // A hypothetical IsPrepared=true entry with AtomicBatchSize=0
        // is a malformed-wire shape (prepared writes always come from
        // a saga and carry the batch size), but pin the conjunction
        // here so a future caller cannot accidentally widen the
        // bypass by stamping IsPrepared on a non-atomic entry.
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(50, 1));
        var txid = Guid.NewGuid();
        var entry = PreparedSetEntry("k", Hlc(20), txid, atomicBatchSize: 0, atomicBatchIndex: 0);

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.False);
    }

    [Test]
    public async Task ApplyAsync_does_not_park_prepared_atomic_batch_entry_with_unsatisfied_causal_dependencies()
    {
        // Regression: D1c bug where a prepared+atomic-batch entry's
        // VectorClock dependency on a sibling's per-leaf clock caused
        // the causal-park gate to buffer it. The siblings whose
        // arrival would advance localVc may themselves be parked
        // behind the same VC frontier (chicken-and-egg deadlock),
        // and neither drains until the matching terminal arrives -
        // but the terminal is gated on every prepared write applying
        // first.
        var (applier, _, apply, hwm) = CreateApplier();
        // VectorClock with a frontier the receiver has NOT yet seen
        // (localVc is empty by default in CreateApplier).
        var vc = new VersionVector();
        vc.Tick("sibling-leaf-author");
        var txid = Guid.NewGuid();
        var entry = PreparedSetEntry(
            "atom-k0",
            Hlc(10),
            txid,
            atomicBatchSize: 4,
            atomicBatchIndex: 0) with { VectorClock = vc };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True,
            "Prepared+atomic-batch entries must NOT be causally parked - parking would deadlock with not-yet-arrived siblings whose VC dependencies point back at the parked entry's own per-leaf clock.");
        await apply.Received(1).ApplyPreparedSetAsync(
            "atom-k0",
            Arg.Any<byte[]>(),
            Hlc(10),
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            0L,
            txid,
            4,
            0);
    }

    [Test]
    public async Task ApplyBatchAsync_does_not_dedup_or_park_prepared_atomic_batch_entries_with_non_monotonic_hlcs()
    {
        // Batched-applier regression: the same two bypasses must hold
        // on the per-entry pass inside ApplyBatchAsync, which is
        // the shipper's hot path. Drive a batch where a high-HLC
        // saga-A sibling lands before a low-HLC saga-B sibling on
        // different keys - both must apply via the prepared seam
        // even though saga-B's HLC sits below saga-A's HLC (and
        // therefore below the runningHwm that saga-A advanced).
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        var txidA = Guid.NewGuid();
        var txidB = Guid.NewGuid();
        var batch = new List<WalRecord>
        {
            PreparedSetEntry("atom-A-k0", Hlc(100, 0), txidA, atomicBatchSize: 2, atomicBatchIndex: 0),
            PreparedSetEntry("atom-B-k0", Hlc(20, 0),  txidB, atomicBatchSize: 2, atomicBatchIndex: 0),
        };

        await applier.ApplyBatchAsync(batch);

        // Both entries must hit the prepared-set apply seam, neither
        // may be dropped by HWM dedup or parked.
        await apply.Received(1).ApplyPreparedSetAsync(
            "atom-A-k0",
            Arg.Any<byte[]>(),
            Hlc(100, 0),
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            0L,
            txidA,
            2,
            0);
        await apply.Received(1).ApplyPreparedSetAsync(
            "atom-B-k0",
            Arg.Any<byte[]>(),
            Hlc(20, 0),
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            0L,
            txidB,
            2,
            0);
    }

    [Test]
    public async Task ApplyBatchAsync_does_not_park_prepared_atomic_batch_entries_with_unsatisfied_causal_dependencies()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var vc = new VersionVector();
        vc.Tick("sibling-leaf-author");
        var txid = Guid.NewGuid();
        var batch = new List<WalRecord>
        {
            PreparedSetEntry("atom-k0", Hlc(10), txid, atomicBatchSize: 2, atomicBatchIndex: 0) with { VectorClock = vc },
            PreparedSetEntry("atom-k1", Hlc(11), txid, atomicBatchSize: 2, atomicBatchIndex: 1) with { VectorClock = vc },
        };

        await applier.ApplyBatchAsync(batch);

        await apply.Received(1).ApplyPreparedSetAsync(
            "atom-k0",
            Arg.Any<byte[]>(),
            Hlc(10),
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            0L,
            txid,
            2,
            0);
        await apply.Received(1).ApplyPreparedSetAsync(
            "atom-k1",
            Arg.Any<byte[]>(),
            Hlc(11),
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            0L,
            txid,
            2,
            1);
    }
}
