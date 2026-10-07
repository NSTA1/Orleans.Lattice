using Orleans.Lattice.Primitives;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4549: a re-bootstrap must also converge on rows written by a third
/// origin. The export carries, at open, the source's applied frontier per
/// origin. The receiver installs it as a drop floor before the drain, so a
/// third-origin write still in flight cannot resurrect a key the source deleted
/// and reaped, and after the drain it deletes its live third-origin rows the
/// export lacks whose writes the source had applied. Site C is a third cluster
/// whose writes reach both sites through their real appliers.
/// </summary>
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    private static readonly byte[] ThirdValue = [3];

    /// <summary>A third-origin write stamped well before any write either site makes during the test.</summary>
    private static HybridLogicalClock PastHlc(int minutesAgo) =>
        new() { WallClockTicks = DateTime.UtcNow.AddMinutes(-minutesAgo).Ticks };

    private static Task<ApplyResult> ApplyFromSiteCAsync(TestCluster cluster, string tree, string key, HybridLogicalClock timestamp) =>
        Applier(cluster).ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = ThirdValue,
            Timestamp = timestamp,
            OriginClusterId = SiteCClusterId,
        });

    /// <summary>
    /// Applies from outside the bootstrap drain's ambient scope: the drain hook
    /// runs inside the coordinator's flow, where the drain's own bypass is on.
    /// </summary>
    private static async Task<ApplyResult> DeliverFromSiteCOutsideTheDrainAsync(
        TestCluster cluster, string tree, string key, HybridLogicalClock timestamp)
    {
        Task<ApplyResult> delivery;
        using (ExecutionContext.SuppressFlow())
        {
            delivery = Task.Run(() => ApplyFromSiteCAsync(cluster, tree, key, timestamp));
        }

        return await delivery;
    }

    private static Func<RemoteSnapshotMetadata, SnapshotSourceFrontier?> SiteCFrontier(
        HybridLogicalClock lowWatermark,
        HybridLogicalClock[]? held = null,
        Guid? lineage = null) =>
        metadata => new SnapshotSourceFrontier
        {
            Lineage = lineage ?? metadata.OpenGeneration?.Lineage,
            LowWatermarks = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [SiteCClusterId] = lowWatermark },
            Held = new Dictionary<string, HybridLogicalClock[]>(StringComparer.Ordinal) { [SiteCClusterId] = held ?? [] },
        };

    private async Task RebootstrapSiteBAsync(
        string tree,
        Func<RemoteSnapshotMetadata, SnapshotSourceFrontier?> frontier,
        Func<Task>? inDrain = null)
    {
        _openFrontier = frontier;
        _onDrainStarted = inDrain;
        try
        {
            await BootstrapSiteBAsync(tree);
        }
        finally
        {
            _openFrontier = null;
            _onDrainStarted = null;
        }
    }

    [Test]
    public async Task Re_bootstrap_deletes_a_third_origin_key_the_source_applied_then_deleted_and_reaped()
    {
        const string tree = "rsdr-4549-third-delete";
        const string key = "third-deleted";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition: the source applied C's write");
        Assert.That((await ApplyFromSiteCAsync(_siteB, tree, key, written)).Applied, Is.True, "precondition: the receiver applied C's write");

        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);
        Assert.That((await ExportSiteAAsync(tree)).Select(e => e.Key), Has.No.Member(key),
            "precondition: the reaped delete leaves no row in the export");

        await RebootstrapSiteBAsync(tree, SiteCFrontier(HybridLogicalClock.Tick(written)));

        Assert.Multiple(async () =>
        {
            Assert.That(await siteB.GetAsync(key), Is.Null,
                "the source applied C's write and then deleted the key, so the receiver must not keep C's row");
            Assert.That(await siteB.GetAsync("anchor"), Is.EqualTo(new byte[] { 1 }));
        });
    }

    [Test]
    public async Task Bootstrap_drop_floor_stops_an_in_flight_third_origin_write_resurrecting_a_reaped_delete()
    {
        const string tree = "rsdr-4549-in-flight";
        const string key = "third-in-flight";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // C's write reaches the source, which deletes the key and reaps the
        // tombstone; the copy bound for the receiver is still in flight.
        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition: the source applied C's write");
        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);

        ApplyResult? inDrain = null;
        await RebootstrapSiteBAsync(
            tree,
            SiteCFrontier(HybridLogicalClock.Tick(written)),
            async () => inDrain = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, key, written));

        Assert.That(inDrain, Is.Not.Null, "precondition: the delivery landed inside the drain");
        var late = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, key, written);
        Assert.Multiple(async () =>
        {
            Assert.That(inDrain!.Value.Applied, Is.False, "a write below the floor arriving mid-drain is not merged");
            Assert.That(inDrain.Value.Deferred, Is.True, "while the import is open the floor is provisional, so it defers");
            Assert.That(late.Applied, Is.False, "the floor stays in force after the drain");
            Assert.That(late.Deferred, Is.False, "the import closed stable, so the re-shipped write is dropped");
            Assert.That(await siteB.GetAsync(key), Is.Null,
                "the in-flight write must not resurrect a key the source deleted and reaped");
        });
    }

    [Test]
    public async Task A_third_origin_write_the_source_holds_is_neither_dropped_nor_reconciled()
    {
        const string tree = "rsdr-4549-held";
        const string heldKey = "held-at-source";
        const string inDrainKey = "held-in-drain";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // The source holds both writes without applying them, below its low watermark.
        var heldWrite = PastHlc(6);
        var inDrainWrite = PastHlc(5);
        var lowWatermark = PastHlc(4);
        Assert.That((await ApplyFromSiteCAsync(_siteB, tree, heldKey, heldWrite)).Applied, Is.True, "precondition");

        ApplyResult? inDrain = null;
        await RebootstrapSiteBAsync(
            tree,
            SiteCFrontier(lowWatermark, [heldWrite, inDrainWrite]),
            async () => inDrain = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, inDrainKey, inDrainWrite));

        Assert.Multiple(async () =>
        {
            Assert.That(inDrain?.Applied, Is.True, "a held write is not covered by the export, so it must apply");
            Assert.That(await siteB.GetAsync(inDrainKey), Is.EqualTo(ThirdValue));
            Assert.That(await siteB.GetAsync(heldKey), Is.EqualTo(ThirdValue),
                "the export lacks a held write because the source never applied it, so it must not be deleted");
        });
    }

    [Test]
    public async Task An_unstable_export_clears_the_drop_floor_and_infers_no_third_origin_delete()
    {
        const string tree = "rsdr-4549-unstable";
        const string key = "third-kept";
        const string late = "third-late";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition");
        Assert.That((await ApplyFromSiteCAsync(_siteB, tree, key, written)).Applied, Is.True, "precondition");
        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);

        var lateWrite = PastHlc(4);
        ApplyResult? inDrain = null;
        await RebootstrapSiteBAsync(
            tree,
            SiteCFrontier(PastHlc(3)),
            async () =>
            {
                inDrain = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, late, lateWrite);
                await siteA.DeleteTreeAsync();
                await siteA.RecoverTreeAsync();
            });

        // The sender re-ships the write the drain deferred.
        var reShipped = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, late, lateWrite);
        Assert.Multiple(async () =>
        {
            Assert.That(await siteB.GetAsync(key), Is.EqualTo(ThirdValue), "an unstable export must not infer deletes");
            Assert.That(inDrain?.Deferred, Is.True,
                "below the floor while the import was open: deferred, never acknowledged as dropped");
            Assert.That(reShipped.Applied, Is.True, "the floor of an unstable export is cleared, so the re-shipped write applies");
            Assert.That(await siteB.GetAsync(late), Is.EqualTo(ThirdValue),
                "a third-origin write below the floor during an unstable drain is kept, not lost");
        });
    }

    [Test]
    public async Task A_write_admitted_before_the_floor_and_arriving_after_the_scan_is_refused_then_dropped()
    {
        const string tree = "rsdr-4549-straddler";
        const string key = "third-straddler";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // C's write reached the source, which deleted the key and reaped it. The
        // copy bound for the receiver was admitted there before the floor existed
        // and stalls past the whole re-bootstrap.
        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition");
        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);
        var admittedUnder = (await _siteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(tree)
            .GetAdmissionAsync(SiteCClusterId)).FloorEpoch;

        await RebootstrapSiteBAsync(tree, SiteCFrontier(HybridLogicalClock.Tick(written)));

        Exception? refusal = null;
        RequestContext.Clear();
        ReplicationFloorAdmission.Stamp(admittedUnder);
        try
        {
            await _siteB.Client.GetGrain<IReplicationApplyGrain>(tree)
                .ApplySetAsync(key, ThirdValue, written, SiteCClusterId, null, 0);
        }
        catch (Exception ex)
        {
            refusal = ex;
        }
        finally
        {
            RequestContext.Clear();
        }

        var reShipped = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, key, written);
        Assert.Multiple(async () =>
        {
            Assert.That(refusal, Is.InstanceOf<ReplicationFloorAdmissionStaleException>(),
                "every shard was armed with the floor's epoch before the scan, so the straddler is refused at the shard");
            Assert.That(reShipped.Applied, Is.False);
            Assert.That(reShipped.Deferred, Is.False, "the re-shipped delivery is admitted against the floor and dropped");
            Assert.That(await siteB.GetAsync(key), Is.Null, "the straddler must not resurrect the key after the scan");
        });
    }

    [Test]
    public async Task A_pending_prepare_below_the_floor_whose_terminal_commits_after_the_scan_does_not_resurrect_the_key()
    {
        const string tree = "rsdr-4549-pending-prepare";
        const string key = "third-prepared";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // C's single-key saga is staged on the receiver; the source applied it,
        // committed it, then deleted and reaped the key, so the export lacks it.
        var txid = Guid.NewGuid();
        var prepared = PastHlc(5);
        var staged = await Applier(_siteB).ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = ThirdValue,
            Timestamp = prepared,
            OriginClusterId = SiteCClusterId,
            TransactionId = txid,
            IsPrepared = true,
            AtomicBatchSize = 1,
            AtomicBatchIndex = 0,
        });
        Assert.That(staged.Applied, Is.True, "precondition: the prepare is staged");
        Assert.That(await siteB.GetAsync(key), Is.Null, "precondition: a staged prepare is not visible");

        await RebootstrapSiteBAsync(tree, SiteCFrontier(HybridLogicalClock.Tick(prepared)));

        await Applier(_siteB).ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.TxCommit,
            Key = "0",
            ShardIndex = 0,
            Timestamp = PastHlc(4),
            OriginClusterId = SiteCClusterId,
            TransactionId = txid,
        });

        Assert.That(await siteB.GetAsync(key), Is.Null,
            "the stale saga's bucket was discarded, so its commit installs nothing");
    }

    [Test]
    public async Task A_third_origin_orphan_above_the_watermark_owes_a_retry_that_reconciles_it_once_the_watermark_passes()
    {
        const string tree = "rsdr-4549-owed-above";
        const string key = "third-above-watermark";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // The source applies C's write and deletes the key, but its watermark for
        // C, read when the export opened, is still below the write.
        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition");
        Assert.That((await ApplyFromSiteCAsync(_siteB, tree, key, written)).Applied, Is.True, "precondition");
        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);

        await RebootstrapSiteBAsync(tree, SiteCFrontier(PastHlc(6)));
        Assert.That(await siteB.GetAsync(key), Is.EqualTo(ThirdValue),
            "precondition: above the watermark, this export cannot prove the source deleted it");

        // The watermark has since passed the write; the owed retry settles it.
        _openFrontier = SiteCFrontier(HybridLogicalClock.Tick(written));
        try
        {
            await DriveSiteBAsync(tree, c => c.RetryOwedReconcileAsync(SiteAClusterId));
        }
        finally
        {
            _openFrontier = null;
        }

        Assert.That(await siteB.GetAsync(key), Is.Null,
            "the orphan must not survive for ever: it was owed a retry, and the retry reconciles it");
    }

    [Test]
    public async Task A_source_row_taken_during_the_drain_that_the_export_lacks_blocks_alignment()
    {
        const string tree = "rsdr-4549-realign-mid-drain";
        const string stale = "old-lineage-row";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });

        // The receiver holds no source row when the import begins. Mid-drain it
        // takes a source-origin row the export does not carry, as an entry the
        // source shipped under an older lineage before a restore would be.
        Task<ApplyResult> delivery = null!;
        _onDrainStarted = async () =>
        {
            using (ExecutionContext.SuppressFlow())
            {
                delivery = Task.Run(() => Applier(_siteB).ApplyAsync(new WalRecord
                {
                    TreeId = tree,
                    Op = MutationKind.Set,
                    Key = stale,
                    Value = new byte[] { 7 },
                    Timestamp = PastHlc(5),
                    OriginClusterId = SiteAClusterId,
                }));
            }

            await delivery;
        };
        try
        {
            await BootstrapSiteBAsync(tree);
        }
        finally
        {
            _onDrainStarted = null;
        }

        Assert.That((await delivery).Applied, Is.True, "precondition: the row landed during the drain");

        // A later pass must not vouch for it: the source never deleted it.
        await BootstrapSiteBAsync(tree);

        Assert.That(await siteB.GetAsync(stale), Is.EqualTo(new byte[] { 7 }),
            "aligning over a row the export lacked would let the next pass delete a value the source never deleted");
    }

    [Test]
    public async Task A_frontier_read_under_another_lineage_installs_no_floor_and_reconciles_nothing()
    {
        const string tree = "rsdr-4549-lineage";
        const string key = "third-other-lineage";
        const string inDrainKey = "third-in-drain";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteB, tree, key, written)).Applied, Is.True, "precondition");

        // Both the frontier the export opens with (the drop floor and the
        // reconcile) and the one it closes with (the tree frontier pin) are read
        // under a lineage the export did not open under. Both writes sit below
        // the foreign watermark, so either one installed would act on them.
        var foreignLineage = Guid.NewGuid();
        var foreignWatermark = PastHlc(3);
        ApplyResult? inDrain = null;
        _closeFrontier = _ => ClosingFrontier(foreignWatermark, foreignLineage);
        try
        {
            await RebootstrapSiteBAsync(
                tree,
                SiteCFrontier(foreignWatermark, lineage: foreignLineage),
                async () => inDrain = await DeliverFromSiteCOutsideTheDrainAsync(_siteB, tree, inDrainKey, PastHlc(4)));
        }
        finally
        {
            _closeFrontier = null;
        }

        var pinned = await _siteB.Client.GetGrain<IReplicationTreeFrontierGrain>(tree).GetAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(inDrain?.Applied, Is.True, "no floor is installed from a frontier of another lineage");
            Assert.That(await siteB.GetAsync(key), Is.EqualTo(ThirdValue), "nothing is reconciled against it either");
            Assert.That(pinned.LowWatermarks.ContainsKey(SiteCClusterId), Is.False,
                "and none of its watermarks is pinned on the receiver's tree frontier");
        });
    }
}
