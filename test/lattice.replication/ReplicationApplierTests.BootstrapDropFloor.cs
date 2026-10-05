using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The bootstrap drop floor at the applier (issue #4549), over a REAL
/// <see cref="ReplicationHighWaterMarkGrain"/>. A full bootstrap's export
/// reflects every write of an origin stamped below the source's applied low
/// watermark at export open and not held there; a late delivery of one must be
/// acknowledged without being merged, or a write still in flight from a third
/// cluster resurrects a key the source deleted and reaped (the model's
/// third-writer trace). A held write, a write at or above the watermark, and a
/// row the bootstrap drain itself applies must still apply.
/// </summary>
public partial class ReplicationApplierTests
{
    /// <summary>Installs a floor and closes its import stable, so it drops.</summary>
    private static async Task InstallFloorAsync(IReplicationHighWaterMarkGrain hwm, HybridLogicalClock lowWatermark, params HybridLogicalClock[] held)
    {
        await InstallProvisionalFloorAsync(hwm, lowWatermark, held);
        await hwm.FinalizeBootstrapFloorAsync();
    }

    /// <summary>Installs a floor whose import has not closed yet, so it defers.</summary>
    private static Task<long> InstallProvisionalFloorAsync(IReplicationHighWaterMarkGrain hwm, HybridLogicalClock lowWatermark, params HybridLogicalClock[] held) =>
        hwm.SetBootstrapFloorAsync(
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [ThirdOrigin] = lowWatermark },
            new Dictionary<string, HybridLogicalClock[]>(StringComparer.Ordinal) { [ThirdOrigin] = held });

    [Test]
    public async Task ApplyAsync_defers_a_write_below_a_provisional_floor()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await InstallProvisionalFloorAsync(hwm, Hlc(100));

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(3), origin: ThirdOrigin));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.Deferred, Is.True,
                "not acknowledged: an unstable import clears the floor, and the re-shipped write must then apply");
        });
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_defers_the_run_when_an_entry_is_below_a_provisional_floor()
    {
        var (applier, _, hwm) = CreateApplierOverRealHwmGrain();
        await InstallProvisionalFloorAsync(hwm, Hlc(100));

        var result = await applier.ApplyBatchAsync(new[]
        {
            SetEntry("below", Hlc(3), origin: ThirdOrigin),
            SetEntry("above", Hlc(150), origin: ThirdOrigin),
        });

        Assert.That(result.Deferred, Is.True, "the run is re-shipped; its applied entries are re-deliveries");
    }

    [Test]
    public async Task ApplyAsync_stamps_the_write_with_the_floor_epoch_it_was_admitted_under()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        var epoch = await InstallProvisionalFloorAsync(hwm, Hlc(100));
        long? stamped = null;
        apply.When(a => a.ApplySetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<string>(), Arg.Any<VersionVector?>(), Arg.Any<long>()))
            .Do(_ => stamped = ReplicationFloorAdmission.TryGet(out var carried) ? carried : null);

        await applier.ApplyAsync(SetEntry("k", Hlc(150), origin: ThirdOrigin));

        Assert.That(stamped, Is.EqualTo(epoch), "the shard compares this against the epoch it was armed with");
    }

    [Test]
    public async Task ApplyAsync_defers_a_write_a_shard_refuses_as_admitted_before_a_later_floor()
    {
        var (applier, apply, _) = CreateApplierOverRealHwmGrain();
        apply.ApplySetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<string>(), Arg.Any<VersionVector?>(), Arg.Any<long>())
            .Returns(Task.FromException(new ReplicationFloorAdmissionStaleException("t", 0, 1)));

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(3), origin: ThirdOrigin));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.Deferred, Is.True, "re-shipped, and the re-delivery is admitted against the floor");
        });
    }

    [Test]
    public async Task ApplyAsync_drops_a_write_below_the_bootstrap_floor_so_it_cannot_resurrect_a_deleted_key()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await InstallFloorAsync(hwm, Hlc(100));

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(3), origin: ThirdOrigin));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.Deferred, Is.False, "acknowledged, so the sender moves past it");
        });
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_applies_a_held_write_and_a_write_at_or_above_the_floor()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await InstallFloorAsync(hwm, Hlc(100), Hlc(3));

        var held = await applier.ApplyAsync(SetEntry("held", Hlc(3), origin: ThirdOrigin));
        var atFloor = await applier.ApplyAsync(SetEntry("at", Hlc(100), origin: ThirdOrigin));
        var otherOrigin = await applier.ApplyAsync(SetEntry("other", Hlc(3)));

        Assert.Multiple(() =>
        {
            Assert.That(held.Applied, Is.True, "the source held it unapplied, so the export lacks it");
            Assert.That(atFloor.Applied, Is.True, "the watermark is strict");
            Assert.That(otherOrigin.Applied, Is.True, "another origin has no floor");
        });
    }

    [Test]
    public async Task ApplyAsync_applies_a_bootstrap_drain_row_below_the_floor()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await InstallFloorAsync(hwm, Hlc(100));

        ApplyResult result;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            result = await applier.ApplyAsync(SetEntry("k", Hlc(3), origin: ThirdOrigin));
        }

        Assert.That(result.Applied, Is.True, "the drain's own rows are the export the floor describes");
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(3), ThirdOrigin, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_drops_a_saga_prepare_below_the_floor()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await InstallFloorAsync(hwm, Hlc(100));
        var prepare = SetEntry("k", Hlc(3), origin: ThirdOrigin) with
        {
            IsPrepared = true,
            TransactionId = Guid.NewGuid(),
            AtomicBatchSize = 1,
        };

        var result = await applier.ApplyAsync(prepare);

        Assert.That(result.Applied, Is.False, "a re-delivered prepare and its terminal would otherwise re-commit a deleted key");
        Assert.That(apply.ReceivedCalls().Any(c => c.GetMethodInfo().Name.StartsWith("ApplyPrepared", StringComparison.Ordinal)), Is.False);
    }

    [Test]
    public async Task ApplyBatchAsync_drops_only_the_entries_below_the_floor()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await InstallFloorAsync(hwm, Hlc(100), Hlc(5));

        var result = await applier.ApplyBatchAsync(new[]
        {
            SetEntry("dropped", Hlc(3), origin: ThirdOrigin),
            SetEntry("held", Hlc(5), origin: ThirdOrigin),
            SetEntry("above", Hlc(150), origin: ThirdOrigin),
        });

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyMergeManyAsync(Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
            items.Count == 2 && items.All(i => i.Key != "dropped")));
    }

    [Test]
    public async Task A_lineage_reset_lifts_the_floor_at_the_applier()
    {
        var (applier, apply, hwm) = CreateApplierOverRealHwmGrain();
        await InstallFloorAsync(hwm, Hlc(100));

        await ((IReplicationHighWaterMarkGrain)hwm).ResetAppliedIdentitiesAsync();
        var result = await applier.ApplyAsync(SetEntry("k", Hlc(3), origin: ThirdOrigin));

        Assert.That(result.Applied, Is.True, "a floor never outlives the contents it vouched for");
    }
}
