using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Defence-in-depth tests for the <see cref="MutationKind.Tombstone"/>
/// no-op guard in <c>ReplicationApplier.ApplyAsync</c>. Tombstone-reap
/// envelopes are filtered out at the producer boundary by
/// <c>ReplicationShipperGrain.ShouldShip</c> and
/// <c>ChangeFeed.Subscribe</c>, so a healthy receiver never sees one
/// in the steady state. The guard tested here covers the case where
/// an older or hand-built shipper still delivers one - the applier
/// must surface it as an explicit dedup-shaped no-op (Applied=false,
/// HWM unchanged) rather than faulting the apply loop and dead-
/// lettering the entry.
/// </summary>
public partial class ReplicationApplierTests
{
    private static WalRecord TombstoneReapEntry(string key, HybridLogicalClock ts, string origin = RemoteCluster) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Tombstone,
        Key = key,
        Timestamp = ts,
        IsTombstone = true,
        OriginClusterId = origin,
    };

    [Test]
    public async Task ApplyAsync_returns_no_op_for_tombstone_reap_envelope()
    {
        var (applier, _, apply, hwm) = CreateApplier();

        var result = await applier.ApplyAsync(TombstoneReapEntry("k", Hlc(10)));

        Assert.That(result.Applied, Is.False,
            "tombstone-reap envelopes must surface as Applied=false; receiver-side "
            + "compaction is run independently per cluster against its own copy of the data.");
        Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero),
            "the HWM must not advance on a tombstone-reap; otherwise the receiver would "
            + "subsequently dedup legitimate user writes whose HLC falls below the bumped mark.");
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await apply.DidNotReceiveWithAnyArgs().ApplyDeleteAsync(default!, default, default!, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_does_not_throw_on_tombstone_reap_with_empty_key()
    {
        // The body's `MutationKind.Tombstone` guard runs after the TreeId
        // and OriginClusterId required-field checks but before the
        // per-key apply dispatch. A tombstone-reap envelope authored
        // by a path that left the key empty is still a tombstone-reap;
        // the guard must short-circuit without attempting the dispatch.
        var (applier, _, _, _) = CreateApplier();

        var entry = TombstoneReapEntry(string.Empty, Hlc(1));
        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.False);
    }
}

