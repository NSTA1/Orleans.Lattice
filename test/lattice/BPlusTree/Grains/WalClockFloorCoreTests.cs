using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class WalClockFloorCoreTests
{
    private const string Local = "site-a";
    private static readonly HybridLogicalClock Floor = new() { WallClockTicks = 1_000, Counter = 0 };

    private static WalRecord Fresh(long wall, int counter = 0) => new()
    {
        TreeId = "t",
        Op = MutationKind.Set,
        Key = "k",
        Timestamp = new HybridLogicalClock { WallClockTicks = wall, Counter = counter },
        OriginClusterId = Local,
    };

    [Test]
    public void A_fresh_local_stamp_below_the_floor_is_refused()
    {
        var record = Fresh(999, 7);

        Assert.Multiple(() =>
        {
            Assert.That(WalClockFloorCore.IsSubjectToFloor(in record, Local), Is.True);
            Assert.That(WalClockFloorCore.IsAdmitted(in record, Floor, Local), Is.False);
        });
    }

    [Test]
    public void A_fresh_local_stamp_at_or_above_the_floor_is_admitted()
    {
        var atFloor = Fresh(1_000);
        var above = Fresh(1_000, 1);

        Assert.Multiple(() =>
        {
            Assert.That(WalClockFloorCore.IsAdmitted(in atFloor, Floor, Local), Is.True);
            Assert.That(WalClockFloorCore.IsAdmitted(in above, Floor, Local), Is.True);
        });
    }

    [Test]
    public void A_zero_floor_admits_everything()
    {
        var record = Fresh(1);

        Assert.That(WalClockFloorCore.IsAdmitted(in record, HybridLogicalClock.Zero, Local), Is.True);
    }

    [Test]
    public void Carried_stamps_are_exempt()
    {
        var old = Fresh(10);
        var carried = new[]
        {
            old with { IsCarriedStamp = true },
            old with { IsMigrated = true },
            old with { IsMerge = true },
            old with { IsBackstop = true },
            old with { OriginClusterId = "site-b" },
            old with { Op = MutationKind.Tombstone },
            old with { Timestamp = HybridLogicalClock.Zero },
        };

        Assert.Multiple(() =>
        {
            foreach (var record in carried)
            {
                Assert.That(WalClockFloorCore.IsSubjectToFloor(in record, Local), Is.False, record.ToString());
                Assert.That(WalClockFloorCore.IsAdmitted(in record, Floor, Local), Is.True, record.ToString());
            }
        });
    }

    [Test]
    public void A_prepare_this_leaf_minted_its_original_stamp_for_is_governed()
    {
        var record = Fresh(10) with { IsPrepared = true, PrepareStampOriginal = true };

        Assert.That(WalClockFloorCore.IsAdmitted(in record, Floor, Local), Is.False,
            "an original prepare stamp the leaf minted itself names a fresh write");
    }

    [Test]
    public void Saga_terminals_and_range_deletes_are_governed()
    {
        var terminal = Fresh(10) with { Op = MutationKind.TxCommit };
        var rangeDelete = Fresh(10) with { Op = MutationKind.DeleteRange, EndExclusiveKey = "z" };

        Assert.Multiple(() =>
        {
            Assert.That(WalClockFloorCore.IsAdmitted(in terminal, Floor, Local), Is.False);
            Assert.That(WalClockFloorCore.IsAdmitted(in rangeDelete, Floor, Local), Is.False);
        });
    }

    [Test]
    public void Without_a_local_cluster_id_every_origin_is_governed()
    {
        var foreign = Fresh(10) with { OriginClusterId = "site-b" };

        Assert.That(WalClockFloorCore.IsSubjectToFloor(in foreign, null), Is.True);
    }

    [Test]
    public void The_target_trails_the_wall_clock_by_the_lag()
    {
        var target = WalClockFloorCore.Target(TimeSpan.FromMinutes(5).Ticks, TimeSpan.FromMinutes(1));

        Assert.That(target, Is.EqualTo(new HybridLogicalClock { WallClockTicks = TimeSpan.FromMinutes(4).Ticks }));
        Assert.That(WalClockFloorCore.Target(10, TimeSpan.FromSeconds(1)), Is.EqualTo(HybridLogicalClock.Zero),
            "the target never goes negative");
    }

    [Test]
    public void The_floor_advances_only_once_it_trails_its_target_by_half_a_lag()
    {
        var lag = TimeSpan.FromSeconds(60);
        var current = new HybridLogicalClock { WallClockTicks = TimeSpan.FromSeconds(100).Ticks };

        Assert.Multiple(() =>
        {
            Assert.That(WalClockFloorCore.ShouldAdvance(HybridLogicalClock.Zero, current, lag), Is.True, "first publication");
            Assert.That(WalClockFloorCore.ShouldAdvance(current, current with { WallClockTicks = current.WallClockTicks + TimeSpan.FromSeconds(29).Ticks }, lag), Is.False);
            Assert.That(WalClockFloorCore.ShouldAdvance(current, current with { WallClockTicks = current.WallClockTicks + TimeSpan.FromSeconds(30).Ticks }, lag), Is.True);
            Assert.That(WalClockFloorCore.ShouldAdvance(current, current with { WallClockTicks = current.WallClockTicks - TimeSpan.FromSeconds(90).Ticks }, lag), Is.False,
                "a target behind the floor never lowers it");
        });
    }
}
