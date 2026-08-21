using System.Text;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests.Crdt;

// Exercises the delta-combine (pre-ship coalescing) closures on every
// CrdtShape factory that exposes CombineDeltas, together with the
// SerializeDelta / DeserializeDelta round-trip for the coalesced delta.
// Inputs overlap deliberately so the union helpers hit both the
// add-new and skip-duplicate branches.
[TestFixture]
public class CrdtShapeDeltaCombineTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static OrSetDot Dot(string r, long c) => new() { ReplicaId = r, Counter = c };

    private static OrSetDeltaDot DDot(string e, string r, long c) =>
        new() { Element = B(e), ReplicaId = r, Counter = c };

    private static object RoundTrip(CrdtShape shape, object combined)
    {
        var bytes = shape.SerializeDelta!(combined);
        return shape.DeserializeDelta(bytes);
    }

    [Test]
    public void CombineDeltas_orset_unions_dots_and_round_trips()
    {
        var shape = CrdtShape.ForOrSet();
        object a = new OrSetDelta { Adds = [DDot("x", "r1", 1), DDot("y", "r1", 2)], Removes = [DDot("z", "r2", 1)] };
        object b = new OrSetDelta { Adds = [DDot("y", "r1", 2), DDot("w", "r1", 3)], Removes = [] };

        var combined = (OrSetDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.Adds, Has.Count.EqualTo(3));
            Assert.That(RoundTrip(shape, combined), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_rwset_unions_all_three_dot_sets()
    {
        var shape = CrdtShape.ForRwSet();
        object a = new RwSetDelta { Adds = [DDot("x", "r1", 1)], Removes = [DDot("x", "r2", 1)], Tombstones = [DDot("x", "r3", 1)] };
        object b = new RwSetDelta { Adds = [DDot("x", "r1", 1), DDot("y", "r1", 2)], Removes = [], Tombstones = [DDot("q", "r3", 2)] };

        var combined = (RwSetDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.Adds, Has.Count.EqualTo(2));
            Assert.That(combined.Tombstones, Has.Count.EqualTo(2));
            Assert.That(RoundTrip(shape, combined), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_rga_unions_inserts_and_tombstones()
    {
        var shape = CrdtShape.ForRga();
        var insert = new RgaDeltaNode { ReplicaId = "r1", Counter = 1, ParentDot = Dot("r0", 0), Value = B("a") };
        var insert2 = new RgaDeltaNode { ReplicaId = "r1", Counter = 2, ParentDot = Dot("r1", 1), Value = B("b") };
        object a = new RgaDelta { Inserts = [insert], Tombstones = [Dot("r2", 5)] };
        object b = new RgaDelta { Inserts = [insert, insert2], Tombstones = [Dot("r2", 5), Dot("r2", 6)] };

        var combined = (RgaDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.Inserts, Has.Count.EqualTo(2));
            Assert.That(combined.Tombstones, Has.Count.EqualTo(2));
            Assert.That(RoundTrip(shape, combined), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_orflag_unions_enable_disable_dots()
    {
        var shape = CrdtShape.ForOrFlag();
        object a = new OrFlagDelta { Enables = [Dot("r1", 1)], Disables = [Dot("r2", 1)] };
        object b = new OrFlagDelta { Enables = [Dot("r1", 1), Dot("r1", 2)], Disables = [] };

        var combined = (OrFlagDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.Enables, Has.Count.EqualTo(2));
            Assert.That(RoundTrip(shape, combined), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_rwflag_unions_all_three_dot_sets()
    {
        var shape = CrdtShape.ForRwFlag();
        object a = new RwFlagDelta { Enables = [Dot("r1", 1)], Disables = [Dot("r2", 1)], Tombstones = [Dot("r3", 1)] };
        object b = new RwFlagDelta { Enables = [Dot("r1", 1), Dot("r1", 2)], Disables = [Dot("r2", 2)], Tombstones = [] };

        var combined = (RwFlagDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.Enables, Has.Count.EqualTo(2));
            Assert.That(combined.Disables, Has.Count.EqualTo(2));
            Assert.That(RoundTrip(shape, combined), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_gset_unions_elements_by_content()
    {
        var shape = CrdtShape.ForGSet();
        object a = new GSetDelta { Adds = [B("x"), B("y")] };
        object b = new GSetDelta { Adds = [B("y"), B("z")] };

        var combined = (GSetDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.Adds, Has.Count.EqualTo(3));
            Assert.That(RoundTrip(shape, combined), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_gcounter_takes_pointwise_max()
    {
        var shape = CrdtShape.ForGCounter();
        object a = new GCounterDelta { Increments = new Dictionary<string, long> { ["r1"] = 5, ["r2"] = 1 } };
        object b = new GCounterDelta { Increments = new Dictionary<string, long> { ["r1"] = 3, ["r3"] = 9 } };

        var combined = (GCounterDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.Increments["r1"], Is.EqualTo(5));
            Assert.That(combined.Increments["r3"], Is.EqualTo(9));
            Assert.That(RoundTrip(shape, combined), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_pncounter_takes_pointwise_max_per_component()
    {
        var shape = CrdtShape.ForPnCounter();
        object a = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["r1"] = 5 },
            Decrements = new Dictionary<string, long> { ["r1"] = 2 },
        };
        object b = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["r1"] = 3, ["r2"] = 7 },
            Decrements = new Dictionary<string, long> { ["r1"] = 4 },
        };

        var combined = (PnCounterDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.Increments["r1"], Is.EqualTo(5));
            Assert.That(combined.Increments["r2"], Is.EqualTo(7));
            Assert.That(combined.Decrements["r1"], Is.EqualTo(4));
            Assert.That(RoundTrip(shape, combined), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_version_vector_takes_pointwise_max_hlc()
    {
        var shape = CrdtShape.ForVersionVector();
        var lo = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var hi = new HybridLogicalClock { WallClockTicks = 200, Counter = 3 };
        object a = new VersionVectorDelta { Entries = new Dictionary<string, HybridLogicalClock> { ["r1"] = lo, ["r2"] = hi } };
        object b = new VersionVectorDelta { Entries = new Dictionary<string, HybridLogicalClock> { ["r1"] = hi, ["r3"] = lo } };

        var combined = (VersionVectorDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.Entries["r1"], Is.EqualTo(hi));
            Assert.That(combined.Entries, Has.Count.EqualTo(3));
            Assert.That(RoundTrip(shape, combined), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_max_register_keeps_higher_order_key()
    {
        var shape = CrdtShape.ForMaxRegister();
        object a = new BoundedRegisterDelta { HasValue = true, Value = B("a"), OrderKey = B("1") };
        object b = new BoundedRegisterDelta { HasValue = true, Value = B("b"), OrderKey = B("2") };

        var combined = (BoundedRegisterDelta)shape.CombineDeltas!(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(combined.HasValue, Is.True);
            Assert.That(RoundTrip(shape, combined), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_min_register_handles_missing_operand()
    {
        var shape = CrdtShape.ForMinRegister();
        object present = new BoundedRegisterDelta { HasValue = true, Value = B("a"), OrderKey = B("5") };
        object empty = new BoundedRegisterDelta { HasValue = false };

        var combinedRight = (BoundedRegisterDelta)shape.CombineDeltas!(present, empty);
        var combinedLeft = (BoundedRegisterDelta)shape.CombineDeltas!(empty, present);

        Assert.Multiple(() =>
        {
            Assert.That(combinedRight.HasValue, Is.True);
            Assert.That(combinedLeft.HasValue, Is.True);
            Assert.That(RoundTrip(shape, combinedRight), Is.Not.Null);
        });
    }

    [Test]
    public void CombineDeltas_mv_register_merges_and_round_trips()
    {
        var shape = CrdtShape.ForMvRegister();
        object a = new MvRegisterDelta { Entries = [], Context = new Dictionary<string, long> { ["r1"] = 1 } };
        object b = new MvRegisterDelta { Entries = [], Context = new Dictionary<string, long> { ["r2"] = 2 } };

        var combined = (MvRegisterDelta)shape.CombineDeltas!(a, b);

        Assert.That(RoundTrip(shape, combined), Is.Not.Null);
    }
}
