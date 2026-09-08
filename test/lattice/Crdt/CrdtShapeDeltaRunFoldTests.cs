using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Crdt;

// Pins the linear CombineDeltaRun fold to the pairwise CombineDeltas fold it
// replaces. Every shape's combine is commutative, associative and idempotent,
// so a run fold that visits each source exactly once must produce a delta that
// serialises byte-for-byte identically to the running left fold - which is the
// contract the pre-ship coalescer relies on. Runs are deliberately longer than
// two so the quadratic path is actually exercised, and sources overlap so both
// the add-new and skip-duplicate branches of every union helper are hit.
[TestFixture]
public class CrdtShapeDeltaRunFoldTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static OrSetDot Dot(string r, long c) => new() { ReplicaId = r, Counter = c };

    private static OrSetDeltaDot DDot(string e, string r, long c) =>
        new() { Element = B(e), ReplicaId = r, Counter = c };

    // Folds the run pairwise the way the shipper used to, then asserts the
    // shape's own run fold serialises to the same bytes.
    private static void AssertRunFoldMatchesPairwise(CrdtShape shape, params object[] run)
    {
        Assert.That(shape.CombineDeltaRun, Is.Not.Null, "shape exposes no run fold");

        var pairwise = run[0];
        for (var i = 1; i < run.Length; i++)
        {
            pairwise = shape.CombineDeltas!(pairwise, run[i]);
        }

        var linear = shape.CombineDeltaRun!(run);

        Assert.That(
            shape.SerializeDelta!(linear),
            Is.EqualTo(shape.SerializeDelta!(pairwise)));
    }

    [Test]
    public void CombineDeltaRun_orset_matches_pairwise_fold()
    {
        AssertRunFoldMatchesPairwise(
            CrdtShape.ForOrSet(),
            new OrSetDelta { Adds = [DDot("x", "r1", 1)], Removes = [DDot("z", "r2", 1)] },
            new OrSetDelta { Adds = [DDot("x", "r1", 1), DDot("y", "r1", 2)], Removes = [] },
            new OrSetDelta { Adds = [DDot("w", "r3", 4)], Removes = [DDot("z", "r2", 1)] },
            new OrSetDelta { Adds = [], Removes = [DDot("y", "r1", 2)] });
    }

    [Test]
    public void CombineDeltaRun_rwset_matches_pairwise_fold()
    {
        AssertRunFoldMatchesPairwise(
            CrdtShape.ForRwSet(),
            new RwSetDelta { Adds = [DDot("x", "r1", 1)], Removes = [], Tombstones = [DDot("x", "r3", 1)] },
            new RwSetDelta { Adds = [DDot("x", "r1", 1), DDot("y", "r1", 2)], Removes = [DDot("x", "r2", 1)], Tombstones = [] },
            new RwSetDelta { Adds = [DDot("q", "r4", 9)], Removes = [DDot("x", "r2", 1)], Tombstones = [DDot("q", "r3", 2)] });
    }

    [Test]
    public void CombineDeltaRun_orflag_matches_pairwise_fold()
    {
        AssertRunFoldMatchesPairwise(
            CrdtShape.ForOrFlag(),
            new OrFlagDelta { Enables = [Dot("r1", 1)], Disables = [Dot("r2", 1)] },
            new OrFlagDelta { Enables = [Dot("r1", 1), Dot("r1", 2)], Disables = [] },
            new OrFlagDelta { Enables = [Dot("r3", 7)], Disables = [Dot("r2", 1), Dot("r2", 3)] });
    }

    [Test]
    public void CombineDeltaRun_rwflag_matches_pairwise_fold()
    {
        AssertRunFoldMatchesPairwise(
            CrdtShape.ForRwFlag(),
            new RwFlagDelta { Enables = [Dot("r1", 1)], Disables = [Dot("r2", 1)], Tombstones = [Dot("r3", 1)] },
            new RwFlagDelta { Enables = [Dot("r1", 2)], Disables = [Dot("r2", 1)], Tombstones = [] },
            new RwFlagDelta { Enables = [Dot("r1", 1)], Disables = [Dot("r4", 8)], Tombstones = [Dot("r3", 2)] });
    }

    [Test]
    public void CombineDeltaRun_gset_matches_pairwise_fold()
    {
        AssertRunFoldMatchesPairwise(
            CrdtShape.ForGSet(),
            new GSetDelta { Adds = [B("x"), B("y")] },
            new GSetDelta { Adds = [B("y"), B("z")] },
            new GSetDelta { Adds = [B("z"), B("w")] },
            new GSetDelta { Adds = [] });
    }

    [Test]
    public void CombineDeltaRun_rga_matches_pairwise_fold()
    {
        var a = new RgaDeltaNode { ReplicaId = "r1", Counter = 1, ParentDot = Dot("r0", 0), Value = B("a") };
        var b = new RgaDeltaNode { ReplicaId = "r1", Counter = 2, ParentDot = Dot("r1", 1), Value = B("b") };
        var c = new RgaDeltaNode { ReplicaId = "r2", Counter = 1, ParentDot = Dot("r1", 2), Value = B("c") };

        AssertRunFoldMatchesPairwise(
            CrdtShape.ForRga(),
            new RgaDelta { Inserts = [a], Tombstones = [Dot("r2", 5)] },
            new RgaDelta { Inserts = [a, b], Tombstones = [Dot("r2", 5), Dot("r2", 6)] },
            new RgaDelta { Inserts = [c], Tombstones = [] });
    }

    [Test]
    public void CombineDeltaRun_gcounter_matches_pairwise_fold()
    {
        AssertRunFoldMatchesPairwise(
            CrdtShape.ForGCounter(),
            new GCounterDelta { Increments = new Dictionary<string, long> { ["r1"] = 5, ["r2"] = 1 } },
            new GCounterDelta { Increments = new Dictionary<string, long> { ["r1"] = 3, ["r3"] = 9 } },
            new GCounterDelta { Increments = new Dictionary<string, long> { ["r1"] = 8, ["r2"] = 1 } });
    }

    [Test]
    public void CombineDeltaRun_pncounter_matches_pairwise_fold()
    {
        AssertRunFoldMatchesPairwise(
            CrdtShape.ForPnCounter(),
            new PnCounterDelta
            {
                Increments = new Dictionary<string, long> { ["r1"] = 5 },
                Decrements = new Dictionary<string, long> { ["r1"] = 2 },
            },
            new PnCounterDelta
            {
                Increments = new Dictionary<string, long> { ["r1"] = 3, ["r2"] = 7 },
                Decrements = new Dictionary<string, long> { ["r1"] = 4 },
            },
            new PnCounterDelta
            {
                Increments = new Dictionary<string, long> { ["r2"] = 11 },
                Decrements = new Dictionary<string, long> { ["r3"] = 1 },
            });
    }

    [Test]
    public void CombineDeltaRun_version_vector_matches_pairwise_fold()
    {
        var lo = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var mid = new HybridLogicalClock { WallClockTicks = 150, Counter = 2 };
        var hi = new HybridLogicalClock { WallClockTicks = 200, Counter = 3 };

        AssertRunFoldMatchesPairwise(
            CrdtShape.ForVersionVector(),
            new VersionVectorDelta { Entries = new Dictionary<string, HybridLogicalClock> { ["r1"] = lo, ["r2"] = hi } },
            new VersionVectorDelta { Entries = new Dictionary<string, HybridLogicalClock> { ["r1"] = hi, ["r3"] = lo } },
            new VersionVectorDelta { Entries = new Dictionary<string, HybridLogicalClock> { ["r1"] = mid, ["r3"] = mid } });
    }

    [Test]
    public void CombineDeltaRun_mv_register_matches_pairwise_fold()
    {
        AssertRunFoldMatchesPairwise(
            CrdtShape.ForMvRegister(),
            new MvRegisterDelta { Entries = [], Context = new Dictionary<string, long> { ["r1"] = 1 } },
            new MvRegisterDelta { Entries = [], Context = new Dictionary<string, long> { ["r2"] = 2 } },
            new MvRegisterDelta { Entries = [], Context = new Dictionary<string, long> { ["r1"] = 4, ["r3"] = 1 } });
    }

    [Test]
    public void CombineDeltaRun_max_register_matches_pairwise_fold()
    {
        AssertRunFoldMatchesPairwise(
            CrdtShape.ForMaxRegister(),
            new BoundedRegisterDelta { HasValue = true, Value = B("a"), OrderKey = B("1") },
            new BoundedRegisterDelta { HasValue = true, Value = B("c"), OrderKey = B("3") },
            new BoundedRegisterDelta { HasValue = true, Value = B("b"), OrderKey = B("2") });
    }

    [Test]
    public void CombineDeltaRun_min_register_matches_pairwise_fold_with_empty_operands()
    {
        AssertRunFoldMatchesPairwise(
            CrdtShape.ForMinRegister(),
            new BoundedRegisterDelta { HasValue = false },
            new BoundedRegisterDelta { HasValue = true, Value = B("c"), OrderKey = B("3") },
            new BoundedRegisterDelta { HasValue = false },
            new BoundedRegisterDelta { HasValue = true, Value = B("b"), OrderKey = B("2") });
    }

    [Test]
    public void CombineDeltaRun_ormap_matches_pairwise_fold()
    {
        var shape = CrdtShape.ForOrMap<string, GCounter>();
        var one = new GCounter();
        one.Increment("r1", 1);
        var two = new GCounter();
        two.Increment("r1", 2);

        AssertRunFoldMatchesPairwise(
            shape,
            new OrMapDelta<string, GCounter>
            {
                Adds = [new OrMapDeltaEntry<string, GCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = one }],
                Tombstones = [],
            },
            new OrMapDelta<string, GCounter>
            {
                Adds = [new OrMapDeltaEntry<string, GCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = two }],
                Tombstones = [new OrMapDeltaTombstone<string> { Key = "gone", ReplicaId = "r2", Counter = 4 }],
            },
            new OrMapDelta<string, GCounter>
            {
                Adds = [new OrMapDeltaEntry<string, GCounter> { Key = "j", ReplicaId = "r3", Counter = 7, Value = one }],
                Tombstones = [new OrMapDeltaTombstone<string> { Key = "gone", ReplicaId = "r2", Counter = 4 }],
            });
    }

    [Test]
    public void CombineDeltaRun_single_element_run_returns_the_only_delta()
    {
        var shape = CrdtShape.ForOrSet();
        object only = new OrSetDelta { Adds = [DDot("x", "r1", 1)], Removes = [] };

        var combined = (OrSetDelta)shape.CombineDeltaRun!([only]);

        Assert.That(shape.SerializeDelta!(combined), Is.EqualTo(shape.SerializeDelta!(only)));
    }

    [Test]
    public void Every_shape_with_a_pairwise_combine_also_exposes_a_run_fold()
    {
        // The shipper falls back to the pairwise fold for a shape without a run
        // fold, so a gap here is a silent quadratic regression rather than a
        // failure. Enumerate the registry's factories so a newly added shape
        // has to opt in deliberately.
        CrdtShape[] shapes =
        [
            CrdtShape.ForOrSet(),
            CrdtShape.ForRwSet(),
            CrdtShape.ForOrFlag(),
            CrdtShape.ForRwFlag(),
            CrdtShape.ForGSet(),
            CrdtShape.ForRga(),
            CrdtShape.ForGCounter(),
            CrdtShape.ForPnCounter(),
            CrdtShape.ForVersionVector(),
            CrdtShape.ForMvRegister(),
            CrdtShape.ForMaxRegister(),
            CrdtShape.ForMinRegister(),
            CrdtShape.ForOrMap<string, GCounter>(),
        ];

        Assert.Multiple(() =>
        {
            foreach (var shape in shapes)
            {
                Assert.That(shape.CombineDeltas, Is.Not.Null, shape.Mode.ToString());
                Assert.That(shape.CombineDeltaRun, Is.Not.Null, shape.Mode.ToString());
            }
        });
    }
}
