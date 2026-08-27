namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Convergence regression for <see cref="Rga.MergeDelta(RgaDelta)"/>. Its
/// full-state sibling <see cref="Rga.MergeFrom(Rga)"/> resolves a dot already
/// present locally by a deterministic max - the structural parent by
/// <c>CompareDot</c> and the value by lexicographic byte order.
/// <see cref="Rga.MergeDelta(RgaDelta)"/> used to overwrite an existing node's
/// parent and value unconditionally (last-arrival-wins), so two replicas that
/// applied the same colliding-dot inserts as deltas in different orders diverged
/// from each other and from a full-state-fed replica, breaking the documented
/// commutativity. MergeDelta now applies the same max rules.
/// </summary>
[TestFixture]
public class RgaMergeDeltaConvergenceTests
{
    private static byte[] Bytes(int k) => [(byte)k];

    private static string Observe(Rga rga) =>
        string.Join(",", rga.ToList().Select(static e => Convert.ToHexString(e.Value)));

    // A delta carrying a single insert (r1, 1) directly under Root, stamping the
    // given value. Two such deltas with different values collide on one dot.
    private static RgaDelta InsertDelta(byte[] value) => new()
    {
        Inserts = [new RgaDeltaNode { ReplicaId = "r1", Counter = 1, ParentDot = Rga.Root, Value = value }],
        Tombstones = [],
    };

    [Test]
    public void MergeDelta_is_order_independent_and_agrees_with_full_state_merge()
    {
        var low = Bytes(1);
        var high = Bytes(2);

        var lowThenHigh = new Rga();
        lowThenHigh.MergeDelta(InsertDelta(low));
        lowThenHigh.MergeDelta(InsertDelta(high));

        var highThenLow = new Rga();
        highThenLow.MergeDelta(InsertDelta(high));
        highThenLow.MergeDelta(InsertDelta(low));

        // Reference: two replicas mint the same dot (r1, 1) with divergent
        // values through the pure-local insert path, folded by the full-state
        // merge (MergeFrom) that MergeDelta must agree with.
        var a = new Rga();
        a.InsertAfter(Rga.Root, "r1", low);
        var b = new Rga();
        b.InsertAfter(Rga.Root, "r1", high);
        var fullState = Observe(Rga.Merge(a, b));

        Assert.Multiple(() =>
        {
            Assert.That(Observe(lowThenHigh), Is.EqualTo(Observe(highThenLow)),
                "MergeDelta must converge regardless of delta arrival order");
            Assert.That(Observe(lowThenHigh), Is.EqualTo(fullState),
                "a delta-fed replica must converge to the same node a full-state merge produces");
            Assert.That(Observe(lowThenHigh), Is.EqualTo(Convert.ToHexString(high)),
                "the deterministic winner is the greater value bytes");
        });
    }
}
