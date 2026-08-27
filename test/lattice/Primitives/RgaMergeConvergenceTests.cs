namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Convergence regressions for the two RGA merge entry points. A tombstone
/// delivered before its matching insert (out-of-order or partial delivery)
/// records a tombstoned placeholder whose <see cref="RgaNode.ParentDot"/> is a
/// stand-in <see cref="Rga.Root"/> (see the <see cref="Rga.MergeDelta(RgaDelta)"/>
/// tombstone branch). When the authoritative insert for that dot later reaches
/// the replica through a full-state <see cref="Rga.MergeFrom(Rga)"/> (anti-entropy
/// / catch-up), the real parent must win, or the placeholder's live children stay
/// mis-rooted under <see cref="Rga.Root"/> and the merge stops being commutative.
/// <see cref="Rga.MergeDelta(RgaDelta)"/> already reattaches the parent on its
/// insert path; <see cref="Rga.MergeFrom(Rga)"/> must do the equivalent.
/// </summary>
[TestFixture]
public class RgaMergeConvergenceTests
{
    private static byte[] Bytes(int k) => [(byte)k];

    private static OrSetDot Dot(string replicaId, long counter) =>
        new() { ReplicaId = replicaId, Counter = counter };

    private static int[] Values(Rga rga) =>
        rga.ToList().Select(static e => (int)e.Value[0]).ToArray();

    // A = (r1,1) top-level; D = (r2,5) child of A, later removed; C = (r3,1)
    // child of D. D's counter (5) is deliberately greater than A's (1) so a
    // placeholder D mis-rooted under Root would sort before A among Root's
    // children and reorder C relative to A - a visible divergence.
    private static readonly OrSetDot ADot = Dot("r1", 1);
    private static readonly OrSetDot DDot = Dot("r2", 5);

    /// <summary>
    /// Authoritative replica: receives A, D (under A) and C (under D) in causal
    /// order, then D's removal. D ends tombstoned with its real parent A.
    /// </summary>
    private static Rga AuthoritativePeer()
    {
        var peer = new Rga();
        peer.MergeDelta(new RgaDelta
        {
            Inserts =
            [
                new RgaDeltaNode { ReplicaId = "r1", Counter = 1, ParentDot = Rga.Root, Value = Bytes(1) }, // A
                new RgaDeltaNode { ReplicaId = "r2", Counter = 5, ParentDot = ADot, Value = Bytes(2) },     // D under A
                new RgaDeltaNode { ReplicaId = "r3", Counter = 1, ParentDot = DDot, Value = Bytes(3) },     // C under D
            ],
            Tombstones = [DDot],
        });
        return peer;
    }

    /// <summary>
    /// Lagging replica: sees D's tombstone before D's insert (placeholder under
    /// Root), then C under D. It never receives D's insert as a delta - it will
    /// learn D's real parent only through a full-state merge with the peer.
    /// </summary>
    private static Rga LaggingReplicaWithPlaceholder()
    {
        var local = new Rga();
        local.MergeDelta(new RgaDelta
        {
            Inserts = [new RgaDeltaNode { ReplicaId = "r1", Counter = 1, ParentDot = Rga.Root, Value = Bytes(1) }], // A
            Tombstones = [DDot], // D tombstone before its insert -> placeholder under Root
        });
        local.MergeDelta(new RgaDelta
        {
            Inserts = [new RgaDeltaNode { ReplicaId = "r3", Counter = 1, ParentDot = DDot, Value = Bytes(3) }], // C under placeholder D
            Tombstones = [],
        });
        return local;
    }

    [Test]
    public void MergeFrom_placeholder_parent_reattach_keeps_merge_commutative()
    {
        // Regression for MergeFrom not reattaching a placeholder's ParentDot:
        // without the fix Merge(local, peer) mis-roots C under Root -> [3, 1]
        // while Merge(peer, local) keeps the real parent -> [1, 3], so the two
        // merge orders diverge and the CRDT stops converging.
        var local = LaggingReplicaWithPlaceholder();
        var peer = AuthoritativePeer();

        var localThenPeer = Rga.Merge(local, peer);
        var peerThenLocal = Rga.Merge(peer, local);

        Assert.That(Values(localThenPeer), Is.EqualTo(Values(peerThenLocal)),
            "RGA merge must be commutative: a placeholder's parent must reattach identically in either merge order");
    }

    [Test]
    public void MergeFrom_placeholder_parent_reattach_converges_to_authoritative_order()
    {
        // Both merge orders must converge to the authoritative sequence: D is
        // tombstoned (not emitted), so the live order is A[1] then C[3].
        var local = LaggingReplicaWithPlaceholder();
        var peer = AuthoritativePeer();

        var localThenPeer = Rga.Merge(local, peer);
        var peerThenLocal = Rga.Merge(peer, local);

        Assert.Multiple(() =>
        {
            Assert.That(Values(peer), Is.EqualTo(new[] { 1, 3 }), "authoritative peer order");
            Assert.That(Values(localThenPeer), Is.EqualTo(new[] { 1, 3 }), "local <- peer must adopt D's real parent");
            Assert.That(Values(peerThenLocal), Is.EqualTo(new[] { 1, 3 }), "peer <- local must keep D's real parent");
        });
    }
}
