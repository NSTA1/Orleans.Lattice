using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Unit coverage for <see cref="OrSet.MergeDelta(OrSetDelta)"/>. The merge must
/// be idempotent under duplicate delivery: replaying the same delta twice must
/// union each (element, dot) pair rather than append it a second time, matching
/// the documented contract and the behaviour every sibling CRDT delta-merge
/// (GSet, RwSet, OrMap, ...) already upholds.
/// </summary>
[TestFixture]
public class OrSetMergeDeltaTests
{
    private static readonly byte[] X = new byte[] { 1 };

    // base64 of the single byte 0x01: the internal key OrSet files dots under.
    private const string XKey = "AQ==";

    [Test]
    public void MergeDelta_adds_element_as_member()
    {
        var set = new OrSet();
        var delta = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = X, ReplicaId = "A", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };

        set.MergeDelta(delta);

        Assert.Multiple(() =>
        {
            Assert.That(set.Contains(X), Is.True);
            Assert.That(set.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void MergeDelta_is_idempotent_under_duplicate_delivery()
    {
        var set = new OrSet();
        var delta = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = X, ReplicaId = "A", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };

        set.MergeDelta(delta);
        set.MergeDelta(delta);

        // The same add dot must be unioned, not appended: exactly one dot, not
        // two. Appending a duplicate breaks convergence and grows the dot list
        // without bound on every redelivery.
        Assert.That(set.Adds[XKey], Has.Count.EqualTo(1));
    }

    [Test]
    public void MergeDelta_removes_are_idempotent_under_duplicate_delivery()
    {
        var set = new OrSet();
        var delta = new OrSetDelta
        {
            Adds = Array.Empty<OrSetDeltaDot>(),
            Removes = new[] { new OrSetDeltaDot { Element = X, ReplicaId = "A", Counter = 1 } },
        };

        set.MergeDelta(delta);
        set.MergeDelta(delta);

        Assert.That(set.Tombstones[XKey], Has.Count.EqualTo(1));
    }

    [Test]
    public void MergeDelta_treats_null_collections_as_empty()
    {
        var set = new OrSet();
        set.Add(X, "A", 1);

        set.MergeDelta(default);

        Assert.That(set.Contains(X), Is.True);
    }
}
