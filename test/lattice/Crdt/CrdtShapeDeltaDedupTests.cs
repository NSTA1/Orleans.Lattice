using System.Text;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Regression for the pre-ship delta-coalescing dedup key in
/// <c>CrdtShapeRegistry.AppendOrSetDeltaDots</c>.
/// <para>
/// The dedup key mapped a <see langword="null"/> element to
/// <see cref="string.Empty"/> - but <c>Convert.ToBase64String([])</c> <em>is</em>
/// <see cref="string.Empty"/>, so a null-element dot and a legitimate
/// empty-element dot sharing a <c>(replicaId, counter)</c> collided on one key
/// and the second was dropped. When the null dot won, the empty-element add
/// vanished from the coalesced delta entirely (<c>OrSet.MergeDelta</c> skips a
/// null element), so a write was silently lost. The sibling
/// <c>AppendGSetElements</c> already does the right thing: it skips a null
/// element rather than mapping it onto a legal key.
/// </para>
/// </summary>
[TestFixture]
public class CrdtShapeDeltaDedupTests
{
    private static OrSetDeltaDot Dot(byte[]? element, string replicaId, long counter) =>
        new() { Element = element!, ReplicaId = replicaId, Counter = counter };

    [Test]
    public void CombineDeltas_orset_does_not_drop_an_empty_element_add_behind_a_null_element_dot()
    {
        var shape = CrdtShape.ForOrSet();
        // Same (replicaId, counter) on both dots; the first carries no element
        // at all, the second carries a legitimately empty one.
        object a = new OrSetDelta { Adds = [Dot(null, "r1", 1)], Removes = [] };
        object b = new OrSetDelta { Adds = [Dot([], "r1", 1)], Removes = [] };

        var combined = (OrSetDelta)shape.CombineDeltas!(a, b);

        Assert.That(combined.Adds.Any(d => d.Element is not null && d.Element.Length == 0), Is.True,
            "the empty-element add must survive coalescing with a null-element dot on the same (replicaId, counter)");
    }

    [Test]
    public void CombineDeltas_orset_drops_a_null_element_dot_rather_than_keying_it()
    {
        var shape = CrdtShape.ForOrSet();
        object a = new OrSetDelta { Adds = [Dot(null, "r1", 1)], Removes = [] };
        object b = new OrSetDelta { Adds = [], Removes = [] };

        var combined = (OrSetDelta)shape.CombineDeltas!(a, b);

        Assert.That(combined.Adds.Any(d => d.Element is null), Is.False,
            "a dot with no element is unmergeable by OrSet.MergeDelta and must be skipped, not carried under a legal key");
    }

    [Test]
    public void CombineDeltas_orset_still_dedups_two_identical_empty_element_dots()
    {
        var shape = CrdtShape.ForOrSet();
        object a = new OrSetDelta { Adds = [Dot([], "r1", 1)], Removes = [] };
        object b = new OrSetDelta { Adds = [Dot([], "r1", 1)], Removes = [] };

        var combined = (OrSetDelta)shape.CombineDeltas!(a, b);

        Assert.That(combined.Adds, Has.Count.EqualTo(1),
            "genuine duplicates must still collapse");
    }

    [Test]
    public void CombineDeltas_rwset_does_not_drop_an_empty_element_add_behind_a_null_element_dot()
    {
        var shape = CrdtShape.ForRwSet();
        object a = new RwSetDelta { Adds = [Dot(null, "r1", 1)], Removes = [], Tombstones = [] };
        object b = new RwSetDelta { Adds = [Dot([], "r1", 1)], Removes = [], Tombstones = [] };

        var combined = (RwSetDelta)shape.CombineDeltas!(a, b);

        Assert.That(combined.Adds.Any(d => d.Element is not null && d.Element.Length == 0), Is.True,
            "the RW-Set coalescer shares the same dedup helper and must behave identically");
    }

    [Test]
    public void CombineDeltas_orset_keeps_distinct_elements_under_one_dot()
    {
        var shape = CrdtShape.ForOrSet();
        object a = new OrSetDelta { Adds = [Dot(Encoding.UTF8.GetBytes("x"), "r1", 1)], Removes = [] };
        object b = new OrSetDelta { Adds = [Dot(Encoding.UTF8.GetBytes("y"), "r1", 1)], Removes = [] };

        var combined = (OrSetDelta)shape.CombineDeltas!(a, b);

        Assert.That(combined.Adds, Has.Count.EqualTo(2));
    }
}
