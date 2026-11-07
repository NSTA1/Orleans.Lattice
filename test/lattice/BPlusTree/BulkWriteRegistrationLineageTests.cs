using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The bulk-write registration stamp (issue #5813) is bound to the tree it was
/// issued for and scoped to the flow that set it.
/// </summary>
[TestFixture]
public sealed class BulkWriteRegistrationLineageTests
{
    [Test]
    public void A_stamp_answers_only_for_its_own_tree()
    {
        var lineage = Guid.NewGuid();

        using (BulkWriteRegistrationLineage.Stamp("shadow", lineage))
        {
            Assert.That(BulkWriteRegistrationLineage.For("shadow"), Is.EqualTo(lineage));
            Assert.That(BulkWriteRegistrationLineage.For("other"), Is.Null);
        }

        Assert.That(BulkWriteRegistrationLineage.For("shadow"), Is.Null);
    }

    [Test]
    public void Disposing_a_nested_stamp_restores_the_outer_one()
    {
        var outer = Guid.NewGuid();

        using (BulkWriteRegistrationLineage.Stamp("outer", outer))
        {
            using (BulkWriteRegistrationLineage.Stamp("inner", Guid.NewGuid()))
            {
                Assert.That(BulkWriteRegistrationLineage.For("outer"), Is.Null);
            }

            Assert.That(BulkWriteRegistrationLineage.For("outer"), Is.EqualTo(outer));
        }

        Assert.That(RequestContext.Get(BulkWriteRegistrationLineage.TreeRequestContextKey), Is.Null);
        Assert.That(RequestContext.Get(BulkWriteRegistrationLineage.LineageRequestContextKey), Is.Null);
    }

    [Test]
    public void Its_keys_are_the_wire_keys_the_purged_copy_gate_reads()
    {
        Assert.That(BulkWriteRegistrationLineage.TreeRequestContextKey, Is.EqualTo("ol.bulk.regtree"));
        Assert.That(BulkWriteRegistrationLineage.LineageRequestContextKey, Is.EqualTo("ol.bulk.reglin"));
    }
}
