using NUnit.Framework;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests pinning the wire-compatible defaults and initialiser
/// shape of the atomic-batch metadata slots
/// (<see cref="LatticeMutation.AtomicBatchSize"/> /
/// <see cref="LatticeMutation.AtomicBatchIndex"/>) added by R-094.
/// Sibling fixture to <see cref="LatticeTransactionContextTests"/> /
/// <see cref="LatticeMaintenanceContextTests"/> following the same
/// "Default_LatticeMutation_*" precedent for new wire slots.
/// </summary>
[TestFixture]
public sealed class LatticeMutationAtomicBatchTests
{
    [Test]
    public void Default_LatticeMutation_has_zero_AtomicBatchSize_and_AtomicBatchIndex_for_wire_compat()
    {
        // Wire-compat: legacy persisted observer payloads (and any caller
        // that constructs the struct without setting the new slots) must
        // round-trip with both fields defaulting to 0 so observers
        // persisted before R-094 decode identically to a single-key,
        // non-atomic write.
        var mutation = new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k",
        };

        Assert.Multiple(() =>
        {
            Assert.That(mutation.AtomicBatchSize, Is.EqualTo(0));
            Assert.That(mutation.AtomicBatchIndex, Is.EqualTo(0));
        });
    }

    [Test]
    public void AtomicBatch_slots_are_settable_via_object_initialiser()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k",
            AtomicBatchSize = 5,
            AtomicBatchIndex = 2,
        };

        Assert.Multiple(() =>
        {
            Assert.That(mutation.AtomicBatchSize, Is.EqualTo(5));
            Assert.That(mutation.AtomicBatchIndex, Is.EqualTo(2));
        });
    }

    [Test]
    public void AtomicBatch_slots_participate_in_record_struct_equality()
    {
        // Defensive: the record-struct equality contract must include
        // both atomic-batch slots so a future caller comparing
        // LatticeMutation instances does not silently treat two
        // mutations differing only in (Size, Index) as identical.
        var baseline = new LatticeMutation { TreeId = "t", Kind = MutationKind.Set, Key = "k", AtomicBatchSize = 5, AtomicBatchIndex = 2 };
        var differentIndex = new LatticeMutation { TreeId = "t", Kind = MutationKind.Set, Key = "k", AtomicBatchSize = 5, AtomicBatchIndex = 3 };
        var differentSize = new LatticeMutation { TreeId = "t", Kind = MutationKind.Set, Key = "k", AtomicBatchSize = 4, AtomicBatchIndex = 2 };
        var identical = new LatticeMutation { TreeId = "t", Kind = MutationKind.Set, Key = "k", AtomicBatchSize = 5, AtomicBatchIndex = 2 };

        Assert.Multiple(() =>
        {
            Assert.That(baseline, Is.Not.EqualTo(differentIndex));
            Assert.That(baseline, Is.Not.EqualTo(differentSize));
            Assert.That(baseline, Is.EqualTo(identical));
        });
    }
}
