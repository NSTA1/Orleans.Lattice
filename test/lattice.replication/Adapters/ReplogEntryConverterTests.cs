using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Adapters;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// the dormant seam — translation invariants between <see cref="LatticeMutation"/>
/// and <see cref="ReplogEntry"/>. Mirrors the field-by-field semantics
/// established by the existing <c>ReplicationMutationObserver</c>.
/// </summary>
[TestFixture]
public class ReplogEntryConverterTests
{
    [Test]
    public void ToReplogEntry_translates_Set_mutation_with_all_fields()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            EndExclusiveKey = null,
            Value = new byte[] { 1, 2, 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            IsTombstone = false,
            ExpiresAtTicks = 12345L,
            OriginClusterId = null,
            VectorClock = null,
            TransactionId = Guid.NewGuid(),
            Category = MutationCategory.User,
            DeltaKind = "lww",
            DeltaPayload = new byte[] { 9, 9 },
        };

        var entry = ReplogEntryConverter.ToReplogEntry(mutation, ReplicationMode.LwwRegister, "cluster-A");

        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.EqualTo("tree-A"));
            Assert.That(entry.Op, Is.EqualTo(ReplogOp.Set));
            Assert.That(entry.Key, Is.EqualTo("k"));
            Assert.That(entry.EndExclusiveKey, Is.Null);
            Assert.That(entry.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(entry.Timestamp, Is.EqualTo(mutation.Timestamp));
            Assert.That(entry.IsTombstone, Is.False);
            Assert.That(entry.ExpiresAtTicks, Is.EqualTo(12345L));
            Assert.That(entry.OriginClusterId, Is.EqualTo("cluster-A"));
            Assert.That(entry.Mode, Is.EqualTo(ReplicationMode.LwwRegister));
            Assert.That(entry.DeltaKind, Is.EqualTo("lww"));
            Assert.That(entry.DeltaPayload, Is.EqualTo(new byte[] { 9, 9 }));
        });
    }

    [Test]
    public void ToReplogEntry_preserves_existing_origin_over_supplied_default()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = Array.Empty<byte>(),
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "remote-origin",
        };

        var entry = ReplogEntryConverter.ToReplogEntry(mutation, ReplicationMode.LwwRegister, "local-cluster");

        Assert.That(entry.OriginClusterId, Is.EqualTo("remote-origin"));
    }

    [Test]
    public void ToReplogEntry_translates_DeleteRange()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Zero,
        };

        var entry = ReplogEntryConverter.ToReplogEntry(mutation, ReplicationMode.LwwRegister, "cluster-A");

        Assert.Multiple(() =>
        {
            Assert.That(entry.Op, Is.EqualTo(ReplogOp.DeleteRange));
            Assert.That(entry.Key, Is.EqualTo("a"));
            Assert.That(entry.EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(entry.IsTombstone, Is.True);
        });
    }

    [Test]
    public void ToReplogEntry_clones_VectorClock_defensively()
    {
        var vc = new VersionVector();
        vc.Tick("origin-A");

        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = Array.Empty<byte>(),
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            VectorClock = vc,
        };

        var entry = ReplogEntryConverter.ToReplogEntry(mutation, ReplicationMode.LwwRegister, "cluster-A");

        Assert.That(entry.VectorClock, Is.Not.Null);
        Assert.That(entry.VectorClock, Is.Not.SameAs(vc), "must be a defensive clone");
        Assert.That(entry.DependencySummary, Is.SameAs(entry.VectorClock), "summary aliases the cloned frontier");
    }

    [Test]
    public void FromReplogEntry_reverses_a_Set_translation()
    {
        var original = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 7 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            ExpiresAtTicks = 999L,
            OriginClusterId = "origin-A",
            DeltaKind = "lww",
            DeltaPayload = new byte[] { 1 },
        };

        var entry = ReplogEntryConverter.ToReplogEntry(original, ReplicationMode.LwwRegister, "cluster-A");
        var roundTripped = ReplogEntryConverter.FromReplogEntry(entry);

        Assert.Multiple(() =>
        {
            Assert.That(roundTripped.TreeId, Is.EqualTo(original.TreeId));
            Assert.That(roundTripped.Kind, Is.EqualTo(original.Kind));
            Assert.That(roundTripped.Key, Is.EqualTo(original.Key));
            Assert.That(roundTripped.Value, Is.EqualTo(original.Value));
            Assert.That(roundTripped.Timestamp, Is.EqualTo(original.Timestamp));
            Assert.That(roundTripped.ExpiresAtTicks, Is.EqualTo(original.ExpiresAtTicks));
            Assert.That(roundTripped.OriginClusterId, Is.EqualTo(original.OriginClusterId));
            Assert.That(roundTripped.DeltaKind, Is.EqualTo(original.DeltaKind));
            Assert.That(roundTripped.DeltaPayload, Is.EqualTo(original.DeltaPayload));
            // Wire-compat defaults — replication wire does not carry these today.
            Assert.That(roundTripped.TransactionId, Is.EqualTo(Guid.Empty));
            Assert.That(roundTripped.Category, Is.EqualTo(MutationCategory.User));
        });
    }

    [Test]
    public void FromReplogEntry_reverses_a_Delete_translation()
    {
        var original = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Delete,
            Key = "k",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        };

        var entry = ReplogEntryConverter.ToReplogEntry(original, ReplicationMode.LwwRegister, "cluster-A");
        var roundTripped = ReplogEntryConverter.FromReplogEntry(entry);

        Assert.Multiple(() =>
        {
            Assert.That(roundTripped.Kind, Is.EqualTo(MutationKind.Delete));
            Assert.That(roundTripped.IsTombstone, Is.True);
            Assert.That(roundTripped.Key, Is.EqualTo("k"));
        });
    }
}

