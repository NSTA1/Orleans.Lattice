using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplogEntryTests
{
    [Test]
    public void Default_instance_has_empty_or_zero_fields()
    {
        var entry = default(ReplogEntry);
        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.Null);
            Assert.That(entry.Op, Is.EqualTo(ReplogOp.Set));
            Assert.That(entry.Key, Is.Null);
            Assert.That(entry.EndExclusiveKey, Is.Null);
            Assert.That(entry.Value, Is.Null);
            Assert.That(entry.Timestamp, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(entry.IsTombstone, Is.False);
            Assert.That(entry.ExpiresAtTicks, Is.EqualTo(0L));
            Assert.That(entry.OriginClusterId, Is.Null);
            Assert.That(entry.Mode, Is.EqualTo(ReplicationMode.LwwRegister));
            Assert.That(entry.VectorClock, Is.Null);
            Assert.That(entry.DependencySummary, Is.Null);
            Assert.That(entry.DeltaKind, Is.Null);
            Assert.That(entry.DeltaPayload, Is.Null);
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var bytes = new byte[] { 1, 2 };
        var entry = new ReplogEntry
        {
            TreeId = "tree",
            Op = ReplogOp.Delete,
            Key = "k",
            EndExclusiveKey = "z",
            Value = bytes,
            Timestamp = ts,
            IsTombstone = true,
            ExpiresAtTicks = 42L,
            OriginClusterId = "site-a",
        };

        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.EqualTo("tree"));
            Assert.That(entry.Op, Is.EqualTo(ReplogOp.Delete));
            Assert.That(entry.Key, Is.EqualTo("k"));
            Assert.That(entry.EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(entry.Value, Is.SameAs(bytes));
            Assert.That(entry.Timestamp, Is.EqualTo(ts));
            Assert.That(entry.IsTombstone, Is.True);
            Assert.That(entry.ExpiresAtTicks, Is.EqualTo(42L));
            Assert.That(entry.OriginClusterId, Is.EqualTo("site-a"));
        });
    }

    [Test]
    public void Mode_is_settable_via_object_initialiser()
    {
        var entry = new ReplogEntry { TreeId = "t", Key = "k", Mode = ReplicationMode.LwwRegister };
        Assert.That(entry.Mode, Is.EqualTo(ReplicationMode.LwwRegister));
    }

    [Test]
    public void VectorClock_and_dependency_summary_are_settable_via_object_initialiser()
    {
        var vc = new VersionVector();
        vc.Tick("site-a");
        var entry = new ReplogEntry
        {
            TreeId = "t",
            Key = "k",
            VectorClock = vc,
            DependencySummary = vc,
        };
        Assert.Multiple(() =>
        {
            Assert.That(entry.VectorClock, Is.SameAs(vc));
            Assert.That(entry.DependencySummary, Is.SameAs(vc));
        });
    }

    // -- Gap (vii): IsTombstone + non-null VectorClock on a Set op ----

    [Test]
    public void Tombstone_flag_and_vector_clock_coexist_on_set_op_via_initialiser()
    {
        // Degenerate but contractually allowed: a Set entry that is
        // also flagged as a tombstone (e.g. hand-authored test
        // fixtures that pre-stage a tombstone for replay) carries the
        // causal-plus frontier verbatim. The record struct does not
        // collapse, drop, or transform either field.
        var vc = new VersionVector();
        vc.Tick("site-a");
        var entry = new ReplogEntry
        {
            TreeId = "t",
            Op = ReplogOp.Set,
            Key = "k",
            Value = new byte[] { 1 },
            IsTombstone = true,
            VectorClock = vc,
            DependencySummary = vc,
        };

        Assert.Multiple(() =>
        {
            Assert.That(entry.Op, Is.EqualTo(ReplogOp.Set));
            Assert.That(entry.IsTombstone, Is.True);
            Assert.That(entry.VectorClock, Is.SameAs(vc));
            Assert.That(entry.DependencySummary, Is.SameAs(vc));
        });
    }

    [Test]
    public void Equality_uses_value_semantics_on_shared_byte_references()
    {
        var bytes = new byte[] { 9 };
        var c = new ReplogEntry { TreeId = "t", Key = "k", Value = bytes };
        var d = new ReplogEntry { TreeId = "t", Key = "k", Value = bytes };
        Assert.That(c, Is.EqualTo(d));
    }
}

[TestFixture]
public class ReplogOpTests
{
    [Test]
    public void Underlying_values_are_stable()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)ReplogOp.Set, Is.EqualTo(0));
            Assert.That((int)ReplogOp.Delete, Is.EqualTo(1));
            Assert.That((int)ReplogOp.DeleteRange, Is.EqualTo(2));
        });
    }
}

[TestFixture]
public class ReplogEntryDeltaSlotTests
{
    [Test]
    public void Delta_kind_and_payload_are_settable_via_object_initialiser()
    {
        var payload = new byte[] { 9, 8, 7 };
        var entry = new ReplogEntry
        {
            TreeId = "t",
            Key = "k",
            DeltaKind = "ol.crdt.ors.add",
            DeltaPayload = payload,
        };

        Assert.Multiple(() =>
        {
            Assert.That(entry.DeltaKind, Is.EqualTo("ol.crdt.ors.add"));
            Assert.That(entry.DeltaPayload, Is.SameAs(payload));
        });
    }

    [Test]
    public void Delta_slots_default_to_null_when_unset()
    {
        var entry = new ReplogEntry { TreeId = "t", Key = "k" };
        Assert.Multiple(() =>
        {
            Assert.That(entry.DeltaKind, Is.Null);
            Assert.That(entry.DeltaPayload, Is.Null);
        });
    }
}
