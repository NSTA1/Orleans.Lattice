using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class WalRecordTests
{
    [Test]
    public void Default_instance_has_empty_or_zero_fields()
    {
        var entry = default(WalRecord);
        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.Null);
            Assert.That(entry.Op, Is.EqualTo(MutationKind.Set));
            Assert.That(entry.Key, Is.Null);
            Assert.That(entry.EndExclusiveKey, Is.Null);
            Assert.That(entry.Value, Is.Null);
            Assert.That(entry.Timestamp, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(entry.IsTombstone, Is.False);
            Assert.That(entry.ExpiresAtTicks, Is.EqualTo(0L));
            Assert.That(entry.OriginClusterId, Is.Null);
            Assert.That(entry.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(entry.VectorClock, Is.Null);
            Assert.That(entry.DependencySummary, Is.Null);
            Assert.That(entry.Delta, Is.Null);
            Assert.That(entry.AtomicBatchSize, Is.EqualTo(0));
            Assert.That(entry.AtomicBatchIndex, Is.EqualTo(0));
            Assert.That(entry.ShardIndex, Is.EqualTo(0));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var bytes = new byte[] { 1, 2 };
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Delete,
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
            Assert.That(entry.Op, Is.EqualTo(MutationKind.Delete));
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
        var entry = new WalRecord { TreeId = "t", Key = "k", Mode = LatticeMergeMode.LwwRegister };
        Assert.That(entry.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
    }

    [Test]
    public void VectorClock_and_dependency_summary_are_settable_via_object_initialiser()
    {
        var vc = new VersionVector();
        vc.Tick("site-a");
        var entry = new WalRecord
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
        var entry = new WalRecord
        {
            TreeId = "t",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            IsTombstone = true,
            VectorClock = vc,
            DependencySummary = vc,
        };

        Assert.Multiple(() =>
        {
            Assert.That(entry.Op, Is.EqualTo(MutationKind.Set));
            Assert.That(entry.IsTombstone, Is.True);
            Assert.That(entry.VectorClock, Is.SameAs(vc));
            Assert.That(entry.DependencySummary, Is.SameAs(vc));
        });
    }

    [Test]
    public void Equality_uses_value_semantics_on_shared_byte_references()
    {
        var bytes = new byte[] { 9 };
        var c = new WalRecord { TreeId = "t", Key = "k", Value = bytes };
        var d = new WalRecord { TreeId = "t", Key = "k", Value = bytes };
        Assert.That(c, Is.EqualTo(d));
    }
}

[TestFixture]
public class WalRecordOpTests
{
    [Test]
    public void Underlying_values_are_stable()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)MutationKind.Set, Is.EqualTo(0));
            Assert.That((int)MutationKind.Delete, Is.EqualTo(1));
            Assert.That((int)MutationKind.DeleteRange, Is.EqualTo(2));
        });
    }
}

[TestFixture]
public class WalRecordDeltaSlotTests
{
    [Test]
    public void Delta_payload_is_settable_via_object_initialiser()
    {
        var payload = new byte[] { 9, 8, 7 };
        var entry = new WalRecord
        {
            TreeId = "t",
            Key = "k",
            Delta = payload,
        };

        Assert.That(entry.Delta, Is.SameAs(payload));
    }

    [Test]
    public void Delta_slot_defaults_to_null_when_unset()
    {
        var entry = new WalRecord { TreeId = "t", Key = "k" };
        Assert.That(entry.Delta, Is.Null);
    }
}

[TestFixture]
public class WalRecordAtomicBatchSlotTests
{
    [Test]
    public void Atomic_batch_size_and_index_are_settable_via_object_initialiser()
    {
        var entry = new WalRecord
        {
            TreeId = "t",
            Key = "k",
            AtomicBatchSize = 5,
            AtomicBatchIndex = 2,
        };

        Assert.Multiple(() =>
        {
            Assert.That(entry.AtomicBatchSize, Is.EqualTo(5));
            Assert.That(entry.AtomicBatchIndex, Is.EqualTo(2));
        });
    }

    [Test]
    public void Atomic_batch_slots_default_to_zero_when_unset()
    {
        var entry = new WalRecord { TreeId = "t", Key = "k" };
        Assert.Multiple(() =>
        {
            Assert.That(entry.AtomicBatchSize, Is.EqualTo(0));
            Assert.That(entry.AtomicBatchIndex, Is.EqualTo(0));
        });
    }

    [Test]
    public void Atomic_batch_slots_participate_in_record_struct_equality()
    {
        // Defensive: the record-struct equality contract must include
        // both atomic-batch slots so a future caller that compares
        // WalRecord instances (for example a deduplication cache
        // keyed off entry equality) does not silently treat two
        // entries differing only in (Size, Index) as identical.
        var baseline = new WalRecord { TreeId = "t", Key = "k", AtomicBatchSize = 5, AtomicBatchIndex = 2 };
        var differentIndex = new WalRecord { TreeId = "t", Key = "k", AtomicBatchSize = 5, AtomicBatchIndex = 3 };
        var differentSize = new WalRecord { TreeId = "t", Key = "k", AtomicBatchSize = 4, AtomicBatchIndex = 2 };
        var identical = new WalRecord { TreeId = "t", Key = "k", AtomicBatchSize = 5, AtomicBatchIndex = 2 };

        Assert.Multiple(() =>
        {
            Assert.That(baseline, Is.Not.EqualTo(differentIndex));
            Assert.That(baseline, Is.Not.EqualTo(differentSize));
            Assert.That(baseline, Is.EqualTo(identical));
        });
    }
}

[TestFixture]
public class WalRecordAtomicBatchRoundTripTests
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private WalRecord RoundTrip(WalRecord entry)
    {
        var bytes = _serializer.SerializeToArray(entry);
        return _serializer.Deserialize(bytes);
    }

    [Test]
    public void Atomic_batch_slots_round_trip_with_explicit_values()
    {
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.LwwRegister,
            AtomicBatchSize = 5,
            AtomicBatchIndex = 2,
        };

        var decoded = RoundTrip(entry);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.AtomicBatchSize, Is.EqualTo(5));
            Assert.That(decoded.AtomicBatchIndex, Is.EqualTo(2));
        });
    }

    [Test]
    public void Atomic_batch_slots_round_trip_zero_for_legacy_decode()
    {
        // Wire-compat: a producer that never sets the slots emits the
        // default zero value; receivers must decode the same shape so
        // a tree opt-in flip cannot break legacy traffic.
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        };

        var decoded = RoundTrip(entry);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.AtomicBatchSize, Is.EqualTo(0));
            Assert.That(decoded.AtomicBatchIndex, Is.EqualTo(0));
        });
    }
}

[TestFixture]
public class WalRecordShardIndexRoundTripTests
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private WalRecord RoundTrip(WalRecord entry)
    {
        var bytes = _serializer.SerializeToArray(entry);
        return _serializer.Deserialize(bytes);
    }

    [Test]
    public void ShardIndex_round_trips_with_explicit_value()
    {
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.LwwRegister,
            ShardIndex = 5,
        };

        var decoded = RoundTrip(entry);

        Assert.That(decoded.ShardIndex, Is.EqualTo(5));
    }

    [Test]
    public void ShardIndex_round_trips_zero_for_legacy_decode()
    {
        // Wire-compat: a pre-Option-A producer that never sets the
        // slot emits the default zero value; receivers must decode
        // the same shape so legacy WAL entries upgraded in place
        // read as shard 0.
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        };

        var decoded = RoundTrip(entry);

        Assert.That(decoded.ShardIndex, Is.EqualTo(0));
    }
}

[TestFixture]
public class WalRecordShardIndexSlotTests
{
    [Test]
    public void ShardIndex_is_settable_via_object_initialiser()
    {
        var entry = new WalRecord { TreeId = "t", Key = "k", ShardIndex = 3 };
        Assert.That(entry.ShardIndex, Is.EqualTo(3));
    }

    [Test]
    public void ShardIndex_defaults_to_zero_when_unset()
    {
        var entry = new WalRecord { TreeId = "t", Key = "k" };
        Assert.That(entry.ShardIndex, Is.EqualTo(0));
    }

    [Test]
    public void ShardIndex_participates_in_record_struct_equality()
    {
        var baseline = new WalRecord { TreeId = "t", Key = "k", ShardIndex = 2 };
        var different = new WalRecord { TreeId = "t", Key = "k", ShardIndex = 3 };
        var identical = new WalRecord { TreeId = "t", Key = "k", ShardIndex = 2 };

        Assert.Multiple(() =>
        {
            Assert.That(baseline, Is.Not.EqualTo(different));
            Assert.That(baseline, Is.EqualTo(identical));
        });
    }
}
