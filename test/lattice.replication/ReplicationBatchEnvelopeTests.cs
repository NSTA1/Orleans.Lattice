using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationBatchEnvelopeTests
{
    private ServiceProvider _services = null!;
    private Serializer<ReplicationBatchEnvelope> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Default_instance_has_zero_or_null_fields()
    {
        var envelope = default(ReplicationBatchEnvelope);
        Assert.Multiple(() =>
        {
            Assert.That(envelope.WireVersion, Is.EqualTo(0));
            Assert.That(envelope.TreeName, Is.Null);
            Assert.That(envelope.OriginClusterId, Is.Null);
            Assert.That(envelope.Entries, Is.Null);
        });
    }

    [Test]
    public void CurrentVersion_is_one()
    {
        Assert.That(ReplicationBatchEnvelope.CurrentVersion, Is.EqualTo(1));
    }

    [Test]
    public void CurrentMinorVersion_is_one()
    {
        // Diagnostic minor version is bumped on strictly additive
        // changes to the envelope shape - e.g. a new [Id] slot on
        // WalRecord that legacy peers safely decode as null. Pinning
        // the value here guarantees a future bump is a deliberate
        // edit, not an accidental drift.
        Assert.That(ReplicationBatchEnvelope.CurrentMinorVersion, Is.EqualTo(1));
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var entry = new WalRecord { TreeId = "t", Op = MutationKind.Set, Key = "k" };
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 7,
            TreeName = "tree-x",
            OriginClusterId = "site-a",
            Entries = new[] { entry },
        };

        Assert.Multiple(() =>
        {
            Assert.That(envelope.WireVersion, Is.EqualTo(7));
            Assert.That(envelope.TreeName, Is.EqualTo("tree-x"));
            Assert.That(envelope.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(envelope.Entries, Has.Count.EqualTo(1));
            Assert.That(envelope.Entries[0].Key, Is.EqualTo("k"));
        });
    }

    [Test]
    public void Equality_is_record_struct_value_based_for_scalar_members()
    {
        // Reference-typed members (Entries) compare by reference under
        // record-struct equality; this test pins scalar-member equality
        // and reminds future maintainers that Entries-equality is
        // by-reference, not deep.
        var a = new ReplicationBatchEnvelope { WireVersion = 1, TreeName = "t", OriginClusterId = "o" };
        var b = new ReplicationBatchEnvelope { WireVersion = 1, TreeName = "t", OriginClusterId = "o" };

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Serializer_round_trips_envelope_with_entries()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Timestamp = ts,
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.LwwRegister,
        };
        var original = new ReplicationBatchEnvelope
        {
            WireVersion = ReplicationBatchEnvelope.CurrentVersion,
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = new[] { entry },
        };

        var bytes = _serializer.SerializeToArray(original);
        var copy = _serializer.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(copy.WireVersion, Is.EqualTo(original.WireVersion));
            Assert.That(copy.TreeName, Is.EqualTo(original.TreeName));
            Assert.That(copy.OriginClusterId, Is.EqualTo(original.OriginClusterId));
            Assert.That(copy.Entries, Has.Count.EqualTo(1));
            Assert.That(copy.Entries[0].Key, Is.EqualTo("k"));
            Assert.That(copy.Entries[0].Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(copy.Entries[0].Timestamp, Is.EqualTo(ts));
            Assert.That(copy.Entries[0].OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(copy.Entries[0].Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        });
    }

    [Test]
    public void Serializer_round_trips_empty_entry_list()
    {
        var original = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = Array.Empty<WalRecord>(),
        };

        var bytes = _serializer.SerializeToArray(original);
        var copy = _serializer.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Entries, Is.Not.Null);
            Assert.That(copy.Entries, Is.Empty);
            Assert.That(copy.TreeName, Is.EqualTo("tree"));
        });
    }

    [Test]
    public void Serializer_round_trips_large_entry_batch()
    {
        var entries = Enumerable.Range(0, 100)
            .Select(i => new WalRecord
            {
                TreeId = "tree",
                Op = MutationKind.Set,
                Key = $"k-{i}",
                Value = new byte[] { (byte)i, (byte)(i + 1) },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            })
            .ToArray();
        var original = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = entries,
        };

        var bytes = _serializer.SerializeToArray(original);
        var copy = _serializer.Deserialize(bytes);

        Assert.That(copy.Entries, Has.Count.EqualTo(100));
        Assert.That(copy.Entries[57].Key, Is.EqualTo("k-57"));
    }

    [Test]
    public void Serializer_decodes_legacy_entry_without_vector_clock_as_null()
    {
        // Pin the wire-additive contract: an entry authored without a
        // commit-time vector clock (every entry produced before the
        // causal-plus slots existed, and every local-write entry from
        // a host that does not stamp ambient frontiers) must round-trip
        // through the canonical encoder with VectorClock and
        // DependencySummary both null. Receivers treat null as the
        // empty frontier and behave identically to the per-origin-only
        // high-water-mark check.
        var legacy = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.LwwRegister,
        };
        var original = new ReplicationBatchEnvelope
        {
            WireVersion = ReplicationBatchEnvelope.CurrentVersion,
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = new[] { legacy },
        };

        var bytes = _serializer.SerializeToArray(original);
        var copy = _serializer.Deserialize(bytes);

        var entry = copy.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.VectorClock, Is.Null);
            Assert.That(entry.DependencySummary, Is.Null);
            Assert.That(entry.Key, Is.EqualTo("k"));
        });
    }

    [Test]
    public void Serializer_round_trips_entry_with_vector_clock_and_dependency_summary()
    {
        var vc = new VersionVector();
        vc.Entries["site-a"] = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        vc.Entries["site-b"] = HybridLogicalClock.Tick(HybridLogicalClock.Tick(HybridLogicalClock.Zero));

        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.LwwRegister,
            VectorClock = vc,
            DependencySummary = vc,
        };
        var original = new ReplicationBatchEnvelope
        {
            WireVersion = ReplicationBatchEnvelope.CurrentVersion,
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = new[] { entry },
        };

        var bytes = _serializer.SerializeToArray(original);
        var copy = _serializer.Deserialize(bytes);

        var decoded = copy.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(decoded.VectorClock, Is.Not.Null);
            Assert.That(decoded.VectorClock!.Entries, Has.Count.EqualTo(2));
            Assert.That(decoded.VectorClock.GetClock("site-a").WallClockTicks, Is.GreaterThan(0L));
            Assert.That(decoded.DependencySummary, Is.Not.Null);
            Assert.That(decoded.DependencySummary!.Entries, Has.Count.EqualTo(2));
        });
    }
}
