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
    public void Properties_are_settable_via_object_initialiser()
    {
        var entry = new ReplogEntry { TreeId = "t", Op = ReplogOp.Set, Key = "k" };
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
        var entry = new ReplogEntry
        {
            TreeId = "tree",
            Op = ReplogOp.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Timestamp = ts,
            OriginClusterId = "site-a",
            Mode = ReplicationMode.LwwRegister,
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
            Assert.That(copy.Entries[0].Mode, Is.EqualTo(ReplicationMode.LwwRegister));
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
            Entries = Array.Empty<ReplogEntry>(),
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
            .Select(i => new ReplogEntry
            {
                TreeId = "tree",
                Op = ReplogOp.Set,
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
}
