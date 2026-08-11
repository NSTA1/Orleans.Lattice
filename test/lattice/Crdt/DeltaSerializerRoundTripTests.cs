using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// End-to-end Orleans serializer round-trip tests for every typed CRDT
/// delta record. These are the safety net for the wire format - the
/// alias-hygiene tests prove an alias is registered, but only an actual
/// serialize / deserialize round-trip proves the codegen produces a
/// working envelope and that none of the <c>[Id(...)]</c> slots have
/// been silently reordered or dropped.
/// </summary>
[TestFixture]
public class DeltaSerializerRoundTripTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        var bytes = serializer.SerializeToArray(value);
        return serializer.Deserialize(bytes);
    }

    [Test]
    public void LwwRegisterDelta_round_trips_set_payload()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var original = new LwwRegisterDelta
        {
            Value = new byte[] { 1, 2, 3, 4 },
            Timestamp = ts,
            IsTombstone = false,
            ExpiresAtTicks = 9_876_543_210L,
            OriginClusterId = "site-a",
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Value, Is.EqualTo(original.Value));
            Assert.That(copy.Timestamp, Is.EqualTo(original.Timestamp));
            Assert.That(copy.IsTombstone, Is.EqualTo(original.IsTombstone));
            Assert.That(copy.ExpiresAtTicks, Is.EqualTo(original.ExpiresAtTicks));
            Assert.That(copy.OriginClusterId, Is.EqualTo(original.OriginClusterId));
        });
    }

    [Test]
    public void LwwRegisterDelta_round_trips_tombstone()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var original = LwwRegisterDelta.Tombstone(ts, "site-b");

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Value, Is.Null);
            Assert.That(copy.IsTombstone, Is.True);
            Assert.That(copy.Timestamp, Is.EqualTo(original.Timestamp));
            Assert.That(copy.OriginClusterId, Is.EqualTo("site-b"));
        });
    }

    [Test]
    public void OrSetDeltaDot_round_trips()
    {
        var original = new OrSetDeltaDot { Element = new byte[] { 0xCA, 0xFE }, ReplicaId = "r-1", Counter = 42L };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Element, Is.EqualTo(original.Element));
            Assert.That(copy.ReplicaId, Is.EqualTo(original.ReplicaId));
            Assert.That(copy.Counter, Is.EqualTo(original.Counter));
        });
    }

    [Test]
    public void OrSetDelta_round_trips()
    {
        var addOne = new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "r1", Counter = 1 };
        var addTwo = new OrSetDeltaDot { Element = new byte[] { 2 }, ReplicaId = "r1", Counter = 2 };
        var remove = new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "r2", Counter = 7 };
        var original = new OrSetDelta
        {
            Adds = new[] { addOne, addTwo },
            Removes = new[] { remove },
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Adds, Has.Count.EqualTo(2));
            Assert.That(copy.Adds[0].Element, Is.EqualTo(addOne.Element));
            Assert.That(copy.Adds[0].ReplicaId, Is.EqualTo(addOne.ReplicaId));
            Assert.That(copy.Adds[0].Counter, Is.EqualTo(addOne.Counter));
            Assert.That(copy.Adds[1].Counter, Is.EqualTo(addTwo.Counter));
            Assert.That(copy.Removes, Has.Count.EqualTo(1));
            Assert.That(copy.Removes[0].ReplicaId, Is.EqualTo(remove.ReplicaId));
            Assert.That(copy.Removes[0].Counter, Is.EqualTo(remove.Counter));
        });
    }

    [Test]
    public void OrSetDelta_empty_round_trips()
    {
        var copy = RoundTrip(OrSetDelta.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Adds, Is.Not.Null);
            Assert.That(copy.Adds, Is.Empty);
            Assert.That(copy.Removes, Is.Not.Null);
            Assert.That(copy.Removes, Is.Empty);
        });
    }

    [Test]
    public void RwSetDelta_round_trips()
    {
        var add = new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "r1", Counter = 1 };
        var remove = new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "r2", Counter = 7 };
        var tombstone = new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "r3", Counter = 5 };
        var original = new RwSetDelta
        {
            Adds = new[] { add },
            Removes = new[] { remove },
            Tombstones = new[] { tombstone },
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Adds, Has.Count.EqualTo(1));
            Assert.That(copy.Adds[0].Element, Is.EqualTo(add.Element));
            Assert.That(copy.Adds[0].ReplicaId, Is.EqualTo(add.ReplicaId));
            Assert.That(copy.Adds[0].Counter, Is.EqualTo(add.Counter));
            Assert.That(copy.Removes, Has.Count.EqualTo(1));
            Assert.That(copy.Removes[0].ReplicaId, Is.EqualTo(remove.ReplicaId));
            Assert.That(copy.Tombstones, Has.Count.EqualTo(1));
            Assert.That(copy.Tombstones[0].ReplicaId, Is.EqualTo(tombstone.ReplicaId));
            Assert.That(copy.Tombstones[0].Counter, Is.EqualTo(tombstone.Counter));
        });
    }

    [Test]
    public void RwSetDelta_empty_round_trips()
    {
        var copy = RoundTrip(RwSetDelta.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Adds, Is.Not.Null);
            Assert.That(copy.Adds, Is.Empty);
            Assert.That(copy.Removes, Is.Not.Null);
            Assert.That(copy.Removes, Is.Empty);
            Assert.That(copy.Tombstones, Is.Not.Null);
            Assert.That(copy.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void PnCounterDelta_round_trips()
    {
        var original = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["r1"] = 5, ["r2"] = 100 },
            Decrements = new Dictionary<string, long> { ["r1"] = 1 },
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Increments, Has.Count.EqualTo(2));
            Assert.That(copy.Increments["r1"], Is.EqualTo(5L));
            Assert.That(copy.Increments["r2"], Is.EqualTo(100L));
            Assert.That(copy.Decrements, Has.Count.EqualTo(1));
            Assert.That(copy.Decrements["r1"], Is.EqualTo(1L));
        });
    }

    [Test]
    public void PnCounterDelta_empty_round_trips()
    {
        var copy = RoundTrip(PnCounterDelta.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Increments, Is.Not.Null);
            Assert.That(copy.Increments, Is.Empty);
            Assert.That(copy.Decrements, Is.Not.Null);
            Assert.That(copy.Decrements, Is.Empty);
        });
    }

    [Test]
    public void GCounterDelta_round_trips()
    {
        var original = new GCounterDelta
        {
            Increments = new Dictionary<string, long> { ["r1"] = 5, ["r2"] = 100 },
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Increments, Has.Count.EqualTo(2));
            Assert.That(copy.Increments["r1"], Is.EqualTo(5L));
            Assert.That(copy.Increments["r2"], Is.EqualTo(100L));
        });
    }

    [Test]
    public void GCounterDelta_empty_round_trips()
    {
        var copy = RoundTrip(GCounterDelta.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Increments, Is.Not.Null);
            Assert.That(copy.Increments, Is.Empty);
        });
    }

    [Test]
    public void VersionVectorDelta_round_trips()
    {
        var clockOne = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var clockTwo = HybridLogicalClock.Tick(clockOne);
        var original = new VersionVectorDelta
        {
            Entries = new Dictionary<string, HybridLogicalClock>
            {
                ["r1"] = clockOne,
                ["r2"] = clockTwo,
            },
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Entries, Has.Count.EqualTo(2));
            Assert.That(copy.Entries["r1"], Is.EqualTo(clockOne));
            Assert.That(copy.Entries["r2"], Is.EqualTo(clockTwo));
        });
    }

    [Test]
    public void VersionVectorDelta_empty_round_trips()
    {
        var copy = RoundTrip(VersionVectorDelta.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Entries, Is.Not.Null);
            Assert.That(copy.Entries, Is.Empty);
        });
    }

    [Test]
    public void RgaDeltaNode_round_trips()
    {
        var original = new RgaDeltaNode
        {
            ReplicaId = "r-1",
            Counter = 7L,
            ParentDot = new OrSetDot { ReplicaId = "r-0", Counter = 3L },
            Value = new byte[] { 0xDE, 0xAD },
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.ReplicaId, Is.EqualTo(original.ReplicaId));
            Assert.That(copy.Counter, Is.EqualTo(original.Counter));
            Assert.That(copy.ParentDot, Is.EqualTo(original.ParentDot));
            Assert.That(copy.Value, Is.EqualTo(original.Value));
            Assert.That(copy.Dot, Is.EqualTo(new OrSetDot { ReplicaId = "r-1", Counter = 7L }));
        });
    }

    [Test]
    public void RgaDelta_round_trips()
    {
        var insertOne = new RgaDeltaNode
        {
            ReplicaId = "r1",
            Counter = 1,
            ParentDot = Rga.Root,
            Value = new byte[] { 1 },
        };
        var insertTwo = new RgaDeltaNode
        {
            ReplicaId = "r1",
            Counter = 2,
            ParentDot = new OrSetDot { ReplicaId = "r1", Counter = 1 },
            Value = new byte[] { 2 },
        };
        var tombstone = new OrSetDot { ReplicaId = "r2", Counter = 9 };
        var original = new RgaDelta
        {
            Inserts = new[] { insertOne, insertTwo },
            Tombstones = new[] { tombstone },
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Inserts, Has.Count.EqualTo(2));
            Assert.That(copy.Inserts[0].ReplicaId, Is.EqualTo(insertOne.ReplicaId));
            Assert.That(copy.Inserts[0].Counter, Is.EqualTo(insertOne.Counter));
            Assert.That(copy.Inserts[0].ParentDot, Is.EqualTo(insertOne.ParentDot));
            Assert.That(copy.Inserts[1].ParentDot, Is.EqualTo(insertTwo.ParentDot));
            Assert.That(copy.Inserts[1].Value, Is.EqualTo(insertTwo.Value));
            Assert.That(copy.Tombstones, Has.Count.EqualTo(1));
            Assert.That(copy.Tombstones[0], Is.EqualTo(tombstone));
        });
    }

    [Test]
    public void RgaDelta_empty_round_trips()
    {
        var copy = RoundTrip(RgaDelta.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Inserts, Is.Not.Null);
            Assert.That(copy.Inserts, Is.Empty);
            Assert.That(copy.Tombstones, Is.Not.Null);
            Assert.That(copy.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void GSetDelta_round_trips()
    {
        var original = new GSetDelta
        {
            Adds = new[] { new byte[] { 1, 2 }, new byte[] { 3, 4, 5 } },
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Adds, Has.Count.EqualTo(2));
            Assert.That(copy.Adds[0], Is.EqualTo(original.Adds[0]));
            Assert.That(copy.Adds[1], Is.EqualTo(original.Adds[1]));
        });
    }

    [Test]
    public void GSetDelta_empty_round_trips()
    {
        var copy = RoundTrip(GSetDelta.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Adds, Is.Not.Null);
            Assert.That(copy.Adds, Is.Empty);
        });
    }

    [Test]
    public void BoundedRegisterDelta_round_trips_candidate()
    {
        var original = new BoundedRegisterDelta
        {
            Value = new byte[] { 0xBE, 0xEF },
            OrderKey = new byte[] { 0x00, 0x01, 0x02 },
            HasValue = true,
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Value, Is.EqualTo(original.Value));
            Assert.That(copy.OrderKey, Is.EqualTo(original.OrderKey));
            Assert.That(copy.HasValue, Is.True);
        });
    }

    [Test]
    public void BoundedRegisterDelta_empty_round_trips()
    {
        var copy = RoundTrip(BoundedRegisterDelta.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(copy.HasValue, Is.False);
            Assert.That(copy.Value, Is.Null);
            Assert.That(copy.OrderKey, Is.Null);
        });
    }
}
