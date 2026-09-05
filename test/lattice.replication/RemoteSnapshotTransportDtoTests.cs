using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for the two wire-shaped DTOs the cross-cluster snapshot transport
/// exchanges - <see cref="RemoteSnapshotMetadataRequest"/> and
/// <see cref="RemoteSnapshotStreamItem"/>. Both are public, Orleans-serialized
/// contract types shared by the gRPC binding and any future transport, so what
/// matters is that their slots round-trip, that value equality is structural
/// (the receiver dedupes on it), and that the wire-format attributes - a stable
/// alias plus sequential <c>[Id]</c> members - are present and unchanged.
/// </summary>
[TestFixture]
public class RemoteSnapshotTransportDtoTests
{
    private static HybridLogicalClock Hlc(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    // ----- RemoteSnapshotMetadataRequest -----

    [Test]
    public void MetadataRequest_round_trips_every_routing_slot()
    {
        var request = new RemoteSnapshotMetadataRequest
        {
            TreeName = "orders",
            SourceClusterId = "cluster-a",
            FromAsOfHlc = Hlc(12345, 7),
        };

        Assert.Multiple(() =>
        {
            Assert.That(request.TreeName, Is.EqualTo("orders"));
            Assert.That(request.SourceClusterId, Is.EqualTo("cluster-a"));
            Assert.That(request.FromAsOfHlc, Is.EqualTo(Hlc(12345, 7)));
        });
    }

    [Test]
    public void MetadataRequest_defaults_to_a_disabled_hlc_filter()
    {
        var request = default(RemoteSnapshotMetadataRequest);

        Assert.That(request.FromAsOfHlc, Is.EqualTo(HybridLogicalClock.Zero),
            "a fresh peer sends the zero clock, which disables the upper-bound filter");
    }

    [Test]
    public void MetadataRequest_equality_is_structural()
    {
        var a = new RemoteSnapshotMetadataRequest { TreeName = "orders", SourceClusterId = "c", FromAsOfHlc = Hlc(1) };
        var b = new RemoteSnapshotMetadataRequest { TreeName = "orders", SourceClusterId = "c", FromAsOfHlc = Hlc(1) };
        var different = a with { FromAsOfHlc = Hlc(2) };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
            Assert.That(a, Is.Not.EqualTo(different));
        });
    }

    [Test]
    public void MetadataRequest_carries_its_stable_wire_attributes()
    {
        var type = typeof(RemoteSnapshotMetadataRequest);
        Assert.Multiple(() =>
        {
            Assert.That(type.GetCustomAttributes(typeof(GenerateSerializerAttribute), false), Is.Not.Empty);
            Assert.That(type.GetCustomAttributes(typeof(ImmutableAttribute), false), Is.Not.Empty,
                "the DTO is never mutated after construction, so Orleans may skip copying it");
            Assert.That(type.GetCustomAttributes(typeof(AliasAttribute), false), Is.Not.Empty,
                "the alias pins the wire format across rolling upgrades");
        });
    }

    [Test]
    public void MetadataRequest_members_carry_sequential_ids()
    {
        Assert.Multiple(() =>
        {
            AssertId<RemoteSnapshotMetadataRequest>(nameof(RemoteSnapshotMetadataRequest.TreeName), 0);
            AssertId<RemoteSnapshotMetadataRequest>(nameof(RemoteSnapshotMetadataRequest.SourceClusterId), 1);
            AssertId<RemoteSnapshotMetadataRequest>(nameof(RemoteSnapshotMetadataRequest.FromAsOfHlc), 2);
        });
    }

    // ----- RemoteSnapshotStreamItem -----

    [Test]
    public void StreamItem_round_trips_the_wrapped_entry()
    {
        var entry = new SnapshotEntry { Key = "k", Value = [1, 2, 3] };
        var item = new RemoteSnapshotStreamItem { Entry = entry };

        Assert.That(item.Entry.Key, Is.EqualTo("k"));
        Assert.That(item.Entry.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public void StreamItem_defaults_to_a_zero_valued_entry()
    {
        var item = default(RemoteSnapshotStreamItem);

        Assert.That(item.Entry, Is.EqualTo(default(SnapshotEntry)),
            "a hand-constructed message that leaves the slot defaulted must decode as a zero-valued entry, not throw");
    }

    [Test]
    public void StreamItem_equality_is_structural()
    {
        var entry = new SnapshotEntry { Key = "k", Value = [1] };
        var a = new RemoteSnapshotStreamItem { Entry = entry };
        var b = new RemoteSnapshotStreamItem { Entry = entry };

        Assert.That(a, Is.EqualTo(b));
        Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
    }

    [Test]
    public void StreamItem_carries_its_stable_wire_attributes()
    {
        var type = typeof(RemoteSnapshotStreamItem);
        Assert.Multiple(() =>
        {
            Assert.That(type.GetCustomAttributes(typeof(GenerateSerializerAttribute), false), Is.Not.Empty);
            Assert.That(type.GetCustomAttributes(typeof(ImmutableAttribute), false), Is.Not.Empty);
            Assert.That(type.GetCustomAttributes(typeof(AliasAttribute), false), Is.Not.Empty);
        });
    }

    [Test]
    public void StreamItem_entry_carries_id_zero()
        => AssertId<RemoteSnapshotStreamItem>(nameof(RemoteSnapshotStreamItem.Entry), 0);

    [Test]
    public void StreamItem_wraps_rather_than_aliases_the_entry_shape()
    {
        Assert.That(typeof(RemoteSnapshotStreamItem), Is.Not.EqualTo(typeof(SnapshotEntry)),
            "the wrapper exists so the stream message shape can evolve without breaking the per-entry alias");
    }

    private static void AssertId<T>(string propertyName, uint expectedId)
    {
        var prop = typeof(T).GetProperty(propertyName);
        Assert.That(prop, Is.Not.Null, $"{propertyName} must exist");
        var idAttr = prop!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null, $"{propertyName} must carry [Id]");
        Assert.That(idAttr!.Id, Is.EqualTo(expectedId), $"{propertyName} must be [Id({expectedId})]");
    }
}
