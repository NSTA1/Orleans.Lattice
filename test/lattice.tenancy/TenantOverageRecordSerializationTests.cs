using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Persistence round-trip coverage for <see cref="TenantOverageRecord"/> through the
/// exact <see cref="OrleansLatticeSerializer{T}"/> the overage store uses. A record
/// with several clusters' grow-only counter components must recover its identity and
/// every component's value with full fidelity, because the durable
/// <c>sys-tenant-overage</c> state is stored and read back through this serializer.
/// The contrasting default-JSON test documents why the Orleans serializer is
/// required: the JSON path silently hollows the private-init id and the internal
/// grow-only counters, the exact defect the binary serializer fixes.
/// </summary>
[TestFixture]
public sealed class TenantOverageRecordSerializationTests
{
    private static TenantOverageRecord Populated() =>
        OverageRecord(
            "acme",
            ("east", Overage(100, 1, 10, 1)),
            ("west", Overage(200, 2, 20, 2)),
            ("north", Overage(300, 3, 30, 3)));

    [Test]
    public void Orleans_serializer_round_trips_the_grow_only_counters_with_full_fidelity()
    {
        var record = Populated();
        var serializer = TestSerializers.For<TenantOverageRecord>();

        var recovered = serializer.Deserialize(serializer.Serialize(record));

        Assert.Multiple(() =>
        {
            Assert.That(recovered.Id, Is.EqualTo(TenantId.Parse("acme")), "id");
            Assert.That(recovered.ClusterCount, Is.EqualTo(3), "every cluster component survives");
            Assert.That(recovered.LocalOverage("east"), Is.EqualTo(Overage(100, 1, 10, 1)), "east component");
            Assert.That(recovered.LocalOverage("west"), Is.EqualTo(Overage(200, 2, 20, 2)), "west component");
            Assert.That(recovered.LocalOverage("north"), Is.EqualTo(Overage(300, 3, 30, 3)), "north component");
            Assert.That(recovered.Fold(), Is.EqualTo(Overage(600, 6, 60, 6)), "the fold survives");
        });
    }

    [Test]
    public void Default_json_serializer_hollows_the_record_which_is_why_the_store_uses_orleans()
    {
        var record = Populated();
        var json = JsonLatticeSerializer<TenantOverageRecord>.Default;

        var hollow = json.Deserialize(json.Serialize(record));

        // System.Text.Json cannot set the private-init Id or the internal grow-only
        // counters, so the record comes back hollow. This is the exact defect the
        // OrleansLatticeSerializer fixes; the store must never use this path.
        Assert.Multiple(() =>
        {
            Assert.That(hollow.Id.Value, Is.Null, "the JSON path drops the private-init id");
            Assert.That(hollow.ClusterCount, Is.EqualTo(0), "the JSON path drops the internal counter state");
            Assert.That(hollow.Fold(), Is.EqualTo(TenantOverageSample.Empty));
        });
    }
}
