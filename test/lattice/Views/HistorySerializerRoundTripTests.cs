using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Orleans-serializer wire-shape round-trips for the durable-history serializable
/// surface: <see cref="HistoryRow"/>, <see cref="HistoryRetentionSettings"/>, the
/// <see cref="RuntimeViewRegistration.Accumulative"/> slot, and the
/// <see cref="TreeRegistryEntry"/> history-retention slots. Pins that each field
/// survives serialize/deserialize and that a legacy payload leaving the new slots
/// unset decodes to the documented defaults.
/// </summary>
[TestFixture]
public sealed class HistorySerializerRoundTripTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() => _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void HistoryRow_round_trips_every_field()
    {
        var row = new HistoryRow
        {
            Timestamp = new HybridLogicalClock { WallClockTicks = 123, Counter = 9 },
            Kind = HistoryRowKind.CrdtDelta,
            SourceKey = "k",
            OriginClusterId = "cluster-b",
            Value = new byte[] { 1, 2 },
            Delta = new byte[] { 3, 4, 5 },
            ValueHash = -42,
            ValueLength = 2,
            Mode = LatticeMergeMode.OrSet,
            RetentionShape = HistoryRetentionMode.Hybrid,
            EndKey = "z",
        };

        var decoded = RoundTrip(row);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Timestamp, Is.EqualTo(row.Timestamp));
            Assert.That(decoded.Kind, Is.EqualTo(row.Kind));
            Assert.That(decoded.SourceKey, Is.EqualTo(row.SourceKey));
            Assert.That(decoded.OriginClusterId, Is.EqualTo(row.OriginClusterId));
            Assert.That(decoded.Value, Is.EqualTo(row.Value));
            Assert.That(decoded.Delta, Is.EqualTo(row.Delta));
            Assert.That(decoded.ValueHash, Is.EqualTo(row.ValueHash));
            Assert.That(decoded.ValueLength, Is.EqualTo(row.ValueLength));
            Assert.That(decoded.Mode, Is.EqualTo(row.Mode));
            Assert.That(decoded.RetentionShape, Is.EqualTo(row.RetentionShape));
            Assert.That(decoded.EndKey, Is.EqualTo(row.EndKey));
        });
    }

    [Test]
    public void HistoryRetentionSettings_round_trips()
    {
        var settings = new HistoryRetentionSettings
        {
            Mode = HistoryRetentionMode.FullValue,
            Window = TimeSpan.FromHours(6),
        };

        Assert.That(RoundTrip(settings), Is.EqualTo(settings));
    }

    [Test]
    public void RuntimeViewRegistration_runtime_fields_round_trip()
    {
        var record = new RuntimeViewRegistration
        {
            ViewName = "history-orders",
            SourceTreeId = "orders",
            ProjectionTypeName = "Some.Type, Some.Assembly",
            ProjectionVersion = "history-v1",
            Accumulative = true,
            ProjectionProviderKey = "app.history.v1",
            ProjectionProviderPayload = [1, 2, 3],
        };

        var decoded = RoundTrip(record);

        Assert.That(decoded.Accumulative, Is.True);
        Assert.That(decoded.IsAggregation, Is.False);
        Assert.That(decoded.ProjectionProviderKey, Is.EqualTo("app.history.v1"));
        Assert.That(decoded.ProjectionProviderPayload, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public void RuntimeViewRegistration_legacy_accumulative_defaults_false()
    {
        var record = new RuntimeViewRegistration
        {
            ViewName = "v",
            SourceTreeId = "t",
            ProjectionTypeName = "T, A",
            ProjectionVersion = "x",
        };

        var decoded = RoundTrip(record);
        Assert.Multiple(() =>
        {
            Assert.That(decoded.Accumulative, Is.False);
            Assert.That(decoded.ProjectionProviderKey, Is.Null);
            Assert.That(decoded.ProjectionProviderPayload, Is.Null);
        });
    }

    [Test]
    public void TreeRegistryEntry_history_slots_round_trip()
    {
        var entry = new TreeRegistryEntry
        {
            HistoryRetentionMode = HistoryRetentionMode.Hybrid,
            HistoryRetentionWindowTicks = TimeSpan.FromDays(30).Ticks,
        };

        var decoded = RoundTrip(entry);

        Assert.That(decoded.HistoryRetentionMode, Is.EqualTo(HistoryRetentionMode.Hybrid));
        Assert.That(decoded.HistoryRetentionWindowTicks, Is.EqualTo(TimeSpan.FromDays(30).Ticks));
    }

    [Test]
    public void TreeRegistryEntry_legacy_history_slots_default_null()
    {
        var entry = new TreeRegistryEntry { PublishEvents = true };

        var decoded = RoundTrip(entry);

        Assert.That(decoded.HistoryRetentionMode, Is.Null);
        Assert.That(decoded.HistoryRetentionWindowTicks, Is.Null);
    }
}
