using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Orleans-serializer wire-shape round-trips for the public per-key history read
/// surface: <see cref="EntryRevision"/> and <see cref="EntryHistoryPage"/>. Pins
/// that every field survives serialize/deserialize so the read path and the State
/// API decode the same shape across the wire.
/// </summary>
[TestFixture]
public sealed class EntryHistorySerializerRoundTripTests
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
    public void EntryRevision_round_trips_every_field()
    {
        var revision = new EntryRevision
        {
            Hlc = new HybridLogicalClock { WallClockTicks = 77, Counter = 4 },
            Kind = HistoryRowKind.CrdtDelta,
            SourceKey = "k",
            OriginClusterId = "cluster-b",
            ValuePreview = new byte[] { 1, 2 },
            ValueLength = 9,
            ValueTruncated = true,
            ValueHash = -7,
            Delta = new byte[] { 3, 4, 5 },
            Mode = LatticeMergeMode.OrSet,
            RetentionShape = HistoryRetentionMode.Hybrid,
            EndKey = "z",
            VectorClock = new VersionVector(),
        };

        var decoded = RoundTrip(revision);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Hlc, Is.EqualTo(revision.Hlc));
            Assert.That(decoded.Kind, Is.EqualTo(revision.Kind));
            Assert.That(decoded.SourceKey, Is.EqualTo(revision.SourceKey));
            Assert.That(decoded.OriginClusterId, Is.EqualTo(revision.OriginClusterId));
            Assert.That(decoded.ValuePreview, Is.EqualTo(revision.ValuePreview));
            Assert.That(decoded.ValueLength, Is.EqualTo(revision.ValueLength));
            Assert.That(decoded.ValueTruncated, Is.True);
            Assert.That(decoded.ValueHash, Is.EqualTo(revision.ValueHash));
            Assert.That(decoded.Delta, Is.EqualTo(revision.Delta));
            Assert.That(decoded.Mode, Is.EqualTo(revision.Mode));
            Assert.That(decoded.RetentionShape, Is.EqualTo(revision.RetentionShape));
            Assert.That(decoded.EndKey, Is.EqualTo(revision.EndKey));
            Assert.That(decoded.VectorClock, Is.Not.Null);
        });
    }

    [Test]
    public void EntryHistoryPage_round_trips_every_field()
    {
        var page = new EntryHistoryPage
        {
            Revisions = new[]
            {
                new EntryRevision { Hlc = new HybridLogicalClock { WallClockTicks = 1 }, Kind = HistoryRowKind.Set, SourceKey = "k" },
                new EntryRevision { Hlc = new HybridLogicalClock { WallClockTicks = 2 }, Kind = HistoryRowKind.Delete, SourceKey = "k" },
            },
            Continuation = "k/0000000000000002.00000000",
            Truncated = true,
            EarliestAvailable = new HybridLogicalClock { WallClockTicks = 1 },
            Source = EntryHistorySource.WalWindow,
        };

        var decoded = RoundTrip(page);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Revisions, Has.Count.EqualTo(2));
            Assert.That(decoded.Revisions[0].Kind, Is.EqualTo(HistoryRowKind.Set));
            Assert.That(decoded.Revisions[1].Kind, Is.EqualTo(HistoryRowKind.Delete));
            Assert.That(decoded.Continuation, Is.EqualTo(page.Continuation));
            Assert.That(decoded.Truncated, Is.True);
            Assert.That(decoded.EarliestAvailable, Is.EqualTo(page.EarliestAvailable));
            Assert.That(decoded.Source, Is.EqualTo(EntryHistorySource.WalWindow));
        });
    }

    [Test]
    public void EntryHistoryPage_defaults_are_empty_and_none()
    {
        var page = new EntryHistoryPage();

        Assert.Multiple(() =>
        {
            Assert.That(page.Revisions, Is.Empty);
            Assert.That(page.Continuation, Is.Null);
            Assert.That(page.Truncated, Is.False);
            Assert.That(page.Source, Is.EqualTo(EntryHistorySource.None));
        });
    }
}
