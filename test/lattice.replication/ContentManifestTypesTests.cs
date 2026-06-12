using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for the content-hash payload-elision public wire types
/// (<see cref="ContentManifestEntry"/>, <see cref="ContentManifestRequest"/>,
/// and <see cref="ContentManifestResponse"/>).
/// </summary>
[TestFixture]
public sealed class ContentManifestTypesTests
{
    [Test]
    public void Entry_properties_round_trip_through_init()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 100, Counter = 2 };
        var entry = new ContentManifestEntry
        {
            EntryIndex = 3,
            Key = "k",
            ContentHash = 0xDEADBEEFUL,
            Hlc = hlc,
        };

        Assert.Multiple(() =>
        {
            Assert.That(entry.EntryIndex, Is.EqualTo(3));
            Assert.That(entry.Key, Is.EqualTo("k"));
            Assert.That(entry.ContentHash, Is.EqualTo(0xDEADBEEFUL));
            Assert.That(entry.Hlc, Is.EqualTo(hlc));
        });
    }

    [Test]
    public void Entry_default_has_zero_fields()
    {
        var entry = default(ContentManifestEntry);

        Assert.Multiple(() =>
        {
            Assert.That(entry.EntryIndex, Is.Zero);
            Assert.That(entry.Key, Is.Null);
            Assert.That(entry.ContentHash, Is.Zero);
            Assert.That(entry.Hlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public void Entry_value_equality_holds()
    {
        var a = new ContentManifestEntry { EntryIndex = 1, Key = "k", ContentHash = 7UL };
        var b = new ContentManifestEntry { EntryIndex = 1, Key = "k", ContentHash = 7UL };

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Request_properties_round_trip_through_init()
    {
        var entries = new[]
        {
            new ContentManifestEntry { EntryIndex = 0, Key = "a", ContentHash = 1UL },
            new ContentManifestEntry { EntryIndex = 1, Key = "b", ContentHash = 2UL },
        };
        var request = new ContentManifestRequest
        {
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = entries,
        };

        Assert.Multiple(() =>
        {
            Assert.That(request.TreeName, Is.EqualTo("tree"));
            Assert.That(request.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(request.Entries, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Response_properties_round_trip_through_init()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 50, Counter = 1 };
        var response = new ContentManifestResponse
        {
            ExchangeSupported = true,
            MissingEntryIndices = new[] { 0, 2 },
            AdvancedHlc = hlc,
        };

        Assert.Multiple(() =>
        {
            Assert.That(response.ExchangeSupported, Is.True);
            Assert.That(response.MissingEntryIndices, Is.EqualTo(new[] { 0, 2 }));
            Assert.That(response.AdvancedHlc, Is.EqualTo(hlc));
        });
    }

    [Test]
    public void NotSupported_reports_unsupported_with_empty_missing_set()
    {
        var response = ContentManifestResponse.NotSupported;

        Assert.Multiple(() =>
        {
            Assert.That(response.ExchangeSupported, Is.False);
            Assert.That(response.MissingEntryIndices, Is.Empty);
            Assert.That(response.AdvancedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }
}
