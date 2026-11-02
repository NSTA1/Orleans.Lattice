using System.Text;
using Bunit;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// #5583: surfaces that preview a value's stored bytes - a key's history, dead
/// letters, a schema preview - show its body, as the Keys tab does, not the
/// schema-version envelope drawn as hexadecimal.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaEnvelopePreviewTests : DataTestContext
{
    private const uint SchemaId = 7;
    private const uint Version = 2;
    private const string First = "{\"order\":100128,\"total\":3400,\"status\":\"shipped\"}";
    private const string Second = "{\"order\":100128,\"total\":3500,\"status\":\"shipped\"}";
    private static readonly DateTimeOffset Start = new(2026, 9, 28, 14, 0, 0, TimeSpan.Zero);

    [Test]
    public void A_versioned_revision_shows_its_body_as_json_not_the_envelope_as_hex()
    {
        SeedHistory(First);

        var cut = RenderAt("data/orders?tab=history&key=k");

        cut.WaitUntil(() =>
        {
            var value = cut.Find(".lt-data-timeline__rev .lt-data-value").TextContent;
            Assert.That(value, Does.Contain("\"total\": 3400"));
            Assert.That(value, Does.Not.Contain("fe 01"));
        });
    }

    [Test]
    public void Versioned_revisions_diff_their_bodies()
    {
        SeedHistory(First, Second);

        var cut = RenderAt("data/orders?tab=history&key=k");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-diff__line--added").TextContent, Does.Contain("\"total\": 3500"));
            Assert.That(cut.Find(".lt-data-diff__line--removed").TextContent, Does.Contain("\"total\": 3400"));
            Assert.That(cut.Find(".lt-data-timeline").TextContent, Does.Not.Contain("fe 01"));
        });
    }

    [Test]
    public void A_versioned_revision_reports_its_body_size_and_schema_version()
    {
        SeedHistory(First);

        var cut = RenderAt("data/orders?tab=history&key=k");

        cut.WaitUntil(() =>
        {
            var meta = cut.FindAll(".lt-data-timeline__rev .lt-data-timeline__meta").Select(element => element.TextContent).ToList();
            Assert.That(meta, Does.Contain($"{Encoding.UTF8.GetByteCount(First)} B"));
            Assert.That(meta, Does.Contain($"schema version {Version}"));
        });
    }

    [Test]
    public void A_truncated_versioned_preview_still_shows_its_body()
    {
        var stored = Enveloped(First);
        Client.WithTree("orders");
        Client.History[("orders", "k")] =
        [
            Revision(0, stored[..30]) with { ValueLength = stored.Length, Truncated = true },
        ];

        var cut = RenderAt("data/orders?tab=history&key=k");

        cut.WaitUntil(() =>
        {
            var value = cut.Find(".lt-data-timeline__rev .lt-data-value").TextContent;
            Assert.That(value, Does.StartWith("{\"order\":100128"));
            Assert.That(value, Does.Not.Contain("fe 01"));
        });
    }

    [Test]
    public void An_unversioned_revision_is_shown_as_stored()
    {
        Client.WithTree("orders");
        Client.History[("orders", "k")] = [Revision(0, Encoding.UTF8.GetBytes(First))];

        var cut = RenderAt("data/orders?tab=history&key=k");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-timeline__rev .lt-data-value").TextContent, Does.Contain("\"total\": 3400"));
            Assert.That(cut.FindAll(".lt-data-timeline__meta").Select(element => element.TextContent), Has.None.StartsWith("schema version"));
        });
    }

    [Test]
    public void A_versioned_dead_letter_shows_its_body()
    {
        Client.WithTree("orders");
        var stored = Enveloped(First);
        Client.DeadLetters["orders"] =
        [
            new DeadLetterEntryRecord
            {
                Key = "order/100128",
                ValuePreview = stored,
                ValueByteLength = stored.Length,
                Reason = "stamped newer than the target",
                Source = DeadLetterSourceKind.Replication,
                TimestampUtc = Start,
            },
        ];
        var cut = RenderAt("data/orders?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button.lt-data-link"), Has.Count.EqualTo(1)));

        cut.Find("button.lt-data-link").Click();

        cut.WaitUntil(() =>
        {
            var shown = cut.Find("section.lt-data-entry pre").TextContent;
            Assert.That(shown, Does.Contain("\"total\": 3400"));
            Assert.That(shown, Does.Not.Contain("fe 01"));
        });
    }

    [Test]
    public void A_schema_preview_of_a_versioned_value_reads_as_its_body()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormat.Preview(Enveloped(First)), Is.EqualTo(First));
            Assert.That(SchemaFormat.Preview(Enveloped(First)[..20], truncated: true), Is.EqualTo(First[..10]));
        });
    }

    private static byte[] Enveloped(string body) => LatticeSchemaEnvelope.Encode(SchemaId, Version, Encoding.UTF8.GetBytes(body));

    private static EntryRevisionRecord Revision(int index, byte[] preview) => new()
    {
        SourceKey = "k",
        Hlc = new HybridLogicalClock { WallClockTicks = Start.AddMinutes(index).UtcTicks },
        Kind = HistoryRowKind.Set,
        ValuePreview = preview,
        ValueLength = preview.Length,
        Retention = new RevisionRetention { Mode = HistoryRetentionMode.FullValue, ValueRetained = true },
    };

    private void SeedHistory(params string[] bodies)
    {
        Client.WithTree("orders");
        Client.History[("orders", "k")] = [.. bodies.Select((body, index) => Revision(index, Enveloped(body)))];
    }
}
