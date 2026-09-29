using System.Text;
using Bunit;
using Grpc.Core;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The History tab: a key's timeline with diffs, order, older pages, a point in
/// time, the live tail; a prefix's live changes; and choosing a key.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataHistoryPanelTests : DataTestContext
{
    private static readonly DateTimeOffset Start = new(2026, 9, 28, 14, 0, 0, TimeSpan.Zero);

    [Test]
    public void The_timeline_lists_revisions_newest_first_with_a_diff_against_the_previous_value()
    {
        SeedHistory("orders", "k", "{\"status\":\"open\"}", "{\"status\":\"shipped\"}");

        var cut = RenderAt("data/orders?tab=history&key=k");

        cut.WaitUntil(() =>
        {
            var revisions = cut.FindAll(".lt-data-timeline__rev");
            Assert.That(revisions, Has.Count.EqualTo(2));
            Assert.That(revisions[0].QuerySelector(".lt-data-timeline__time")!.TextContent, Is.EqualTo("2026-09-28 14:01:00 UTC"));
            Assert.That(cut.Find(".lt-data-diff__line--added").TextContent, Does.Contain("+   \"status\": \"shipped\""));
            Assert.That(cut.Find(".lt-data-diff__line--removed").TextContent, Does.Contain("-   \"status\": \"open\""));
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("2 revisions"));
        });

        Switch(cut, "Newest first").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__time")[0].TextContent, Is.EqualTo("2026-09-28 14:00:00 UTC")));
    }

    [Test]
    public void Older_revisions_load_on_demand()
    {
        SeedHistory("orders", "k", [.. Enumerable.Range(0, 60).Select(i => $"\"v{i}\"")]);

        var cut = RenderAt("data/orders?tab=history&key=k");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(50)));

        Button(cut, "Load older revisions").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(60));
            Assert.That(cut.FindAll("button").Any(button => button.TextContent.Trim() == "Load older revisions"), Is.False);
        });
    }

    [Test]
    public void A_point_in_time_shows_the_key_as_it_stood_and_marks_the_revision_then_in_effect()
    {
        SeedHistory("orders", "k", "\"a\"", "\"b\"", "\"c\"");

        var cut = RenderAt("data/orders?tab=history&key=k&at=2026-09-28T14:01:30Z");

        cut.WaitUntil(() =>
        {
            var revisions = cut.FindAll(".lt-data-timeline__rev");
            Assert.That(revisions, Has.Count.EqualTo(2));
            Assert.That(revisions[0].GetAttribute("aria-current"), Is.EqualTo("true"));
            Assert.That(revisions[0].QuerySelector(".lt-node--join"), Is.Not.Null);
            Assert.That(revisions[1].GetAttribute("aria-current"), Is.Null);
            Assert.That(cut.Markup, Does.Contain("Showing the key as it stood at 2026-09-28 14:01:30 UTC"));
            Assert.That(cut.FindAll("button[role=switch]").Select(s => s.TextContent), Has.None.Contains("Follow new revisions"));
        });

        Button(cut, "Latest").Click();

        cut.WaitUntil(() => Assert.That(CurrentRelative, Is.EqualTo("/data/orders?tab=history&key=k")));
    }

    [Test]
    public void Setting_a_point_in_time_puts_it_in_the_address_and_a_bad_one_is_refused()
    {
        SeedHistory("orders", "k", "\"a\"");
        var cut = RenderAt("data/orders?tab=history&key=k");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(1)));

        Control(cut, "As of (UTC)").Input("not a time");
        cut.Find("form.lt-toolbar").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Write a time such as")));

        Control(cut, "As of (UTC)").Input("2026-09-28 14:05:00");
        cut.Find("form.lt-toolbar").Submit();

        cut.WaitUntil(() => Assert.That(CurrentRelative, Is.EqualTo("/data/orders?tab=history&key=k&at=2026-09-28T14%3A05%3A00Z")));
    }

    [Test]
    public void The_live_tail_appends_new_revisions_as_they_happen()
    {
        SeedHistory("orders", "k", "\"a\"");
        var cut = RenderAt("data/orders?tab=history&key=k");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));
        Assert.That(Client.ObserveRequests[^1].StartInclusive, Is.EqualTo("k"));

        Client.Feeds[^1].Writer.TryWrite(FakeStateClient.Change("orders", "k", ticks: Start.AddMinutes(5).UtcTicks));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(2));
            Assert.That(cut.FindAll(".lt-data-timeline__kind")[0].TextContent, Is.EqualTo("Live set"));
        });

        Switch(cut, "Follow new revisions").Click();

        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(0)));
    }

    [Test]
    public void With_only_a_prefix_the_tab_follows_every_change_under_it()
    {
        Client.WithTree("orders");
        var cut = RenderAt("data/orders?tab=history&prefix=order%2F");
        cut.WaitUntil(() =>
        {
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
            Assert.That(cut.Find(".lt-data-section-title").TextContent, Is.EqualTo("Changes under order/"));
        });

        Client.Feeds[^1].Writer.TryWrite(FakeStateClient.Change("orders", "order/9", StateChangeKind.Delete));

        cut.WaitUntil(() =>
        {
            var change = cut.Find(".lt-data-timeline__rev");
            Assert.That(change.TextContent, Does.Contain("Deleted").And.Contain("order/9"));
            Assert.That(change.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("data/orders?tab=history&prefix=order%2F&key=order%2F9"));
        });
    }

    [Test]
    public void With_neither_key_nor_prefix_the_tab_asks_for_a_key()
    {
        Client.WithTree("orders");
        var cut = RenderAt("data/orders?tab=history");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Choose a key")));

        Control(cut, "Key").Input("order/1");
        cut.Find(".lt-empty form").Submit();

        cut.WaitUntil(() => Assert.That(CurrentRelative, Is.EqualTo("/data/orders?tab=history&key=order%2F1")));
    }

    [Test]
    public void The_key_picker_offers_the_trees_keys_by_prefix_and_accepts_a_key_that_no_longer_exists()
    {
        Client.WithTree("orders", keys: 30, prefix: "order/");
        var cut = RenderAt("data/orders?tab=history");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Choose a key")));

        var offered = Orleans.Lattice.Explorer.Tests.UI.Suggestions.SuggestionFields.Offers(cut, "Key", "order/002");

        Assert.That(offered, Is.EqualTo(Enumerable.Range(20, 8).Select(i => $"order/{i:D4}")), "a bounded prefix scan, in key order");

        Control(cut, "Key").Input("order/deleted");
        cut.Find(".lt-empty form").Submit();
        cut.WaitUntil(() => Assert.That(CurrentRelative, Is.EqualTo("/data/orders?tab=history&key=order%2Fdeleted")));
    }
    [Test]
    public void An_expired_live_tail_offers_a_restart_and_a_history_failure_offers_a_retry()
    {
        SeedHistory("orders", "k", "\"a\"");
        var cut = RenderAt("data/orders?tab=history&key=k");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));

        Client.Feeds[^1].Writer.TryComplete(new LatticeStateCursorExpiredException());
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-note[role=status]").TextContent, Does.Contain("moved past this position")));
        Button(cut, "Restart live updates").Click();
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));

        Client.Fault = call => call == nameof(ILatticeStateClient.GetEntryHistoryAsync) ? new RpcException(new Status(StatusCode.Internal, "x")) : null;
        Navigation.NavigateTo("data/orders?tab=history&key=other");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h3").TextContent, Is.EqualTo("The history did not load")));
    }

    [Test]
    public void A_key_with_no_history_says_so()
    {
        Client.WithTree("orders");
        var cut = RenderAt("data/orders?tab=history&key=none");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h3").TextContent, Is.EqualTo("No revisions")));
    }

    private void SeedHistory(string tree, string key, params string[] values)
    {
        if (!Client.Entries.ContainsKey(tree))
        {
            Client.WithTree(tree);
        }

        Client.History[(tree, key)] =
        [
            .. values.Select((value, index) => new EntryRevisionRecord
            {
                SourceKey = key,
                Hlc = new HybridLogicalClock { WallClockTicks = Start.AddMinutes(index).UtcTicks },
                Kind = HistoryRowKind.Set,
                ValuePreview = Encoding.UTF8.GetBytes(value),
                ValueLength = value.Length,
                Retention = new RevisionRetention { Mode = HistoryRetentionMode.FullValue, ValueRetained = true },
            }),
        ];
    }
}
