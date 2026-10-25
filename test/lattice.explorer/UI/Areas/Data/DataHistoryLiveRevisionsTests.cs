using System.Text;
using Bunit;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>Live revisions keep their identity, chronology and mutation kind across history pages.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataHistoryLiveRevisionsTests : DataTestContext
{
    private static readonly DateTimeOffset Start = new(2026, 9, 28, 14, 0, 0, TimeSpan.Zero);

    [Test]
    public async Task Loading_older_revisions_does_not_forget_an_already_seen_live_change()
    {
        SeedHistory(60);
        var cut = RenderAt("data/orders?tab=history&key=k");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));
        var change = FakeStateClient.Change("orders", "k", ticks: Start.AddMinutes(70).UtcTicks);
        await Client.Feeds[^1].Writer.WriteAsync(change);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(51)));

        Button(cut, "Load older revisions").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(61)));
        await Client.Feeds[^1].Writer.WriteAsync(change);
        await TestPoll.UntilAsync(() => Client.DeliveredChangeCount == 2, "the duplicate live notification to be consumed");
        await cut.InvokeAsync(() => { });

        Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(61));
    }

    [Test]
    public async Task An_older_durable_page_replaces_its_live_placeholder_instead_of_duplicating_it()
    {
        SeedHistory(60);
        var cut = RenderAt("data/orders?tab=history&key=k");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));
        await Client.Feeds[^1].Writer.WriteAsync(FakeStateClient.Change("orders", "k", ticks: Start.UtcTicks));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__kind").Select(kind => kind.TextContent), Does.Contain("Live set")));

        Button(cut, "Load older revisions").Click();
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain("GetEntryHistoryAsync:50")));
        await cut.InvokeAsync(() => { });

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(60));
            Assert.That(cut.FindAll(".lt-data-timeline__kind").Select(kind => kind.TextContent), Has.None.EqualTo("Live set"));
            Assert.That(cut.FindAll("pre").Any(value => value.TextContent.Contains("v0", StringComparison.Ordinal)), Is.True);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Key_history_orders_delayed_live_notifications_by_revision_clock(bool newestFirst)
    {
        SeedHistory(2);
        var cut = RenderAt("data/orders?tab=history&key=k");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));
        if (!newestFirst)
        {
            Switch(cut, "Newest first").Click();
        }

        await Client.Feeds[^1].Writer.WriteAsync(FakeStateClient.Change("orders", "k", ticks: Start.AddMinutes(3).UtcTicks));
        await Client.Feeds[^1].Writer.WriteAsync(FakeStateClient.Change("orders", "k", ticks: Start.AddSeconds(30).UtcTicks));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(4)));

        var expected = new[]
        {
            "2026-09-28 14:00:00 UTC", "2026-09-28 14:00:30 UTC",
            "2026-09-28 14:01:00 UTC", "2026-09-28 14:03:00 UTC",
        };
        Assert.That(cut.FindAll(".lt-data-timeline__time").Select(time => time.TextContent),
            Is.EqualTo(newestFirst ? expected.Reverse() : expected));
    }

    [Test]
    public async Task Prefix_history_orders_delayed_notifications_newest_first()
    {
        Client.WithTree("orders");
        var cut = RenderAt("data/orders?tab=history&prefix=order/");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));

        await Client.Feeds[^1].Writer.WriteAsync(FakeStateClient.Change("orders", "order/newer", ticks: Start.AddMinutes(3).UtcTicks));
        await Client.Feeds[^1].Writer.WriteAsync(FakeStateClient.Change("orders", "order/older", ticks: Start.UtcTicks));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(2)));

        Assert.That(cut.FindAll(".lt-data-timeline__head a").Select(link => link.TextContent),
            Is.EqualTo(new[] { "order/newer", "order/older" }));
    }

    [Test]
    public async Task A_delayed_prefix_notification_cannot_evict_a_newer_change_at_the_limit()
    {
        Client.WithTree("orders");
        var cut = RenderAt("data/orders?tab=history&prefix=order/");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));
        for (var index = 1; index <= DataHistoryPanel.PrefixChangeLimit; index++)
        {
            await Client.Feeds[^1].Writer.WriteAsync(FakeStateClient.Change(
                "orders", $"order/{index}", ticks: Start.AddMinutes(index).UtcTicks));
        }

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(DataHistoryPanel.PrefixChangeLimit)));
        await Client.Feeds[^1].Writer.WriteAsync(FakeStateClient.Change("orders", "order/delayed", ticks: Start.UtcTicks));
        await TestPoll.UntilAsync(
            () => Client.DeliveredChangeCount == DataHistoryPanel.PrefixChangeLimit + 1,
            "the delayed notification to be consumed at the prefix history limit");
        await cut.InvokeAsync(() => { });

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(DataHistoryPanel.PrefixChangeLimit));
            Assert.That(cut.FindAll(".lt-data-timeline__head a").Select(link => link.TextContent), Has.None.EqualTo("order/delayed"));
            Assert.That(cut.FindAll(".lt-data-timeline__head a")[^1].TextContent, Is.EqualTo("order/1"));
        });
    }

    [TestCase(StateChangeKind.Set, "k", null, "Live set")]
    [TestCase(StateChangeKind.Delete, "k", null, "Live delete")]
    [TestCase(StateChangeKind.DeleteRange, "a", "z", "Live range delete")]
    public async Task Live_revisions_describe_the_actual_mutation_kind(StateChangeKind kind, string key, string? end, string expected)
    {
        SeedHistory(1);
        var cut = RenderAt("data/orders?tab=history&key=k");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));

        await Client.Feeds[^1].Writer.WriteAsync(FakeStateClient.Change("orders", key, kind, Start.AddMinutes(3).UtcTicks) with
        {
            EndExclusiveKey = end,
        });
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(2)));

        Assert.That(cut.FindAll(".lt-data-timeline__kind")[0].TextContent, Is.EqualTo(expected));
    }

    private void SeedHistory(int count)
    {
        Client.WithTree("orders");
        Client.History[("orders", "k")] =
        [
            .. Enumerable.Range(0, count).Select(index => new EntryRevisionRecord
            {
                SourceKey = "k",
                Hlc = new HybridLogicalClock { WallClockTicks = Start.AddMinutes(index).UtcTicks },
                Kind = HistoryRowKind.Set,
                ValuePreview = Encoding.UTF8.GetBytes($"\"v{index}\""),
                Retention = new RevisionRetention { Mode = HistoryRetentionMode.FullValue, ValueRetained = true },
            }),
        ];
    }
}
