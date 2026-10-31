using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Keys tab across a connection that degrades and recovers (found by journey C2,
/// #5390): the shared connection ends a live feed once it stops resubscribing, and
/// the tab says so and follows the feed again, reading the page afresh, once the
/// connection is back - never a switch reading "Live updates" over a feed that ended.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataKeysPanelConnectionTests : DataTestContext
{
    /// <summary>Puts the in-memory state API behind a connection whose health the test moves.</summary>
    public DataKeysPanelConnectionTests()
    {
        Connection = new FakeLiveStateConnection(Client);
        Services.AddSingleton<ILatticeStateClient>(Connection);
    }

    private FakeLiveStateConnection Connection { get; }

    [Test]
    public void A_feed_ended_by_a_degraded_connection_is_reported_and_followed_again_when_it_is_back()
    {
        Client.WithTree("orders", keys: 2);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(2));
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
        });

        // The connection degrades and gives up resubscribing: the feed ends without an error.
        Connection.Raise(FakeLiveStateConnection.Faulted);
        Client.Feeds[^1].Writer.TryComplete();
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-note").TextContent, Does.Contain(DataKeysPanel.FeedLostNote));
            Assert.That(Client.OpenFeeds, Is.Zero);
        });

        // Meanwhile a key is written; the connection comes back.
        var feeds = Client.Feeds.Count;
        Client.Entries["orders"]["key/0009"] = FakeStateClient.Entry("key/0009", "\"written while away\"");
        Connection.Raise(FakeLiveStateConnection.Connected);

        cut.WaitUntil(() =>
        {
            Assert.That(Client.Feeds, Has.Count.EqualTo(feeds + 1), "the feed is followed again");
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
            Assert.That(cut.FindAll(".lt-data-note"), Is.Empty);
            Assert.That(Rows(cut), Has.Count.EqualTo(3), "the page is read again, so the key written meanwhile shows");
        });
    }

    [Test]
    public void A_feed_ended_by_a_degraded_connection_can_be_restarted_by_hand()
    {
        Client.WithTree("orders", keys: 2);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));

        Connection.Raise(FakeLiveStateConnection.Faulted);
        Client.Feeds[^1].Writer.TryComplete();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-note").TextContent, Does.Contain(DataKeysPanel.FeedLostNote)));

        Button(cut, "Restart live updates").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
            Assert.That(cut.FindAll(".lt-data-note"), Is.Empty);
        });

        // Restarted by hand, the tab does not follow a second feed when the connection reports back.
        var feeds = Client.Feeds.Count;
        Connection.Raise(FakeLiveStateConnection.Connected);
        Assert.That(Client.Feeds, Has.Count.EqualTo(feeds));
    }

    [Test]
    public void A_connection_that_recovers_without_losing_the_feed_changes_nothing()
    {
        Client.WithTree("orders", keys: 2);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));
        var feeds = Client.Feeds.Count;

        Connection.Raise(FakeLiveStateConnection.Faulted);
        Connection.Raise(FakeLiveStateConnection.Connected);

        Assert.Multiple(() =>
        {
            Assert.That(Client.Feeds, Has.Count.EqualTo(feeds));
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
            Assert.That(cut.FindAll(".lt-data-note"), Is.Empty);
        });
    }
}
