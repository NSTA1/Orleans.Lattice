using System.Text;
using Bunit;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>History replies belong to the key and time window that requested them.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataHistorySupersededReadTests : DataTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public void A_superseded_history_reply_or_fault_cannot_replace_the_current_key(bool fails)
    {
        Client.WithTree("orders");
        var previous = new TaskCompletionSource<EntryHistoryResponse>();
        CancellationToken previousToken = default;
        Client.HistoryRead = (request, token) =>
        {
            if (request.Key == "old")
            {
                previousToken = token;
                return previous.Task;
            }

            return Task.FromResult(History(request.Key, "\"current value\""));
        };

        var cut = RenderAt("data/orders?tab=history&key=old");
        var panel = cut.FindComponent<DataHistoryPanel>();
        Navigation.NavigateTo("data/orders?tab=history&key=current");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(1)));
        Assert.That(cut.FindComponent<DataHistoryPanel>().Instance, Is.SameAs(panel.Instance));
        var renders = panel.RenderCount;

        if (fails)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult(History("old", "\"stale value\""));
        }

        panel.WaitForState(() => panel.RenderCount > renders);
        Assert.Multiple(() =>
        {
            Assert.That(previousToken.IsCancellationRequested, Is.True);
            Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(1));
            Assert.That(cut.Markup, Does.Contain("current value").And.Not.Contain("stale value"));
            Assert.That(cut.FindAll(".lt-empty"), Is.Empty);
            Assert.That(Client.OpenFeeds, Is.EqualTo(1), "a superseded load must not restart the current live tail");
        });
    }

    [Test]
    public void A_superseded_as_of_reply_cannot_change_the_same_keys_latest_history()
    {
        Client.WithTree("orders");
        var previous = new TaskCompletionSource<EntryHistoryResponse>();
        Client.HistoryRead = (request, _) => request.ToHlc is not null
            ? previous.Task
            : Task.FromResult(History("k", "\"latest value\""));
        var cut = RenderAt("data/orders?tab=history&key=k&at=2026-09-28T14:01:30Z");
        var panel = cut.FindComponent<DataHistoryPanel>();
        Navigation.NavigateTo("data/orders?tab=history&key=k");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(1)));
        var renders = panel.RenderCount;

        previous.SetResult(History("k", "\"historical value\""));

        panel.WaitForState(() => panel.RenderCount > renders);
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(1));
            Assert.That(cut.Find(".lt-data-timeline").TextContent, Does.Contain("latest value").And.Not.Contain("historical value"));
        });
    }

    [Test]
    public void A_superseded_history_load_cannot_clear_the_current_loading_state()
    {
        Client.WithTree("orders");
        var previous = new TaskCompletionSource<EntryHistoryResponse>();
        var current = new TaskCompletionSource<EntryHistoryResponse>();
        Client.HistoryRead = (request, _) => request.Key == "old" ? previous.Task : current.Task;
        var cut = RenderAt("data/orders?tab=history&key=old");
        var panel = cut.FindComponent<DataHistoryPanel>();
        Navigation.NavigateTo("data/orders?tab=history&key=current");
        var renders = panel.RenderCount;

        previous.SetResult(History("old", "\"stale value\""));

        panel.WaitForState(() => panel.RenderCount > renders);
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-skeleton"), Is.Not.Empty);
            Assert.That(cut.FindAll(".lt-data-timeline__rev"), Is.Empty);
            Assert.That(Client.OpenFeeds, Is.Zero);
        });
        current.SetResult(History("current", "\"current value\""));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(1)));
    }

    private static EntryHistoryResponse History(string key, string value) => new()
    {
        TreeId = "orders",
        Key = key,
        Status = StateQueryStatus.Found,
        Revisions =
        [
            new EntryRevisionRecord
            {
                SourceKey = key,
                Hlc = new HybridLogicalClock { WallClockTicks = new DateTimeOffset(2026, 9, 28, 14, 0, 0, TimeSpan.Zero).UtcTicks },
                Kind = HistoryRowKind.Set,
                ValuePreview = Encoding.UTF8.GetBytes(value),
                ValueLength = value.Length,
                Retention = new RevisionRetention { Mode = HistoryRetentionMode.FullValue, ValueRetained = true },
            },
        ],
    };
}
