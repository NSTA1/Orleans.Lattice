using System.Text;
using Bunit;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>History and metrics replies belong only to the request that still owns the panel.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataHistoryAndMetricsOwnershipTests : DataTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Metrics_discard_a_superseded_reply_or_fault(bool fault)
    {
        Client.WithTree("old").WithTree("current");
        var pending = new TaskCompletionSource<TreeMetricsSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.MetricsReply = (request, _) => request.TreeIds!.Contains("old")
            ? pending.Task
            : Task.FromResult(Metrics("current", 42));
        var cut = RenderAt("data/old?tab=metrics");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain(nameof(FakeStateClient.GetMetricsSnapshotAsync))));
        var panel = cut.FindComponent<DataMetricsPanel>().Instance;

        Navigation.NavigateTo("data/current?tab=metrics");
        cut.WaitUntil(() => Assert.That(cut.Find("figure").TextContent, Does.Contain("42")));
        Assert.That(cut.FindComponent<DataMetricsPanel>().Instance, Is.SameAs(panel));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old metrics failed"));
        }
        else
        {
            pending.SetResult(Metrics("old", 999));
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.FindAll("figure").Any(figure => figure.TextContent.Contains("999", StringComparison.Ordinal))
                || cut.FindAll(".lt-empty").Any(empty => empty.TextContent.Contains("The metrics did not load", StringComparison.Ordinal)),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Find("figure").TextContent, Does.Contain("42"));
    }

    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(true, true)]
    public async Task History_discard_a_superseded_page_or_fault(bool fault, bool asOf)
    {
        Client.WithTree("orders");
        var pending = new TaskCompletionSource<EntryHistoryResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.HistoryReply = (request, _) => request.Key == "old" && request.ToHlc is null
            ? pending.Task
            : Task.FromResult(History(request.Key, "current-value"));
        var cut = RenderAt("data/orders?tab=history&key=old");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain(nameof(FakeStateClient.GetEntryHistoryAsync) + ":first")));
        var panel = cut.FindComponent<DataHistoryPanel>().Instance;

        Navigation.NavigateTo(asOf
            ? "data/orders?tab=history&key=old&at=2026-09-28T14:00:00Z"
            : "data/orders?tab=history&key=current");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-timeline").TextContent, Does.Contain("current-value")));
        Assert.That(cut.FindComponent<DataHistoryPanel>().Instance, Is.SameAs(panel));
        var follows = Client.ObserveRequests.Count;
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old history failed"));
        }
        else
        {
            pending.SetResult(History("old", "stale-value") with { ContinuationToken = "stale-cursor" });
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.FindAll(".lt-data-timeline").Any(timeline => timeline.TextContent.Contains("stale-value", StringComparison.Ordinal))
                || cut.FindAll(".lt-empty").Any(empty => empty.TextContent.Contains("The history did not load", StringComparison.Ordinal))
                || Client.ObserveRequests.Count != follows,
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(1));
        Assert.That(cut.FindAll("button").Any(button => button.TextContent.Trim() == "Load older revisions"), Is.False);
    }

    [Test]
    public async Task Metrics_a_previous_reply_cannot_end_the_current_loading_state()
    {
        Client.WithTree("old").WithTree("current");
        var previous = new TaskCompletionSource<TreeMetricsSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        var current = new TaskCompletionSource<TreeMetricsSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        var oldStarted = false;
        var currentStarted = false;
        Client.MetricsReply = (request, _) =>
        {
            if (request.TreeIds?.Contains("old") == true)
            {
                oldStarted = true;
                return previous.Task;
            }

            if (request.TreeIds?.Contains("current") == true)
            {
                currentStarted = true;
                return current.Task;
            }

            return Task.FromResult(new TreeMetricsSnapshot());
        };
        var cut = RenderAt("data/old?tab=metrics");
        cut.WaitUntil(() => Assert.That(oldStarted, Is.True));

        Navigation.NavigateTo("data/current?tab=metrics");
        cut.WaitUntil(() => Assert.That(currentStarted, Is.True));
        previous.SetResult(Metrics("old", 999));

        Assert.That(await TestPoll.TryUntilAsync(
            () => !cut.FindAll(".lt-skeleton").Any(skeleton => skeleton.TextContent.Contains("Loading the metrics", StringComparison.Ordinal)),
            TimeSpan.FromSeconds(1)), Is.False);
        current.SetResult(Metrics("current", 42));
        cut.WaitUntil(() => Assert.That(cut.Find("figure").TextContent, Does.Contain("42")));
    }

    [Test]
    public async Task History_discard_changes_delivered_by_a_cancelled_prefix_follow()
    {
        Client.WithTree("orders");
        Client.IgnoreObserveCancellation = true;
        var cut = RenderAt("data/orders?tab=history&prefix=old/");
        cut.WaitUntil(() => Assert.That(Client.Feeds, Has.Count.EqualTo(1)));
        var oldFeed = Client.Feeds[0];

        Navigation.NavigateTo("data/orders?tab=history&prefix=current/");
        cut.WaitUntil(() => Assert.That(Client.Feeds, Has.Count.EqualTo(2)));
        var currentFeed = Client.Feeds[1];
        try
        {
            await currentFeed.Writer.WriteAsync(FakeStateClient.Change("orders", "current/key"));
            cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-timeline").TextContent, Does.Contain("current/key")));
            await oldFeed.Writer.WriteAsync(FakeStateClient.Change("orders", "old/stale"));

            Assert.That(await TestPoll.TryUntilAsync(
                () => cut.FindAll(".lt-data-timeline").Any(timeline => timeline.TextContent.Contains("old/stale", StringComparison.Ordinal)),
                TimeSpan.FromSeconds(1)), Is.False);
            Assert.That(cut.FindAll(".lt-data-timeline__rev"), Has.Count.EqualTo(1));
        }
        finally
        {
            oldFeed.Writer.TryComplete();
            currentFeed.Writer.TryComplete();
        }
    }

    private static TreeMetricsSnapshot Metrics(string tree, long keys) => new()
    {
        Trees = [new TreeMetrics { TreeId = tree, ShardCount = 4, LiveKeys = keys }],
    };

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
                Hlc = new HybridLogicalClock { WallClockTicks = 639_000_000_000_000_000 },
                Kind = HistoryRowKind.Set,
                ValuePreview = Encoding.UTF8.GetBytes(value),
                ValueLength = value.Length,
                Retention = new RevisionRetention { Mode = HistoryRetentionMode.FullValue, ValueRetained = true },
            },
        ],
    };
}
