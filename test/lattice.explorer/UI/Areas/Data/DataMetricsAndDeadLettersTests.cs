using System.Text;
using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>The Metrics and Dead letters tabs.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataMetricsAndDeadLettersTests : DataTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Metrics_from_a_previous_tree_cannot_replace_the_current_answer(bool fault)
    {
        Client.WithTree("orders").WithTree("current");
        var pending = new TaskCompletionSource<TreeMetrics?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reader = Substitute.For<IMetricsReader>();
        reader.GetAsync("orders", Arg.Any<CancellationToken>()).Returns(pending.Task);
        reader.GetAsync("current", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TreeMetrics?>(new TreeMetrics { TreeId = "current", LiveKeys = 222 }));
        Services.AddSingleton(reader);
        var cut = RenderAt("data/orders?tab=metrics");
        cut.WaitUntil(() => Assert.That(reader.ReceivedCalls().Count(), Is.GreaterThanOrEqualTo(2)));

        Navigation.NavigateTo("data/current?tab=metrics");
        cut.WaitUntil(() => Assert.That(cut.Find("figure").TextContent, Does.Contain("222")));

        if (fault)
        {
            pending.SetException(new InvalidOperationException("old read failed"));
        }
        else
        {
            pending.SetResult(new TreeMetrics { TreeId = "orders", LiveKeys = 111 });
        }

        var replaced = await TestPoll.TryUntilAsync(
            () => cut.FindAll("figure").Count == 0 || !cut.Find("figure").TextContent.Contains("222", StringComparison.Ordinal),
            TimeSpan.FromMilliseconds(300));
        Assert.That(replaced, Is.False, "the previous tree's late answer or fault must not change the current metrics");
    }

    [Test]
    public void Metrics_are_drawn_as_booktabs_figures_with_per_shard_hotness()
    {
        Client.WithTree("orders", shards: 2);
        Client.Metrics["orders"] = new TreeMetrics
        {
            TreeId = "orders",
            ShardCount = 2,
            LiveKeys = 48_210,
            Tombstones = 12,
            MinDepth = 2,
            MaxDepth = 3,
            ShardsSplitting = 1,
            ViewCount = 1,
            ViewLagTotal = 7,
            ShardHotness = [new ShardHotness { ShardIndex = 0, OpsPerSecond = 12.5, LiveKeys = 24_000 }, new ShardHotness { ShardIndex = 1, OpsPerSecond = 3, LiveKeys = 24_210, SplitInProgress = true }],
        };

        var cut = RenderAt("data/orders?tab=metrics");

        cut.WaitUntil(() =>
        {
            var figures = cut.FindAll("figure.lt-data-figure");
            Assert.That(figures, Has.Count.EqualTo(2));
            var measures = figures[0].QuerySelectorAll("tbody tr").Select(row => row.TextContent).ToArray();
            Assert.That(measures, Has.Some.Contains("Live keys").And.Some.Contains("48,210"));
            Assert.That(measures, Has.Some.Contains("Depth (min to max)2 to 3"));
            Assert.That(measures, Has.Some.Contains("View lag (entries, total)7"));
            Assert.That(figures[1].QuerySelectorAll("tbody tr"), Has.Length.EqualTo(2));
            Assert.That(figures[1].TextContent, Does.Contain("12.5").And.Contain("Yes"));
            Assert.That(cut.Markup, Does.Not.Contain("lt-card"));
        });
    }

    [Test]
    public void Paused_metrics_say_so_instead_of_showing_stale_numbers()
    {
        Client.WithTree("orders");
        Client.Metrics["orders"] = new TreeMetrics { TreeId = "orders", ShardCount = 4, LiveKeys = 99, DetailPaused = true, ShardHotness = [new ShardHotness()] };

        var cut = RenderAt("data/orders?tab=metrics");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-note").TextContent, Does.Contain("paused until it settles"));
            Assert.That(cut.FindAll("figure.lt-data-figure"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("figure").TextContent, Does.Contain("Paused").And.Not.Contain("99"));
        });
    }

    [Test]
    public void Missing_metrics_and_a_metrics_failure_each_have_their_state()
    {
        Client.WithTree("orders");
        var cut = RenderAt("data/orders?tab=metrics");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No metrics for this tree")));

        Client.Fault = call => call == nameof(ILatticeStateClient.GetMetricsSnapshotAsync) ? new InvalidOperationException("x") : null;
        Button(cut, "Refresh").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("The metrics did not load")));
    }

    [Test]
    public void Dead_letters_list_with_their_reason_and_open_for_inspection()
    {
        Client.WithTree("orders");
        Client.DeadLetters["orders"] =
        [
            Letter("order/1", "{\"total\":-1}", "total must be positive"),
            Letter("order/2", "oops", "not JSON"),
        ];

        var cut = RenderAt("data/orders?tab=dead-letters");
        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(2));
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("2 dead letters"));
            Assert.That(Rows(cut)[0].TextContent, Does.Contain("total must be positive").And.Contain("Local write"));
        });

        cut.Find("button.lt-data-link").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("button.lt-data-link").GetAttribute("aria-pressed"), Is.EqualTo("true"));
            var inspection = cut.Find("section.lt-data-entry");
            Assert.That(inspection.TextContent, Does.Contain("order/1").And.Contain("total must be positive"));
            Assert.That(inspection.QuerySelector("pre")!.TextContent, Does.Contain("\"total\": -1"));
        });
    }

    [Test]
    public void Dead_letters_page_with_load_more_and_an_empty_queue_says_so()
    {
        Client.WithTree("orders").WithTree("clean");
        Client.DeadLetters["orders"] = [.. Enumerable.Range(0, 60).Select(i => Letter($"k/{i:D2}", "x", "bad"))];

        var cut = RenderAt("data/orders?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(50)));
        Button(cut, "Load more dead letters").Click();
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(60)));

        Navigation.NavigateTo("data/clean?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No dead letters")));
    }

    [Test]
    public void A_restricted_identity_cannot_read_dead_letters_and_is_told_so()
    {
        Client.WithTree("orders");
        Client.Fault = call => call == nameof(ILatticeStateClient.ListDeadLettersAsync)
            ? new LatticeStateApiException("denied") { IsPermissionDenied = true }
            : null;

        var cut = RenderAt("data/orders?tab=dead-letters");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty").TextContent, Does.Contain("You do not have permission to read this tree's dead letters.")));
    }

    [Test]
    public void Compact_dead_letters_open_their_inspection_in_a_sheet()
    {
        Client.WithTree("orders");
        Client.DeadLetters["orders"] = [Letter("order/1", "x", "bad value")];

        var cut = RenderAt("data/orders?tab=dead-letters", compact: true);
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-compact-row__secondary").TextContent.Trim(), Is.EqualTo("bad value")));

        cut.Find(".lt-table-list__open").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog").TextContent, Does.Contain("bad value").And.Contain("Local write")));
    }

    private static DeadLetterEntryRecord Letter(string key, string value, string reason) => new()
    {
        Key = key,
        ValuePreview = Encoding.UTF8.GetBytes(value),
        ValueByteLength = value.Length,
        Reason = reason,
        Source = DeadLetterSourceKind.LocalRejected,
        TimestampUtc = new DateTimeOffset(2026, 9, 28, 12, 0, 0, TimeSpan.Zero),
    };
}
