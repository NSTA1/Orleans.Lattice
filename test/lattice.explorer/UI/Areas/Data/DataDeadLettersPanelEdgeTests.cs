using System.Text;
using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Dead letters tab's edges: every source label, a missing or lost reader, a
/// failed count or page, reads cancelled by a tree change or by leaving the tab,
/// deselection, and the clipped, preview-noted inspection of a long value.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataDeadLettersPanelEdgeTests : DataTestContext
{
    private const string NoReader = "This Explorer has no state API to read dead letters through.";

    [TestCase(DeadLetterSource.Replication, "Replication")]
    [TestCase(DeadLetterSource.Restore, "Restore")]
    [TestCase(DeadLetterSource.LocalRejected, "Local write")]
    [TestCase((DeadLetterSource)99, "Unknown")]
    public void Each_source_is_named_for_the_path_that_diverted_it(DeadLetterSource source, string expected)
    {
        Assert.That(DataDeadLettersPanel.SourceText(source), Is.EqualTo(expected));
    }

    [Test]
    public void Replicated_and_restored_letters_show_their_source_in_the_row()
    {
        Client.WithTree("orders");
        Client.DeadLetters["orders"] =
        [
            Letter("a", "x", DeadLetterSourceKind.Replication),
            Letter("b", "x", DeadLetterSourceKind.Restore),
        ];

        var cut = RenderAt("data/orders?tab=dead-letters");

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(2));
            Assert.That(Rows(cut)[0].TextContent, Does.Contain("Replication"));
            Assert.That(Rows(cut)[1].TextContent, Does.Contain("Restore"));
        });
    }

    [Test]
    public void Without_a_workspace_the_panel_renders_nothing()
    {
        var cut = Render<DataDeadLettersPanel>();

        Assert.That(cut.Markup.Trim(), Is.Empty);
    }

    [Test]
    public void Without_a_reader_the_tab_says_there_is_no_state_api()
    {
        Client.WithTree("orders");
        Services.AddTransient<IDeadLetterReader>(_ => throw new InvalidOperationException("no state connection"));

        var cut = RenderAt("data/orders?tab=dead-letters");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("The dead letters did not load"));
            Assert.That(cut.Find(".lt-empty").TextContent, Does.Contain(NoReader));
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.Empty);
        });
        Assert.That(Client.Calls, Has.None.StartsWith(nameof(FakeStateClient.ListDeadLettersAsync)));
    }

    [Test]
    public void Losing_the_reader_before_load_more_reports_it_instead_of_paging()
    {
        Client.WithTree("orders");
        Client.DeadLetters["orders"] = [.. Enumerable.Range(0, 60).Select(i => Letter($"k/{i:D2}", "x"))];
        var available = true;
        Services.AddTransient<IDeadLetterReader>(services => available
            ? new DeadLetterReader(services.GetRequiredService<ILatticeStateClient>())
            : throw new InvalidOperationException("the connection went away"));
        var cut = RenderAt("data/orders?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(DataDeadLettersPanel.PageSize)));

        available = false;
        Button(cut, "Load more dead letters").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty").TextContent, Does.Contain(NoReader)));
        Assert.That(Client.Calls.Count(call => call.StartsWith(nameof(FakeStateClient.ListDeadLettersAsync), StringComparison.Ordinal)), Is.EqualTo(1));
    }

    [Test]
    public void A_failed_count_leaves_the_badge_blank_and_still_lists_the_letters()
    {
        Client.WithTree("orders");
        Client.DeadLetters["orders"] = [Letter("order/1", "x")];
        Client.Fault = call => call == nameof(ILatticeStateClient.GetDeadLetterCountAsync) ? new InvalidOperationException("count failed") : null;

        var cut = RenderAt("data/orders?tab=dead-letters");

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut).Single().TextContent, Does.Contain("order/1"));
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.Empty);
        });
    }

    [Test]
    public void A_failed_page_offers_a_retry_that_recovers()
    {
        Client.WithTree("orders");
        Client.DeadLetters["orders"] = [Letter("order/1", "x")];
        Client.Fault = call => call == nameof(ILatticeStateClient.ListDeadLettersAsync) ? new InvalidOperationException("list failed") : null;
        var cut = RenderAt("data/orders?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("The dead letters did not load")));

        Client.Fault = null;
        Button(cut, "Try again").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut).Single().TextContent, Does.Contain("order/1"));
            Assert.That(cut.FindAll(".lt-empty"), Is.Empty);
        });
    }

    [Test]
    public void A_count_cancelled_by_a_tree_change_gives_way_to_the_new_tree()
    {
        Client.WithTree("old").WithTree("current");
        Client.DeadLetters["current"] = [Letter("current-key", "x")];
        Client.DeadLetterCountReply = (request, token) => request.TreeId == "old"
            ? Never<DeadLetterCountResponse>(token)
            : Task.FromResult(new DeadLetterCountResponse { TreeId = request.TreeId, Count = 1 });
        var cut = RenderAt("data/old?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain(nameof(FakeStateClient.GetDeadLetterCountAsync))));

        Navigation.NavigateTo("data/current?tab=dead-letters");

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-key"));
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("1 dead letter"));
        });
        Assert.That(Client.Calls.Count(call => call == nameof(FakeStateClient.ListDeadLettersAsync) + ":first"), Is.EqualTo(1));
    }

    [Test]
    public void A_page_cancelled_by_a_tree_change_reports_no_failure()
    {
        Client.WithTree("old").WithTree("current");
        Client.DeadLetterPageReply = (request, token) => request.TreeId == "old"
            ? Never<DeadLetterQueuePage>(token)
            : Task.FromResult(new DeadLetterQueuePage { Entries = [Letter("current-key", "x")] });
        var cut = RenderAt("data/old?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain(nameof(FakeStateClient.ListDeadLettersAsync) + ":first")));

        Navigation.NavigateTo("data/current?tab=dead-letters");

        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-key")));
        Assert.That(cut.Markup, Does.Not.Contain("The dead letters did not load"));
    }

    [Test]
    public async Task Leaving_the_tab_mid_read_cancels_it_quietly()
    {
        Client.WithTree("orders");
        var observed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.DeadLetterCountReply = (_, token) =>
        {
            token.Register(() => observed.TrySetResult(true));
            return Never<DeadLetterCountResponse>(token);
        };
        var cut = RenderAt("data/orders?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain(nameof(FakeStateClient.GetDeadLetterCountAsync))));

        Navigation.NavigateTo("data/orders?tab=metrics");

        Assert.That(await observed.Task.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
        cut.WaitUntil(() => Assert.That(cut.FindComponents<DataDeadLettersPanel>(), Is.Empty));
        Assert.That(Client.Calls, Has.None.StartsWith(nameof(FakeStateClient.ListDeadLettersAsync)));
    }

    [Test]
    public void Selecting_the_open_letter_again_closes_its_inspection()
    {
        Client.WithTree("orders");
        Client.DeadLetters["orders"] = [Letter("order/1", "x")];
        var cut = RenderAt("data/orders?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(1)));

        cut.Find("button.lt-data-link").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("section.lt-data-entry"), Has.Count.EqualTo(1)));
        cut.Find("button.lt-data-link").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("button.lt-data-link").GetAttribute("aria-pressed"), Is.EqualTo("false"));
            Assert.That(cut.FindAll("section.lt-data-entry"), Is.Empty);
        });
    }

    [Test]
    public void A_long_preview_is_clipped_and_noted_as_a_preview()
    {
        Client.WithTree("orders");
        var value = new string('a', DataValueRendering.DisplayLimit + 500);
        Client.DeadLetters["orders"] = [Letter("order/1", value) with { PreviewTruncated = true, ValueByteLength = value.Length * 2 }];
        var cut = RenderAt("data/orders?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(1)));

        cut.Find("button.lt-data-link").Click();

        cut.WaitUntil(() =>
        {
            var inspection = cut.Find("section.lt-data-entry");
            Assert.That(inspection.QuerySelector(".lt-data-note")!.TextContent, Is.Not.Empty);
            var shown = inspection.QuerySelector("pre")!.TextContent;
            Assert.That(shown, Does.EndWith("\n..."));
            Assert.That(shown, Has.Length.LessThan(value.Length));
        });
    }

    private static async Task<T> Never<T>(CancellationToken token)
    {
        await Task.Delay(Timeout.Infinite, token);
        throw new InvalidOperationException("unreachable");
    }

    private static DeadLetterEntryRecord Letter(string key, string value, DeadLetterSourceKind source = DeadLetterSourceKind.LocalRejected) => new()
    {
        Key = key,
        ValuePreview = Encoding.UTF8.GetBytes(value),
        ValueByteLength = value.Length,
        Reason = "bad value",
        Source = source,
        TimestampUtc = new DateTimeOffset(2026, 9, 28, 12, 0, 0, TimeSpan.Zero),
    };
}
