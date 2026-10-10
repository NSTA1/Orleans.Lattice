using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>Late replies must not replace the dead letters or view status of the current workspace.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataPanelSupersededReadTests : DataTestContext
{
    [Test]
    public async Task Dead_letters_discard_a_count_from_the_previous_tree()
    {
        Client.WithTree("old").WithTree("current");
        Client.DeadLetters["current"] = [Letter("current-key")];
        var pending = new TaskCompletionSource<DeadLetterCountResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.DeadLetterCountReply = (request, _) => request.TreeId == "old"
            ? pending.Task
            : Task.FromResult(new DeadLetterCountResponse { TreeId = request.TreeId, Count = 1 });
        var cut = RenderAt("data/old?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain(nameof(FakeStateClient.GetDeadLetterCountAsync))));
        var panel = cut.FindComponent<DataDeadLettersPanel>().Instance;

        Navigation.NavigateTo("data/current?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-key")));
        Assert.That(cut.FindComponent<DataDeadLettersPanel>().Instance, Is.SameAs(panel));
        pending.SetResult(new DeadLetterCountResponse { TreeId = "old", Count = 99 });

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("99 dead letters", StringComparison.Ordinal) || Rows(cut).Count != 1,
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("1 dead letter"));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Dead_letters_discard_a_page_or_fault_from_the_previous_tree(bool fault)
    {
        Client.WithTree("old").WithTree("current");
        var pending = new TaskCompletionSource<DeadLetterQueuePage>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.DeadLetterPageReply = (request, _) => request.TreeId == "old"
            ? pending.Task
            : Task.FromResult(new DeadLetterQueuePage { Entries = [Letter("current-key")] });
        var cut = RenderAt("data/old?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain(nameof(FakeStateClient.ListDeadLettersAsync) + ":first")));
        var panel = cut.FindComponent<DataDeadLettersPanel>().Instance;

        Navigation.NavigateTo("data/current?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-key")));
        Assert.That(cut.FindComponent<DataDeadLettersPanel>().Instance, Is.SameAs(panel));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old tree failed"));
        }
        else
        {
            pending.SetResult(new DeadLetterQueuePage { Entries = [Letter("stale-key")], NextPageToken = "old-cursor" });
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("stale-key", StringComparison.Ordinal)
                || cut.Markup.Contains("The dead letters did not load", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-key"));
        Assert.That(cut.FindAll("button").Any(button => button.TextContent.Trim() == "Load more dead letters"), Is.False);
    }

    [Test]
    public async Task Dead_letters_keep_loading_the_current_tree_when_a_previous_page_finishes()
    {
        Client.WithTree("old").WithTree("current");
        var previous = new TaskCompletionSource<DeadLetterQueuePage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var current = new TaskCompletionSource<DeadLetterQueuePage>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.DeadLetterPageReply = (request, _) => request.TreeId == "old" ? previous.Task : current.Task;
        var cut = RenderAt("data/old?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Client.Calls.Count(call => call == nameof(FakeStateClient.ListDeadLettersAsync) + ":first"), Is.EqualTo(1)));

        Navigation.NavigateTo("data/current?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Client.Calls.Count(call => call == nameof(FakeStateClient.ListDeadLettersAsync) + ":first"), Is.EqualTo(2)));
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-skeleton").TextContent, Does.Contain("Loading the dead letters")));
        previous.SetResult(new DeadLetterQueuePage());

        Assert.That(await TestPoll.TryUntilAsync(
            () => !cut.Markup.Contains("Loading the dead letters", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        current.SetResult(new DeadLetterQueuePage { Entries = [Letter("current-key")] });
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-key")));
    }

    [Test]
    public async Task Dead_letters_discard_a_superseded_continuation_page()
    {
        Client.WithTree("old").WithTree("current");
        var pending = new TaskCompletionSource<DeadLetterQueuePage>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.DeadLetterPageReply = (request, _) => request.TreeId == "current"
            ? Task.FromResult(new DeadLetterQueuePage { Entries = [Letter("current-key")] })
            : request.PageToken is null
                ? Task.FromResult(new DeadLetterQueuePage { Entries = [Letter("first-key")], NextPageToken = "old-cursor" })
                : pending.Task;
        var cut = RenderAt("data/old?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("first-key")));
        var load = Button(cut, "Load more dead letters").ClickAsync();
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain(nameof(FakeStateClient.ListDeadLettersAsync) + ":old-cursor")));

        Navigation.NavigateTo("data/current?tab=dead-letters");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-key")));
        pending.SetResult(new DeadLetterQueuePage { Entries = [Letter("stale-key")] });
        await load;

        Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-key"));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Views_discard_a_status_or_fault_from_the_previous_workspace(bool fault)
    {
        Client.WithTree("orders");
        Client.Views.Add(new ViewStateSummary { ViewName = "by-status", SourceTreeId = "orders" });
        Client.Entries["view-by-status"] = new(StringComparer.Ordinal);
        var pending = new TaskCompletionSource<TreeViewStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reads = 0;
        Admin.GetViewStatusAsync("by-status", Arg.Any<CancellationToken>())
            .Returns(_ => ++reads == 1
                ? pending.Task
                : Task.FromResult(new TreeViewStatus { ViewName = "by-status", SourceTreeId = "orders", ApplyLag = 0 }));
        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() => Assert.That(reads, Is.EqualTo(1)));
        var panel = cut.FindComponent<DataViewsPanel>().Instance;

        Navigation.NavigateTo("data/view-by-status?tab=views");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("Current")));
        Assert.That(cut.FindComponent<DataViewsPanel>().Instance, Is.SameAs(panel));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old status failed"));
        }
        else
        {
            pending.SetResult(new TreeViewStatus { ViewName = "by-status", SourceTreeId = "orders", ApplyLag = 999 });
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => Rows(cut).Single().TextContent.Contains(fault ? "Status unknown" : "999 behind", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(Rows(cut).Single().TextContent, Does.Contain("Current"));
    }

    private static DeadLetterEntryRecord Letter(string key) => new()
    {
        Key = key,
        Reason = "invalid value",
        Source = DeadLetterSourceKind.LocalRejected,
    };
}
