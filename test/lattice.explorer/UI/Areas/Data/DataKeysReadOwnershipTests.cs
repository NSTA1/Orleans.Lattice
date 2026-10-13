using System.Text;
using Bunit;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>Keys and entry details keep the current read and an available value renderer.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataKeysReadOwnershipTests : DataTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Entry_discards_a_reply_or_fault_for_a_previous_key(bool fault)
    {
        Client.WithTree("orders");
        var pending = new TaskCompletionSource<EntryGetResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.EntryReply = (request, _) => request.Key == "old"
            ? pending.Task
            : Task.FromResult(Entry(request.Key, "current value"));
        var cut = RenderAt("data/orders?key=old");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain("GetEntryAsync:old")));
        var panel = cut.FindComponent<DataEntryView>().Instance;

        Navigation.NavigateTo("data/orders?key=current");
        cut.WaitUntil(() => Assert.That(cut.Find("pre.lt-data-value").TextContent, Is.EqualTo("current value")));
        Assert.That(cut.FindComponent<DataEntryView>().Instance, Is.SameAs(panel));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old entry failed"));
        }
        else
        {
            pending.SetResult(Entry("old", "stale value"));
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains(fault ? "The entry did not load" : "stale value", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Find("pre.lt-data-value").TextContent, Is.EqualTo("current value"));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Keys_discard_a_scan_or_fault_for_a_previous_prefix(bool fault)
    {
        Client.WithTree("orders");
        var pending = new TaskCompletionSource<EntryScanResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.ScanReply = (request, _) => request.StartInclusive == "old/"
            ? pending.Task
            : Task.FromResult(Scan("current/key"));
        var cut = RenderAt("data/orders?prefix=old%2F");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain("ScanEntriesAsync:first")));
        var panel = cut.FindComponent<DataKeysPanel>().Instance;

        Navigation.NavigateTo("data/orders?prefix=current%2F");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current/key")));
        Assert.That(cut.FindComponent<DataKeysPanel>().Instance, Is.SameAs(panel));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old scan failed"));
        }
        else
        {
            pending.SetResult(Scan("stale/key") with { ContinuationToken = "old-cursor" });
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains(fault ? "The keys did not load" : "stale/key", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(Rows(cut).Single().TextContent, Does.Contain("current/key"));
        Assert.That(Button(cut, "Next page").HasAttribute("disabled"), Is.True);
    }

    [Test]
    public async Task Keys_release_a_superseded_scan_cursor_without_replacing_the_current_scan()
    {
        Client.WithTree("orders");
        var pending = new TaskCompletionSource<EntryScanResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.ScanReply = (request, _) => request.StartInclusive == "old/"
            ? pending.Task
            : Task.FromResult(Scan("current/key"));
        var cut = RenderAt("data/orders?prefix=old%2F");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain("ScanEntriesAsync:first")));

        Navigation.NavigateTo("data/orders?prefix=current%2F");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current/key")));
        pending.SetResult(Scan("stale/key") with { ContinuationToken = "old-cursor" });

        Assert.That(await TestPoll.TryUntilAsync(
            () => Client.Calls.Contains(nameof(FakeStateClient.CancelScanAsync)),
            TimeSpan.FromSeconds(1)), Is.True);
        Assert.That(Rows(cut).Single().TextContent, Does.Contain("current/key"));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Keys_discard_a_continuation_or_fault_after_a_new_scan_starts(bool fault)
    {
        Client.WithTree("orders");
        var pending = new TaskCompletionSource<EntryScanResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.ScanReply = (request, _) => request.StartInclusive == "current/"
            ? Task.FromResult(Scan("current/key"))
            : request.ContinuationToken is null
                ? Task.FromResult(Scan("old/key") with { ContinuationToken = "old-cursor" })
                : pending.Task;
        var cut = RenderAt("data/orders?prefix=old%2F");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("old/key")));
        var load = Button(cut, "Next page").ClickAsync();
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain("ScanEntriesAsync:old-cursor")));

        Navigation.NavigateTo("data/orders?prefix=current%2F");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current/key")));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old continuation failed"));
        }
        else
        {
            pending.SetResult(Scan("stale/key"));
        }

        await load;
        Assert.That(Rows(cut).Single().TextContent, Does.Contain("current/key"));
        Assert.That(cut.Find(".lt-data-pager__position").TextContent, Is.EqualTo("Page 1"));
    }

    [Test]
    public async Task Entry_keeps_the_new_selection_loading_when_the_old_reply_finishes()
    {
        Client.WithTree("orders");
        var previous = new TaskCompletionSource<EntryGetResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var current = new TaskCompletionSource<EntryGetResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.EntryReply = (request, _) => request.Key == "old" ? previous.Task : current.Task;
        var cut = RenderAt("data/orders?key=old");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain("GetEntryAsync:old")));

        Navigation.NavigateTo("data/orders?key=current");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain("GetEntryAsync:current")));
        previous.SetResult(Entry("old", "stale value"));

        Assert.That(await TestPoll.TryUntilAsync(
            () => !cut.Markup.Contains("Loading the entry", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        current.SetResult(Entry("current", "current value"));
        cut.WaitUntil(() => Assert.That(cut.Find("pre.lt-data-value").TextContent, Is.EqualTo("current value")));
    }

    [Test]
    public async Task Keys_keep_the_new_prefix_loading_when_the_old_reply_finishes()
    {
        Client.WithTree("orders");
        var previous = new TaskCompletionSource<EntryScanResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var current = new TaskCompletionSource<EntryScanResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.ScanReply = (request, _) => request.StartInclusive == "old/" ? previous.Task : current.Task;
        var cut = RenderAt("data/orders?prefix=old%2F");
        cut.WaitUntil(() => Assert.That(Client.Calls.Count(call => call == "ScanEntriesAsync:first"), Is.EqualTo(1)));

        Navigation.NavigateTo("data/orders?prefix=current%2F");
        cut.WaitUntil(() => Assert.That(Client.Calls.Count(call => call == "ScanEntriesAsync:first"), Is.EqualTo(2)));
        previous.SetResult(Scan("stale/key"));

        Assert.That(await TestPoll.TryUntilAsync(
            () => !cut.Markup.Contains("Loading keys", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        current.SetResult(Scan("current/key"));
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current/key")));
    }

    [Test]
    public void Keys_retry_pending_filter_reads_when_only_the_prefix_changes()
    {
        Client.WithTree("orders", keys: 1, prefix: "current/");
        Client.TagIndexes.Add(new TagIndexStateSummary { IndexName = "labels", TreeId = "index-labels" });
        Client.Covered["labels"] = ["orders"];
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.TreeTagIndexGate = pending;
        var cut = RenderAt("data/orders?prefix=old%2F");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain(nameof(FakeStateClient.ListTagIndexesAsync))));

        Navigation.NavigateTo("data/orders?prefix=current%2F");
        pending.SetResult();

        cut.WaitUntil(() =>
        {
            Assert.That(Control(cut, "Tag index").TextContent, Does.Contain("labels"));
            Assert.That(Rows(cut).Single().TextContent, Does.Contain("current/0000"));
        });
    }

    [Test]
    public void Entry_returns_to_automatic_when_live_updates_remove_the_member_renderer()
    {
        Client.WithTree("sets");
        Client.Entries["sets"]["tags"] = FakeStateClient.Entry("tags", "ignored") with
        {
            CrdtShape = "OrSet",
            CurrentMembers = [new CrdtMemberValue { Element = Encoding.UTF8.GetBytes("red") }],
        };
        var cut = RenderAt("data/sets?key=tags");
        cut.WaitUntil(() => Assert.That(cut.Find("pre.lt-data-value").TextContent, Does.Contain("red")));
        Client.Entries["sets"]["tags"] = FakeStateClient.Entry("tags", "empty set") with { CrdtShape = "OrSet" };
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));

        Client.Feeds[^1].Writer.TryWrite(FakeStateClient.Change("sets", "tags"));

        cut.WaitUntil(() => Assert.That(cut.Find("pre.lt-data-value").TextContent, Is.EqualTo("empty set")));
        var select = cut.FindComponent<DataEntryView>().FindComponent<LtSelect>().Instance;
        Assert.That(select.Value, Is.EqualTo(nameof(DataValueRenderer.Auto)));
        Assert.That(select.Options.Select(option => option.Value), Does.Contain(select.Value));
    }

    [TestCase("Text")]
    [TestCase("Hex")]
    public void Entry_preserves_an_available_renderer_when_live_updates_remove_members(string renderer)
    {
        Client.WithTree("sets");
        Client.Entries["sets"]["tags"] = FakeStateClient.Entry("tags", "value") with
        {
            CrdtShape = "OrSet",
            CurrentMembers = [new CrdtMemberValue { Element = Encoding.UTF8.GetBytes("red") }],
        };
        var cut = RenderAt("data/sets?key=tags");
        cut.WaitUntil(() => Assert.That(cut.FindAll("pre.lt-data-value"), Has.Count.EqualTo(1)));
        Control(cut, "Show value as").Change(renderer);
        Client.Entries["sets"]["tags"] = FakeStateClient.Entry("tags", "empty set");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));

        Client.Feeds[^1].Writer.TryWrite(FakeStateClient.Change("sets", "tags"));

        cut.WaitUntil(() => Assert.That(cut.Find("pre.lt-data-value").TextContent,
            renderer == "Text" ? Is.EqualTo("empty set") : Does.Contain("65 6d 70 74 79")));
        Assert.That(cut.FindComponent<DataEntryView>().FindComponent<LtSelect>().Instance.Value, Is.EqualTo(renderer));
    }

    private static EntryGetResponse Entry(string key, string value) => new()
    {
        TreeId = "orders",
        Key = key,
        Status = StateQueryStatus.Found,
        Entry = FakeStateClient.Entry(key, value),
    };

    private static EntryScanResponse Scan(string key) => new()
    {
        TreeId = "orders",
        Entries = [FakeStateClient.Entry(key, "value")],
    };
}
