using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>Tag-index reads and reconcile submissions belong to the workspace that started them.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataTagIndexOwnershipTests : DataTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Catalogue_discards_a_reply_or_fault_from_the_previous_tree(bool fault)
    {
        var reader = SeedIndexes();
        var pending = new TaskCompletionSource<IReadOnlyList<TagIndexRef>>(TaskCreationOptions.RunContinuationsAsynchronously);
        reader.ListTagIndexesForTreeAsync("orders", Arg.Any<CancellationToken>()).Returns(pending.Task);
        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(reader.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(IDataReader.ListTagIndexesForTreeAsync)), Is.EqualTo(1)));
        var panel = cut.FindComponent<DataTagIndexesPanel>().Instance;

        Navigation.NavigateTo("data/customers?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-index")));
        Assert.That(cut.FindComponent<DataTagIndexesPanel>().Instance, Is.SameAs(panel));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old catalogue failed"));
        }
        else
        {
            pending.SetResult([Index("old-index")]);
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.FindAll(".lt-empty h2").Any(title => title.TextContent == "The tag indexes did not load")
                || Rows(cut).Any(row => row.TextContent.Contains("old-index", StringComparison.Ordinal)),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-index"));
    }

    [Test]
    public async Task Catalogue_retry_loads_the_selection_after_the_initial_failure()
    {
        var reader = SeedIndexes();
        reader.ListTagIndexesForTreeAsync("orders", Arg.Any<CancellationToken>())
            .Returns(Task.FromException<IReadOnlyList<TagIndexRef>>(new InvalidOperationException("catalogue unavailable")));
        var cut = RenderAt("data/orders?tab=tag-indexes&index=old-index&tag=current-tag");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("The tag indexes did not load")));
        reader.ListTagIndexesForTreeAsync("orders", Arg.Any<CancellationToken>()).Returns([Index("old-index")]);

        await Button(cut, "Try again").ClickAsync();

        Assert.That(cut.Find(".lt-data-list a").TextContent, Is.EqualTo("orders"));
        Assert.That(cut.Find(".lt-data-tag").TextContent, Is.EqualTo("old-tag"));
        Assert.That(cut.Find(".lt-data-entry tbody th").TextContent, Does.Contain("current-key"));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Catalogue_discards_a_status_or_fault_for_a_previous_tree(bool fault)
    {
        SeedIndexes();
        var pending = new TaskCompletionSource<TreeTagIndexStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        Admin.GetTagIndexStatusAsync("old-index", Arg.Any<CancellationToken>()).Returns(pending.Task);
        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.GetTagIndexStatusAsync)), Is.EqualTo(1)));
        Admin.ClearReceivedCalls();

        Navigation.NavigateTo("data/customers?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("current-index").And.Contain("Idle")));
        var currentReads = Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.GetTagIndexStatusAsync));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old status failed"));
        }
        else
        {
            pending.SetResult(new TreeTagIndexStatus { IndexName = "old-index", TreeId = "tag-old-index" });
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.GetTagIndexStatusAsync)) != currentReads,
            TimeSpan.FromSeconds(1)), Is.False, "An old catalogue must stop reading its statuses after navigation.");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Selected_index_discards_superseded_covered_trees_and_tags(bool fault)
    {
        var reader = SeedIndexes();
        var pending = new TaskCompletionSource<IReadOnlyList<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        reader.ListCoveredTreesForIndexAsync("old-index", Arg.Any<CancellationToken>()).Returns(pending.Task);
        var cut = RenderAt("data/orders?tab=tag-indexes&index=old-index");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-skeleton").TextContent, Does.Contain("Loading the covered trees")));

        Navigation.NavigateTo("data/orders?tab=tag-indexes&index=current-index");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-list a").TextContent, Is.EqualTo("customers")));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old coverage failed"));
        }
        else
        {
            pending.SetResult(["orders"]);
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.FindAll(".lt-data-list a").SingleOrDefault()?.TextContent != "customers"
                || cut.FindAll(".lt-data-tag").Any(tag => tag.TextContent == "old-tag"),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Find(".lt-data-tag").TextContent, Is.EqualTo("current-tag"));
        Assert.That(reader.ReceivedCalls().Count(call =>
            call.GetMethodInfo().Name == nameof(IDataReader.ListTagsForIndexAsync)
            && (string)call.GetArguments()[0]! == "old-index"), Is.Zero, "A superseded read must not start another request.");
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task Members_discard_a_page_or_fault_for_the_previous_tag(bool continuation, bool fault)
    {
        var reader = SeedIndexes();
        var pending = new TaskCompletionSource<TagMemberPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        reader.ScanTagMembersAsync("old-index", "old-tag", Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => continuation && call.ArgAt<string?>(3) is null
                ? Task.FromResult(Members("first-key") with { ContinuationToken = "old-cursor" })
                : pending.Task);
        var cut = RenderAt("data/orders?tab=tag-indexes&index=old-index&tag=old-tag");
        Task? load = null;
        if (continuation)
        {
            cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-entry tbody th").Single().TextContent, Does.Contain("first-key")));
            load = Button(cut, "Load more keys").ClickAsync();
        }

        cut.WaitUntil(() => Assert.That(reader.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(IDataReader.ScanTagMembersAsync)), Is.EqualTo(continuation ? 2 : 1)));
        Navigation.NavigateTo("data/orders?tab=tag-indexes&index=old-index&tag=current-tag");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-entry tbody th").Single().TextContent, Does.Contain("current-key")));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old members failed"));
        }
        else
        {
            pending.SetResult(Members("stale-key") with { ContinuationToken = "stale-cursor" });
        }

        if (load is not null)
        {
            await load;
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.FindAll(".lt-data-entry tbody th").Count != 1
                || cut.FindAll(".lt-data-entry [role=alert]").Count != 0
                || cut.FindAll("button").Any(button => button.TextContent.Trim() == "Load more keys"),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Find(".lt-data-entry tbody th").TextContent, Does.Contain("current-key"));
    }

    [Test]
    public void Reconcile_confirmation_is_closed_when_the_selected_index_changes()
    {
        SeedIndexes();
        AdministeredTrees.UnionWith(["tag-old-index", "tag-current-index"]);
        var cut = RenderAt("data/orders?tab=tag-indexes&index=old-index");
        cut.WaitUntil(() => Assert.That(Button(cut, "Reconcile").HasAttribute("disabled"), Is.False));
        Button(cut, "Reconcile").Click();
        cut.Find(".lt-confirm input").Input("old-index");

        Navigation.NavigateTo("data/orders?tab=tag-indexes&index=current-index");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry h2").TextContent, Does.Contain("current-index")));
        Assert.That(cut.FindAll(".lt-confirm"), Is.Empty, "A confirmation must not silently retarget to another index.");
        Assert.That(Tracked.Started, Is.Empty);
    }

    [Test]
    public async Task Members_retry_clears_the_failed_page_error()
    {
        var reader = SeedIndexes();
        reader.ScanTagMembersAsync("old-index", "old-tag", Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => call.ArgAt<string?>(3) is null
                ? Task.FromResult(Members("first-key") with { ContinuationToken = "next" })
                : Task.FromException<TagMemberPage>(new InvalidOperationException("page unavailable")));
        var cut = RenderAt("data/orders?tab=tag-indexes&index=old-index&tag=old-tag");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry tbody th").TextContent, Does.Contain("first-key")));
        await Button(cut, "Load more keys").ClickAsync();
        Assert.That(cut.FindAll(".lt-data-entry [role=alert]"), Has.Count.EqualTo(1));
        reader.ScanTagMembersAsync("old-index", "old-tag", Arg.Any<int>(), "next", Arg.Any<CancellationToken>())
            .Returns(Members("next-key"));

        await Button(cut, "Load more keys").ClickAsync();

        Assert.That(cut.FindAll(".lt-data-entry [role=alert]"), Is.Empty);
        Assert.That(cut.FindAll(".lt-data-entry tbody th").Select(cell => cell.TextContent), Is.EqualTo(new[] { "first-key", "next-key" }));
    }

    [Test]
    public async Task Reconcile_failure_from_a_previous_tree_does_not_notify_the_current_workspace()
    {
        SeedIndexes();
        AdministeredTrees.UnionWith(["tag-old-index", "tag-current-index"]);
        var pending = new TaskCompletionSource<LatticeOperationHandle>(TaskCreationOptions.RunContinuationsAsynchronously);
        Tracked.Operations.StartTagIndexReconcileAsync("old-index", Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(pending.Task);
        var cut = RenderAt("data/orders?tab=tag-indexes&index=old-index");
        cut.WaitUntil(() => Assert.That(Button(cut, "Reconcile").HasAttribute("disabled"), Is.False));
        Button(cut, "Reconcile").Click();
        cut.Find(".lt-confirm input").Input("old-index");
        var submission = cut.Find(".lt-confirm").SubmitAsync();
        cut.WaitUntil(() => Assert.That(Button(cut, "Reconciling").HasAttribute("disabled"), Is.True));

        Navigation.NavigateTo("data/customers?tab=tag-indexes&index=current-index");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry h2").TextContent, Does.Contain("current-index")));
        pending.SetException(new InvalidOperationException("old reconcile failed"));
        await submission;

        Assert.That(Services.GetRequiredService<LtToastService>().Toasts, Is.Empty);
        Assert.That(Button(cut, "Reconcile").HasAttribute("disabled"), Is.False);
    }

    private IDataReader SeedIndexes()
    {
        Client.WithTree("orders").WithTree("customers");
        Client.TagIndexes.AddRange([
            new TagIndexStateSummary { IndexName = "old-index", TreeId = "tag-old-index" },
            new TagIndexStateSummary { IndexName = "current-index", TreeId = "tag-current-index" },
        ]);
        var reader = Substitute.For<IDataReader>();
        reader.ListTagIndexesForTreeAsync("orders", Arg.Any<CancellationToken>())
            .Returns([Index("old-index"), Index("current-index")]);
        reader.ListTagIndexesForTreeAsync("customers", Arg.Any<CancellationToken>())
            .Returns([Index("current-index")]);
        reader.ListCoveredTreesForIndexAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new[] { call.Arg<string>() == "old-index" ? "orders" : "customers" });
        reader.ListTagsForIndexAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new[] { call.Arg<string>() == "old-index" ? "old-tag" : "current-tag" });
        reader.ScanTagMembersAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(Members("current-key"));
        Services.AddSingleton(reader);
        Admin.GetTagIndexStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new TreeTagIndexStatus { IndexName = call.Arg<string>(), TreeId = "tag-" + call.Arg<string>(), ReconcileIdle = true });
        return reader;
    }

    private static TagIndexRef Index(string name) => new() { IndexName = name, TreeId = "tag-" + name };

    private static TagMemberPage Members(string key) => new()
    {
        Members = [new TagMemberRow { TreeId = "orders", Key = key }],
    };
}
