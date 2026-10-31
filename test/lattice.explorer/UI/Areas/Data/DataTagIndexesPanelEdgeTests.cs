using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Operations;
using static Orleans.Lattice.Explorer.Tests.UI.Operations.TreeAdminOperationScript;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Tag indexes tab's edges: a missing workspace or reader, a status still
/// reconciling, failed covered-tree and tag reads, reads cancelled by leaving or
/// by a new selection, a selection made while the catalogue loads, and a
/// reconcile whose start or stop fails or whose stop is honoured.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataTagIndexesPanelEdgeTests : DataTestContext
{
    private const string NoReader = "This Explorer has no state API to read tag indexes through.";

    [Test]
    public void Without_a_workspace_the_panel_renders_nothing()
    {
        var cut = Render<DataTagIndexesPanel>();

        Assert.That(cut.Markup.Trim(), Is.Empty);
    }

    [Test]
    public void Without_a_reader_the_tab_says_there_is_no_state_api()
    {
        Client.WithTree("orders");
        Services.AddTransient<IDataReader>(_ => throw new InvalidOperationException("no state connection"));

        var cut = RenderAt("data/orders?tab=tag-indexes");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("The tag indexes did not load"));
            Assert.That(cut.Find(".lt-empty").TextContent, Does.Contain(NoReader));
            Assert.That(Button(cut, "Try again"), Is.Not.Null);
        });
    }

    [Test]
    public void Losing_the_reader_before_an_index_opens_leaves_its_details_unread()
    {
        var reader = SeedReader("by-region");
        var available = true;
        Services.AddTransient(_ => available ? reader : throw new InvalidOperationException("the connection went away"));
        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(1)));

        available = false;
        Navigation.NavigateTo("data/orders?tab=tag-indexes&index=by-region&tag=eu");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-entry h2").TextContent, Is.EqualTo("Tag index by-region"));
            Assert.That(cut.Find(".lt-data-entry").TextContent, Does.Contain("Keys tagged eu").And.Contain("No key carries this tag."));
        });
        Assert.That(cut.FindAll(".lt-data-list"), Is.Empty);
        Assert.That(reader.ReceivedCalls().Select(call => call.GetMethodInfo().Name), Has.None.EqualTo(nameof(IDataReader.ListCoveredTreesForIndexAsync))
            .And.None.EqualTo(nameof(IDataReader.ScanTagMembersAsync)));
    }

    [Test]
    public void An_index_whose_reconcile_is_not_idle_reads_as_reconciling()
    {
        SeedReader("by-region");
        Admin.GetTagIndexStatusAsync("by-region", Arg.Any<CancellationToken>())
            .Returns(new TreeTagIndexStatus { IndexName = "by-region", TreeId = "tag-by-region", CoveredTrees = ["orders"], ReconcileIdle = false });

        var cut = RenderAt("data/orders?tab=tag-indexes");

        cut.WaitUntil(() => Assert.That(Rows(cut).Single().TextContent, Does.Contain("Reconciling").And.Not.Contain("Idle")));
    }

    [Test]
    public void An_index_whose_reconcile_is_not_idle_reads_as_reconciling_in_the_compact_list()
    {
        SeedReader("by-region");
        Admin.GetTagIndexStatusAsync("by-region", Arg.Any<CancellationToken>())
            .Returns(new TreeTagIndexStatus { IndexName = "by-region", TreeId = "tag-by-region", CoveredTrees = ["orders"], ReconcileIdle = false });

        var cut = RenderAt("data/orders?tab=tag-indexes", compact: true);

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Reconciling").And.Contain("1 covered trees")));
    }

    [Test]
    public void Failed_covered_tree_and_tag_reads_show_empty_lists()
    {
        var reader = SeedReader("by-region");
        reader.ListCoveredTreesForIndexAsync("by-region", Arg.Any<CancellationToken>())
            .Returns(Task.FromException<IReadOnlyList<string>>(new InvalidOperationException("covered failed")));
        reader.ListTagsForIndexAsync("by-region", Arg.Any<CancellationToken>())
            .Returns(Task.FromException<IReadOnlyList<string>>(new InvalidOperationException("tags failed")));

        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-list").Children, Is.Empty);
            Assert.That(cut.Find(".lt-data-entry").TextContent, Does.Contain("This index holds no tags yet."));
        });
        Assert.That(cut.Markup, Does.Not.Contain("covered failed").And.Not.Contain("tags failed"));
    }

    [Test]
    public async Task Leaving_the_tab_mid_catalogue_read_cancels_it_quietly()
    {
        var reader = SeedReader("by-region");
        var observed = Observe(out var signal);
        reader.ListTagIndexesForTreeAsync("orders", Arg.Any<CancellationToken>())
            .Returns(call => Never<IReadOnlyList<TagIndexRef>>(signal(call.Arg<CancellationToken>())));
        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(reader.ReceivedCalls(), Is.Not.Empty));

        Navigation.NavigateTo("data/orders?tab=metrics");

        Assert.That(await observed.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
        cut.WaitUntil(() => Assert.That(cut.FindComponents<DataTagIndexesPanel>(), Is.Empty));
    }

    [Test]
    public async Task Leaving_the_tab_mid_status_read_cancels_it_quietly()
    {
        var reader = SeedReader("by-region");
        var observed = Observe(out var signal);
        Admin.GetTagIndexStatusAsync("by-region", Arg.Any<CancellationToken>())
            .Returns(call => Never<TreeTagIndexStatus>(signal(call.Arg<CancellationToken>())));
        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(Admin.ReceivedCalls().Select(call => call.GetMethodInfo().Name), Does.Contain(nameof(ILatticeTreeAdmin.GetTagIndexStatusAsync))));

        Navigation.NavigateTo("data/orders?tab=metrics");

        Assert.That(await observed.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
        cut.WaitUntil(() => Assert.That(cut.FindComponents<DataTagIndexesPanel>(), Is.Empty));
        Assert.That(reader.ReceivedCalls().Select(call => call.GetMethodInfo().Name), Has.None.EqualTo(nameof(IDataReader.ListCoveredTreesForIndexAsync)));
    }

    [Test]
    public void An_index_chosen_while_the_catalogue_loads_opens_once_it_arrives()
    {
        var reader = SeedReader("by-region");
        var pending = new TaskCompletionSource<IReadOnlyList<TagIndexRef>>(TaskCreationOptions.RunContinuationsAsynchronously);
        reader.ListTagIndexesForTreeAsync("orders", Arg.Any<CancellationToken>()).Returns(pending.Task);
        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(reader.ReceivedCalls(), Is.Not.Empty));

        Navigation.NavigateTo("data/orders?tab=tag-indexes&index=by-region");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Loading the tag indexes")));
        pending.SetResult([Index("by-region")]);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-entry h2").TextContent, Is.EqualTo("Tag index by-region"));
            Assert.That(cut.FindAll(".lt-data-tag").Select(tag => tag.TextContent), Is.EqualTo(new[] { "eu" }));
        });
        Assert.That(reader.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(IDataReader.ListTagIndexesForTreeAsync)), Is.EqualTo(1));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task A_detail_read_cancelled_by_choosing_another_index_gives_way_to_it(bool coveredPending)
    {
        var reader = SeedReader("first", "second");
        var observed = Observe(out var signal);
        if (coveredPending)
        {
            reader.ListCoveredTreesForIndexAsync("first", Arg.Any<CancellationToken>())
                .Returns(call => Never<IReadOnlyList<string>>(signal(call.Arg<CancellationToken>())));
        }
        else
        {
            reader.ListTagsForIndexAsync("first", Arg.Any<CancellationToken>())
                .Returns(call => Never<IReadOnlyList<string>>(signal(call.Arg<CancellationToken>())));
        }

        var cut = RenderAt("data/orders?tab=tag-indexes&index=first");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry h2").TextContent, Is.EqualTo("Tag index first")));

        Navigation.NavigateTo("data/orders?tab=tag-indexes&index=second");

        Assert.That(await observed.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-entry h2").TextContent, Is.EqualTo("Tag index second"));
            Assert.That(cut.FindAll(".lt-data-list a").Select(link => link.TextContent), Is.EqualTo(new[] { "orders" }));
            Assert.That(cut.FindAll(".lt-data-tag").Select(tag => tag.TextContent), Is.EqualTo(new[] { "eu" }));
        });
    }

    [Test]
    public async Task A_member_read_cancelled_by_choosing_another_tag_gives_way_to_it()
    {
        var reader = SeedReader("by-region");
        reader.ListTagsForIndexAsync("by-region", Arg.Any<CancellationToken>()).Returns(["eu", "us"]);
        var observed = Observe(out var signal);
        reader.ScanTagMembersAsync("by-region", "eu", Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => Never<TagMemberPage>(signal(call.Arg<CancellationToken>())));
        reader.ScanTagMembersAsync("by-region", "us", Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new TagMemberPage { Members = [new TagMemberRow { TreeId = "orders", Key = "us-key" }] });
        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region&tag=eu");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry").TextContent, Does.Contain("Keys tagged eu")));

        Navigation.NavigateTo("data/orders?tab=tag-indexes&index=by-region&tag=us");

        Assert.That(await observed.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-entry tbody th").TextContent, Does.Contain("us-key"));
            Assert.That(cut.FindAll("[role=alert]"), Is.Empty);
        });
    }

    [Test]
    public void A_reconcile_that_cannot_start_says_so_and_offers_it_again()
    {
        SeedReader("by-region");
        AdministeredTrees.Add("tag-by-region");
        Tracked.Operations.StartTagIndexReconcileAsync("by-region", Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new UnauthorizedAccessException("denied"));
        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Reconcile"), Is.EqualTo(1)));

        Button(cut, "Reconcile").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Reconcile this tag index?")));
        Confirm(cut, "by-region");

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Is.EqualTo(new[] { "You do not have permission to reconcile this tag index." }));
            Assert.That(Button(cut, "Reconcile").HasAttribute("disabled"), Is.False);
            Assert.That(cut.FindAll("[data-lt-data-operation]"), Is.Empty);
        });
    }

    [Test]
    public async Task Leaving_the_tab_while_a_reconcile_starts_drops_it_quietly()
    {
        SeedReader("by-region");
        AdministeredTrees.Add("tag-by-region");
        var observed = Observe(out var signal);
        Tracked.Operations.StartTagIndexReconcileAsync("by-region", Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(call => Never<LatticeOperationHandle>(signal(call.Arg<CancellationToken>())));
        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Reconcile"), Is.EqualTo(1)));
        Button(cut, "Reconcile").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Reconcile this tag index?")));
        Confirm(cut, "by-region");
        cut.WaitUntil(() => Assert.That(Button(cut, "Reconciling").HasAttribute("disabled"), Is.True));

        Navigation.NavigateTo("data/orders?tab=metrics");

        Assert.That(await observed.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
        cut.WaitUntil(() => Assert.That(cut.FindComponents<DataTagIndexesPanel>(), Is.Empty));
        Assert.That(Toasts, Is.Empty);
    }

    [Test]
    public void A_stop_that_fails_says_so_and_can_be_asked_again()
    {
        SeedReader("by-region");
        var operationId = RunningReconcile(
            Status(TreeAdminOperationKinds.TagIndexReconcile, LatticeOperationState.Running, TreeAdminOperationPhases.Repairing, 3, null, TreeAdminOperationUnits.Keys));
        Tracked.Operations.CancelOperationAsync(operationId, Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("cancel failed"));
        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-data-operation]").TextContent, Does.Contain("Reconciling by-region")));

        Button(cut, "Stop").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Is.EqualTo(new[] { "The Explorer could not stop this reconcile. Try again." }));
            Assert.That(Button(cut, "Stop").HasAttribute("disabled"), Is.False);
        });
    }

    [Test]
    public void A_stopped_reconcile_says_it_was_stopped()
    {
        SeedReader("by-region");
        var operationId = RunningReconcile(
            Status(TreeAdminOperationKinds.TagIndexReconcile, LatticeOperationState.Running, TreeAdminOperationPhases.Repairing, 3, null, TreeAdminOperationUnits.Keys),
            Status(TreeAdminOperationKinds.TagIndexReconcile, LatticeOperationState.Cancelled, TreeAdminOperationPhases.Repairing, 3, null, TreeAdminOperationUnits.Keys));
        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-data-operation]").TextContent, Does.Contain("Reconciling by-region")));

        Button(cut, "Stop").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Tracked.Cancelled, Is.EqualTo(new[] { operationId }));
            Assert.That(Toasts, Is.EqualTo(new[] { "Stopped the reconcile of by-region." }));
            Assert.That(cut.Find("[data-lt-data-operation]").TextContent, Does.Contain("Reconcile of by-region"));
        });
    }

    private IReadOnlyList<string> Toasts => Services.GetRequiredService<LtToastService>().Toasts.Select(toast => toast.Message).ToArray();

    private string RunningReconcile(params LatticeOperationStatus[] statuses)
    {
        var operationId = TreeAdminOperationIds.New(TreeAdminOperationKinds.TagIndexReconcile, "by-region");
        Tracked.Running(operationId, statuses);
        return operationId;
    }

    private IDataReader SeedReader(params string[] indexes)
    {
        Client.WithTree("orders");
        var reader = Substitute.For<IDataReader>();
        reader.ListTagIndexesForTreeAsync("orders", Arg.Any<CancellationToken>())
            .Returns(indexes.Select(Index).ToList());
        reader.ListCoveredTreesForIndexAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(["orders"]);
        reader.ListTagsForIndexAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(["eu"]);
        reader.ScanTagMembersAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new TagMemberPage { Members = [new TagMemberRow { TreeId = "orders", Key = "eu-key" }] });
        Services.AddSingleton(reader);
        Admin.GetTagIndexStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new TreeTagIndexStatus { IndexName = call.Arg<string>(), TreeId = "tag-" + call.Arg<string>(), CoveredTrees = ["orders"], ReconcileIdle = true });
        return reader;
    }

    private static Task<bool> Observe(out Func<CancellationToken, CancellationToken> signal)
    {
        var observed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        signal = token =>
        {
            token.Register(() => observed.TrySetResult(true));
            return token;
        };
        return observed.Task;
    }

    private static async Task<T> Never<T>(CancellationToken token)
    {
        await Task.Delay(Timeout.Infinite, token);
        throw new InvalidOperationException("unreachable");
    }

    private static TagIndexRef Index(string name) => new() { IndexName = name, TreeId = "tag-" + name };
}
