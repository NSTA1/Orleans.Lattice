using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Core.Topology;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins.Data;
using Orleans.Lattice.Explorer.Plugins.DeadLetter;
using Orleans.Lattice.Explorer.Plugins.History;
using Orleans.Lattice.Explorer.Plugins.Metrics;
using Orleans.Lattice.Explorer.Plugins.TagIndex;
using Orleans.Lattice.Explorer.Plugins.Topology;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// What each per-selection surface says when it has nothing to show (issue
/// #1855).
/// </summary>
/// <remarks>
/// <para>
/// The defect these pin is a single sentence used for four different
/// situations. "No entries." reads identically whether the table is empty,
/// whether a scope filtered it, whether the caller holds no grant, or whether
/// the read never landed - and under deny-by-default that ambiguity is the most
/// common confusion an operator hits.
/// </para>
/// <para>
/// So every case below drives a real surface into one real state through its
/// own domain contract and asserts the copy names <em>that</em> situation. In
/// particular a failed read must never be worded as an empty result: telling an
/// operator looking for rejected writes that the queue is empty, when the
/// request never completed, is worse than saying nothing.
/// </para>
/// <para>
/// Every render is driven by the substituted surface, so nothing here waits on
/// a clock, a timer, a delay or a background task.
/// </para>
/// </remarks>
[TestFixture]
public sealed class SelectionSurfaceStateTests
{
    private const string Boom = "the endpoint is unreachable";

    [Test]
    public async Task The_tag_index_surface_reports_a_selection_that_is_not_an_index()
    {
        var surface = Substitute.For<ITagIndexSurface>();

        // A tree selection carries no index name, which is the shape a stale
        // link or a superseded selection leaves behind.
        var html = await SelectionViewRenderHarness.RenderAsync<TagIndexDetailTab, ITagIndexSurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Not a tag index"));
            Assert.That(html, Does.Contain("Choose a tag index from the catalog"),
                "an unresolvable selection must say how to get out of it");
        });
    }

    [Test]
    public async Task The_tag_index_surface_reports_a_failed_read_as_a_failure()
    {
        var surface = Substitute.For<ITagIndexSurface>();
        surface.ListCoveredTreesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException(Boom));

        var html = await SelectionViewRenderHarness.RenderAsync<TagIndexDetailTab, ITagIndexSurface>(
            surface,
            SelectionViewRenderHarness.TagIndex());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("is-failed"));
            Assert.That(html, Does.Contain(Boom), "the cluster's own words reach the reader");
            Assert.That(html, Does.Contain(ExplorerVocabulary.RetryAction));
        });
    }

    [Test]
    public async Task The_tag_index_surface_names_the_index_badge_in_words()
    {
        var surface = Substitute.For<ITagIndexSurface>();
        surface.ListCoveredTreesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(["orders"]));
        surface.ListTagsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(["eu-west"]));

        var html = await SelectionViewRenderHarness.RenderAsync<TagIndexDetailTab, ITagIndexSurface>(
            surface,
            SelectionViewRenderHarness.TagIndex());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Tag index"), "the badge reads as words, not as 'index'");
            Assert.That(html, Does.Not.Contain(">index<"));
        });
    }

    [Test]
    public async Task The_dead_letter_surface_reports_a_failed_read_as_a_failure_not_an_empty_queue()
    {
        var surface = Substitute.For<IDeadLetterSurface>();
        surface
            .ListAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException(Boom));

        var html = await SelectionViewRenderHarness.RenderAsync<DeadLetterTab, IDeadLetterSurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("is-failed"));
            Assert.That(html, Does.Contain(Boom));
            Assert.That(html, Does.Not.Contain("No dead-lettered items for this tree."),
                "a read that never landed is not an empty queue");
        });
    }

    [Test]
    public async Task The_dead_letter_surface_prompts_for_a_selection_and_names_the_queue_depth_in_words()
    {
        var surface = Substitute.For<IDeadLetterSurface>();
        surface.CountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(3));
        surface
            .ListAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DeadLetterPage
            {
                Entries = [Entry("orders/1")],
            }));

        var html = await SelectionViewRenderHarness.RenderAsync<DeadLetterTab, IDeadLetterSurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("3 dead-lettered"), "the badge reads as words, not '3 DLQ'");
            Assert.That(html, Does.Not.Contain("3 DLQ"));
            Assert.That(html, Does.Contain("Nothing selected"));
        });
    }

    [Test]
    public async Task The_metrics_surface_distinguishes_a_silent_cluster_from_a_failed_read()
    {
        var quiet = Substitute.For<IMetricsSurface>();
        quiet.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TreeMetrics?>(null));

        var broken = Substitute.For<IMetricsSurface>();
        broken.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException(Boom));

        var quietHtml = await SelectionViewRenderHarness.RenderAsync<MetricsTab, IMetricsSurface>(
            quiet, SelectionViewRenderHarness.Tree());
        var brokenHtml = await SelectionViewRenderHarness.RenderAsync<MetricsTab, IMetricsSurface>(
            broken, SelectionViewRenderHarness.Tree());

        Assert.Multiple(() =>
        {
            Assert.That(quietHtml, Does.Contain("No metrics for this selection"));
            Assert.That(quietHtml, Does.Contain("Nothing is being hidden from you"));
            Assert.That(quietHtml, Does.Not.Contain("is-failed"));

            Assert.That(brokenHtml, Does.Contain("is-failed"));
            Assert.That(brokenHtml, Does.Contain(Boom));
        });
    }

    [Test]
    public async Task The_topology_surface_distinguishes_a_silent_cluster_from_a_failed_read()
    {
        var quiet = Substitute.For<ITopologySurface>();
        quiet.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TopologyFetch { Roots = [] }));

        var broken = Substitute.For<ITopologySurface>();
        broken.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException(Boom));

        var quietHtml = await RenderTopologyAsync(quiet);
        var brokenHtml = await RenderTopologyAsync(broken);

        Assert.Multiple(() =>
        {
            Assert.That(quietHtml, Does.Contain("No structure for this selection"));
            Assert.That(quietHtml, Does.Contain("Nothing is being hidden from you"));
            Assert.That(quietHtml, Does.Not.Contain("is-failed"));

            Assert.That(brokenHtml, Does.Contain("is-failed"));
            Assert.That(brokenHtml, Does.Contain(Boom));
        });
    }

    [Test]
    public async Task The_history_surface_names_the_surface_to_visit_when_no_key_is_chosen()
    {
        var surface = Substitute.For<IHistorySurface>();
        surface.InspectedKey(Arg.Any<string>()).Returns((string?)null);

        var html = await SelectionViewRenderHarness.RenderAsync<HistoryTab, IHistorySurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("No key chosen"));
            Assert.That(html, Does.Contain("Open the Data surface"));
        });
    }

    [Test]
    public async Task The_history_surface_distinguishes_a_missing_table_from_a_key_with_no_revisions()
    {
        var missingTree = HistorySurfaceReturning(StateQueryStatus.TreeNotFound);
        var missingKey = HistorySurfaceReturning(StateQueryStatus.KeyNotFound);

        var missingTreeHtml = await SelectionViewRenderHarness.RenderAsync<HistoryTab, IHistorySurface>(
            missingTree, SelectionViewRenderHarness.Tree());
        var missingKeyHtml = await SelectionViewRenderHarness.RenderAsync<HistoryTab, IHistorySurface>(
            missingKey, SelectionViewRenderHarness.Tree());

        Assert.Multiple(() =>
        {
            Assert.That(missingTreeHtml, Does.Contain("Table not found"));
            Assert.That(missingKeyHtml, Does.Contain("No revisions for this key"));
            Assert.That(missingKeyHtml, Does.Contain("Nothing is being hidden from you"));
        });
    }

    [Test]
    public async Task The_entry_detail_distinguishes_nothing_chosen_from_a_key_that_has_gone()
    {
        var idle = await SelectionViewRenderHarness.RenderComponentAsync<DataEntryDetail>(
            new Dictionary<string, object?>());

        var gone = await SelectionViewRenderHarness.RenderComponentAsync<DataEntryDetail>(
            new Dictionary<string, object?> { ["SelectedKey"] = "orders/1" });

        Assert.Multiple(() =>
        {
            Assert.That(idle, Does.Contain("Nothing selected"));
            Assert.That(gone, Does.Contain("Key not found"));
            Assert.That(gone, Does.Contain("deleted, or expired, since the scan"));
        });
    }

    [Test]
    public async Task The_data_surface_reports_a_failed_scan_as_a_failure_not_an_empty_page()
    {
        var reader = Substitute.For<IDataReader>();
        reader
            .ScanAsync(
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<string?>(),
                Arg.Any<TagFilter?>(),
                Arg.Any<string?>(),
                Arg.Any<EntryScanMode>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException(Boom));

        var surface = Substitute.For<IDataSurface>();
        surface.CreatePager().Returns(_ => new DataPager(reader));
        surface.RetainedLoaded.Returns(true);
        surface.GetRetainedView(Arg.Any<string>())
            .Returns(new DataRetainedView(string.Empty, DataPaging.DefaultPageSize, EntryScanMode.Live, null));
        surface.ObserveConnection(Arg.Any<Action<LatticeConnectionState>>())
            .Returns(Substitute.For<IDisposable>());
        surface.ListTagIndexesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<TagIndexRef>>([]));

        var html = await SelectionViewRenderHarness.RenderAsync<DataTab, IDataSurface>(
            surface,
            SelectionViewRenderHarness.Tree());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("is-failed"));
            Assert.That(html, Does.Contain(Boom));
            Assert.That(html, Does.Not.Contain("No entries on this page."),
                "a scan that never landed is not an empty page");
        });
    }

    private static Task<string> RenderTopologyAsync(ITopologySurface surface) =>
        SelectionViewRenderHarness.RenderAsync<TopologyTab, ITopologySurface>(
            surface,
            SelectionViewRenderHarness.Tree(),
            configure: services => services.AddSingleton(Substitute.For<IJSRuntime>()));

    private static IHistorySurface HistorySurfaceReturning(StateQueryStatus status)
    {
        var surface = Substitute.For<IHistorySurface>();
        surface.InspectedKey(Arg.Any<string>()).Returns("orders/1");
        surface
            .LoadAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<string?>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new HistoryPage { Status = status, Revisions = [] }));

        // A live tail that ends at once rather than a null the follow loop would
        // fault on. Returning an already-finished stream keeps the surface's
        // teardown deterministic: nothing here races the renderer's disposal.
        surface
            .FollowAsync(Arg.Any<string>(), Arg.Any<HistoryLiveTail>(), Arg.Any<CancellationToken>())
            .Returns(_ => EmptyRows());

        return surface;
    }

#pragma warning disable CS1998 // an empty async stream needs no await
    private static async IAsyncEnumerable<HistoryRevisionRow> EmptyRows()
    {
        yield break;
    }
#pragma warning restore CS1998

    private static DeadLetterEntry Entry(string key) => new()
    {
        Key = key,
        Reason = "strict schema violation",
        Source = DeadLetterSource.LocalRejected,
        TimestampUtc = DateTimeOffset.UnixEpoch,
        Value = [],
    };
}
