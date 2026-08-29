using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Plugins.Data;
using Orleans.Lattice.Explorer.Plugins.History;
using Orleans.Lattice.Explorer.Plugins.Selection;
using Orleans.Lattice.Explorer.Plugins.TagIndex;
using Orleans.Lattice.Explorer.Tests.Detail;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The three hand-offs between the per-selection surfaces, now that each ships
/// in its own package and neither side can see the other's source.
/// <para>
/// Every hand-off works by one surface writing retained state that another reads
/// back, so the two spellings of each key are the whole contract - and the only
/// thing that can now drift silently. These drive the real adapters against the
/// real stores and assert the round trip, rather than restating either
/// spelling.
/// </para>
/// </summary>
[TestFixture]
public sealed class SelectionSurfaceHandOffTests
{
    private const string MembershipTree = "tag-region";
    private const string IndexName = "region";
    private const string CoveredTree = "orders";

    [Test]
    public async Task Exploring_a_tag_index_from_the_data_surface_preselects_the_tag_it_opens_on()
    {
        // The seeded tag is a one-shot: the value drill-down surface writes it and
        // the tag-index browser takes it. Neither package can see the other's key,
        // so the round trip is the contract.
        var context = new HandOffContext();

        await context.Data.ExploreTagIndexAsync(new TagIndexRef { IndexName = IndexName, TreeId = MembershipTree }, "emea");

        Assert.Multiple(() =>
        {
            Assert.That(context.Selection.Selected!.Id, Is.EqualTo(MembershipTree));
            Assert.That(context.Selection.Selected.Kind, Is.EqualTo(CatalogKind.TagIndexes));
            Assert.That(context.Selection.Selected.IndexName, Is.EqualTo(IndexName));
        });

        Assert.That(await context.TagIndex.TakeSeededTagAsync(MembershipTree), Is.EqualTo("emea"));
    }

    [Test]
    public async Task A_seeded_tag_is_taken_once_so_a_later_refresh_starts_clean()
    {
        var context = new HandOffContext();

        await context.Data.ExploreTagIndexAsync(new TagIndexRef { IndexName = IndexName, TreeId = MembershipTree }, "emea");

        Assert.Multiple(async () =>
        {
            Assert.That(await context.TagIndex.TakeSeededTagAsync(MembershipTree), Is.EqualTo("emea"));
            Assert.That(await context.TagIndex.TakeSeededTagAsync(MembershipTree), Is.Null);
        });
    }

    [Test]
    public async Task Exploring_a_tag_index_with_no_tag_seeds_nothing()
    {
        var context = new HandOffContext();

        await context.Data.ExploreTagIndexAsync(new TagIndexRef { IndexName = IndexName, TreeId = MembershipTree }, tag: null);

        Assert.That(await context.TagIndex.TakeSeededTagAsync(MembershipTree), Is.Null);
    }

    [Test]
    public async Task Opening_a_covered_tree_selects_it_and_reopens_the_panel_on_the_data_surface()
    {
        // The panel re-applies the retained surface on a selection change, so
        // seeding it is what makes the navigation land on Data rather than on
        // whichever surface happened to be active.
        var context = new HandOffContext();

        await context.TagIndex.GoToTreeAsync(CoveredTree);

        Assert.Multiple(() =>
        {
            Assert.That(context.Selection.Selected!.Id, Is.EqualTo(CoveredTree));
            Assert.That(context.Selection.Selected.Kind, Is.EqualTo(CatalogKind.Trees));
            Assert.That(
                context.Preferences.GetOrDefault<string?>(SelectionPluginKeys.ActivePluginPreferenceKey, null),
                Is.EqualTo(SelectionPluginKeys.Data));
        });
    }

    [Test]
    public async Task Opening_a_member_lands_the_data_surface_on_that_key_with_the_tag_filter_applied()
    {
        var context = new HandOffContext();

        await context.TagIndex.GoToMemberAsync(new TagMemberRow { TreeId = CoveredTree, Key = "order-42" }, IndexName, "emea");

        Assert.Multiple(() =>
        {
            Assert.That(context.Selection.Selected!.Id, Is.EqualTo(CoveredTree));
            Assert.That(context.Selection.Selected.Kind, Is.EqualTo(CatalogKind.Trees));
            Assert.That(
                context.Preferences.GetOrDefault<string?>(SelectionPluginKeys.ActivePluginPreferenceKey, null),
                Is.EqualTo(SelectionPluginKeys.Data));

            // The filter and the inspected key are read back through the value
            // drill-down surface's own contract, so the two key schemes are pinned
            // to each other rather than to a literal in this test.
            var retained = context.Data.GetRetainedView(CoveredTree);
            Assert.That(retained.TagIndexName, Is.EqualTo(IndexName));
            Assert.That(context.Data.GetRetainedTagValue(CoveredTree, IndexName), Is.EqualTo("emea"));
            Assert.That(context.Data.GetInspectedKey(CoveredTree), Is.EqualTo("order-42"));
        });
    }

    [Test]
    public void The_inspected_key_the_data_surface_publishes_is_the_key_the_timeline_opens_on()
    {
        // The History button hands off no payload: both surfaces read one
        // inspected key per tree, which is what lets the timeline render inline
        // for the row the operator drilled into.
        var context = new HandOffContext();

        context.Data.SetInspectedKey(CoveredTree, "order-42");

        Assert.That(context.History.InspectedKey(CoveredTree), Is.EqualTo("order-42"));
    }

    [Test]
    public void An_unvisited_tree_has_no_inspected_key_on_either_surface()
    {
        var context = new HandOffContext();

        Assert.Multiple(() =>
        {
            Assert.That(context.Data.GetInspectedKey(CoveredTree), Is.Null);
            Assert.That(context.History.InspectedKey(CoveredTree), Is.Null);
        });
    }

    [Test]
    public void The_data_surface_offers_a_history_button_only_when_the_timeline_package_is_registered()
    {
        var without = new HandOffContext(registerHistoryView: false);
        var with = new HandOffContext();

        Assert.Multiple(() =>
        {
            Assert.That(without.Data.EntryHistoryViewType, Is.Null);
            Assert.That(with.Data.EntryHistoryViewType, Is.EqualTo(new EntryHistoryNestedSurface().ViewType));
        });
    }

    [Test]
    public void The_session_page_is_cleared_rather_than_stored_as_a_zero()
    {
        var context = new HandOffContext();

        context.Data.SetSessionPage(CoveredTree, 3);
        Assert.That(context.Data.GetSessionPage(CoveredTree), Is.EqualTo(3));

        context.Data.SetSessionPage(CoveredTree, 0);
        Assert.That(context.Data.GetSessionPage(CoveredTree), Is.Zero);
    }

    /// <summary>
    /// The two adapters under test, wired to one shared preference store, one
    /// shared session store and one shared selection service - which is exactly
    /// how a Blazor circuit wires them.
    /// </summary>
    private sealed class HandOffContext
    {
        public HandOffContext(bool registerHistoryView = true)
        {
            Preferences = new FakeUiPreferenceStore();
            Session = new UiSessionStore();
            Selection = new ExplorerSelection();

            var reader = Substitute.For<IDataReader>();
            var nested = new SelectionNestedSurfaceRegistry(
                registerHistoryView ? [new EntryHistoryNestedSurface()] : []);

            Data = new DataSurface(
                reader,
                Preferences,
                Session,
                Selection,
                nested,
                new ServiceCollection().BuildServiceProvider());

            TagIndex = new TagIndexSurface(reader, Preferences, Session, Selection);

            History = new HistorySurface(
                Substitute.For<IHistoryReader>(),
                Substitute.For<IHistoryLiveFollower>(),
                Session,
                Substitute.For<ILatticeStateConnection>());
        }

        public FakeUiPreferenceStore Preferences { get; }

        public UiSessionStore Session { get; }

        public ExplorerSelection Selection { get; }

        public IDataSurface Data { get; }

        public ITagIndexSurface TagIndex { get; }

        public IHistorySurface History { get; }
    }
}
