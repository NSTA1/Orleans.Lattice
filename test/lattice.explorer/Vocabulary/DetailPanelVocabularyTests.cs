using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.UI.Detail;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Vocabulary;

/// <summary>
/// Whether the one shared surface this issue owns actually spends the glossary:
/// the detail panel's prompt, its two "no surface" states, and the explanation of
/// what is selected.
/// </summary>
/// <remarks>
/// <para>
/// The acceptance criterion being defended is "no user-facing explanation depends
/// on a bare <c>title</c> attribute". The panel used to carry the selected item's
/// underlying id in <c>title</c>, which a touch caller never sees and a keyboard
/// caller cannot reach; it now renders the help disclosure instead.
/// </para>
/// <para>
/// Assertions read the parsed DOM rather than raw markup, per the rule in
/// <c>LatticeComponentTestContext</c>. These are pure unit tests - every service
/// is a stub and no host, cluster or channel is stood up - so this fixture
/// carries no slow category.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DetailPanelVocabularyTests : BunitContext
{
    private static readonly CatalogItem Tree = new() { Id = "orders", Kind = CatalogKind.Trees };

    private static readonly CatalogItem View = new()
    {
        Id = "view-totals",
        DisplayName = "totals",
        Kind = CatalogKind.Views,
    };

    private readonly ExplorerPluginAccessStore _access = new();
    private readonly SettableSelection _selection = new();

    private void Configure(params IExplorerPlugin[] plugins)
    {
        Services.AddSingleton<IExplorerPluginCatalog>(new ExplorerPluginCatalog(plugins));
        Services.AddSingleton<IExplorerPluginAccessStore>(_access);
        Services.AddSingleton<IExplorerSelection>(_selection);
        Services.AddSingleton<IUiPreferenceStore>(new FakeUiPreferenceStore());
        Services.AddExplorerNavigation();

        JSInterop.Mode = JSRuntimeMode.Loose;
    }

    private static IExplorerPlugin Plugin(string id, string label) =>
        new FakeExplorerPlugin(
            id,
            ExplorerPluginSurface.Selection,
            order: 0,
            label,
            ExplorerPluginAccessGates.Allowed,
            domainContract: null,
            typeof(ProbeSelectionView));

    // ------------------------------------------------------------ the prompt

    [Test]
    public void With_nothing_selected_the_panel_names_all_three_catalog_kinds()
    {
        Configure(Plugin("a", "Alpha"));

        var text = Render<DetailPanel>().Find(".lx-shell-detail-empty").TextContent;

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain(ExplorerVocabulary.NoSelectionHeadline));
            Assert.That(text, Does.Contain(ExplorerVocabulary.NoSelectionExplanation));
            Assert.That(text, Does.Contain("tag index"), "the old prompt named only trees and views");
        });
    }

    // ----------------------------------------------------- the two empty states

    [Test]
    public void With_no_surface_registered_the_panel_says_so_and_what_to_do()
    {
        Configure();
        var panel = Render<DetailPanel>();

        panel.InvokeAsync(() => _selection.Select(Tree));

        var text = panel.Find(".lx-shell-detail-empty").TextContent;

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("No surface for this selection"));
            Assert.That(text, Does.Contain(ExplorerVocabulary.RemedyLabel), "an empty state says what to do next");
        });
    }

    [Test]
    public void With_every_surface_denied_the_panel_distinguishes_a_refusal_from_an_absence()
    {
        Configure(Plugin("a", "Alpha"));
        var expected = ExplorerStateCopy.NotPermitted(ExplorerSubjects.DetailSurfaces);
        var panel = Render<DetailPanel>();

        panel.InvokeAsync(() =>
        {
            _access.Set("a", ExplorerPluginAccess.Denied);
            _selection.Select(Tree);
        });

        var text = panel.Find(".lx-shell-detail-empty").TextContent;

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain(expected.Headline));
            Assert.That(text, Does.Contain(expected.Remedy!), "a refusal always states its remedy");
            Assert.That(
                text,
                Does.Not.Contain("No surface for this selection"),
                "a refusal must not read as an absence");
        });
    }

    // ------------------------------------------------- explaining the selection

    [Test]
    public void The_selection_title_carries_no_bare_title_attribute()
    {
        Configure(Plugin("a", "Alpha"));
        var panel = Render<DetailPanel>();

        panel.InvokeAsync(() => _selection.Select(View));

        var title = panel.Find(".lx-shell-detail-title");

        Assert.Multiple(() =>
        {
            Assert.That(title.HasAttribute("title"), Is.False,
                "a title attribute is invisible on touch and unreachable by keyboard");
            Assert.That(title.TextContent, Is.EqualTo("totals"));
        });
    }

    [Test]
    public void The_selection_is_explained_through_a_focusable_disclosure()
    {
        Configure(Plugin("a", "Alpha"));
        var panel = Render<DetailPanel>();

        panel.InvokeAsync(() => _selection.Select(Tree));

        var trigger = panel.Find("button.lx-help-trigger");

        Assert.Multiple(() =>
        {
            Assert.That(trigger.GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(trigger.GetAttribute("aria-controls"), Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public void The_explanation_is_the_glossary_definition_of_the_selected_kind()
    {
        Configure(Plugin("a", "Alpha"));
        var panel = Render<DetailPanel>();

        panel.InvokeAsync(() => _selection.Select(Tree));

        var explanation = panel.Find(".lx-help-panel").TextContent;

        Assert.That(explanation, Does.Contain(ExplorerGlossary.Get(ExplorerTermIds.Trees).Explanation));
    }

    [Test]
    public void The_title_is_described_by_the_explanation_even_while_it_is_collapsed()
    {
        Configure(Plugin("a", "Alpha"));
        var panel = Render<DetailPanel>();

        panel.InvokeAsync(() => _selection.Select(Tree));

        var title = panel.Find(".lx-shell-detail-title");
        var explanation = panel.Find(".lx-help-panel");

        Assert.Multiple(() =>
        {
            Assert.That(title.GetAttribute("aria-describedby"), Is.EqualTo(explanation.Id));
            Assert.That(explanation.HasAttribute("hidden"), Is.True,
                "the panel stays in the DOM while collapsed so the description still holds");
        });
    }

    [Test]
    public void A_view_whose_label_hides_its_id_has_that_id_explained_rather_than_hidden()
    {
        Configure(Plugin("a", "Alpha"));
        var panel = Render<DetailPanel>();

        panel.InvokeAsync(() => _selection.Select(View));

        var explanation = panel.Find(".lx-help-panel").TextContent;

        Assert.Multiple(() =>
        {
            Assert.That(explanation, Does.Contain("view-totals"));
            Assert.That(explanation, Does.Contain(ExplorerGlossary.Get(ExplorerTermIds.Views).Explanation));
        });
    }

    [Test]
    public void A_tree_whose_label_is_its_id_is_not_told_its_own_id_twice()
    {
        Configure(Plugin("a", "Alpha"));
        var panel = Render<DetailPanel>();

        panel.InvokeAsync(() => _selection.Select(Tree));

        Assert.That(
            panel.Find(".lx-help-panel").TextContent,
            Does.Not.Contain("underlying id"));
    }

    /// <summary>
    /// A settable selection, so a test drives the transition rather than
    /// depending on a real catalog load.
    /// </summary>
    private sealed class SettableSelection : IExplorerSelection
    {
        public CatalogItem? Selected { get; private set; }

        public event Action? SelectionChanged;

        public void Select(CatalogItem? item)
        {
            Selected = item;
            SelectionChanged?.Invoke();
        }
    }
}
