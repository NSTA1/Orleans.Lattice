using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Detail;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Detail;

/// <summary>
/// The base every per-selection plugin view derives from. It carries the one
/// thing the plugin contract cannot express through <c>DynamicComponent</c>
/// parameters - a cancellation token tied to the view's lifetime - so that
/// in-flight loads for a superseded selection are abandoned when the host
/// re-mounts.
/// </summary>
[TestFixture]
public sealed class SelectionPluginViewBaseTests
{
    [Test]
    public void The_token_is_live_while_the_view_is()
    {
        using var view = new StubView();

        Assert.That(view.Token.IsCancellationRequested, Is.False);
    }

    [Test]
    public void Disposing_the_view_cancels_its_token()
    {
        var view = new StubView();
        var token = view.Token;

        view.Dispose();

        Assert.That(
            token.IsCancellationRequested,
            Is.True,
            "a superseded selection's loads must be abandoned, not left running");
    }

    [Test]
    public void Disposing_twice_is_a_no_op()
    {
        var view = new StubView();
        var token = view.Token;

        view.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(view.Dispose, Throws.Nothing);
            Assert.That(token.IsCancellationRequested, Is.True);
        });
    }

    [Test]
    public void An_override_runs_alongside_the_base_cancellation()
    {
        var view = new CountingStubView();
        var token = view.Token;

        view.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(view.Disposals, Is.EqualTo(1));
            Assert.That(token.IsCancellationRequested, Is.True);
        });
    }

    [Test]
    public void The_selection_parameter_is_the_catalog_item_the_host_hands_over()
    {
        var item = new CatalogItem { Id = "orders", Kind = CatalogKind.Trees };

        // Assigned directly rather than through a render: this asserts the
        // parameter's own shape, and the panel tests already cover the host
        // actually supplying it through DynamicComponent.
#pragma warning disable BL0005
        using var view = new StubView { Selection = item };
#pragma warning restore BL0005

        Assert.That(view.Selection, Is.SameAs(item));
    }

    private class StubView : SelectionPluginViewBase
    {
        public CancellationToken Token => TabToken;
    }

    private sealed class CountingStubView : StubView
    {
        public int Disposals { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Disposals++;
            }

            base.Dispose(disposing);
        }
    }
}

/// <summary>
/// The single projection from the Explorer's own catalog kind onto the plugin
/// contract's selection kind. The host-state adapter and the detail panel both
/// read it, so a disagreement here would let a gate see one kind while the
/// strip resolves another.
/// </summary>
[TestFixture]
public sealed class ExplorerSelectionKindProjectionTests
{
    [TestCase(CatalogKind.Trees, ExplorerPluginSelectionKind.Tree)]
    [TestCase(CatalogKind.Views, ExplorerPluginSelectionKind.View)]
    [TestCase(CatalogKind.TagIndexes, ExplorerPluginSelectionKind.TagIndex)]
    public void Each_catalog_kind_projects_onto_its_own_selection_kind(
        CatalogKind kind,
        ExplorerPluginSelectionKind expected)
    {
        Assert.That(ExplorerSelectionKindProjection.ToPluginKind(kind), Is.EqualTo(expected));
    }

    [Test]
    public void An_unrecognised_catalog_kind_projects_onto_a_tree()
    {
        Assert.That(
            ExplorerSelectionKindProjection.ToPluginKind((CatalogKind)99),
            Is.EqualTo(ExplorerPluginSelectionKind.Tree),
            "an unknown kind resolves to the ordinary surfaces rather than to none");
    }
}
