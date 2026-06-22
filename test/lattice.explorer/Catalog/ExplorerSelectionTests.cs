using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.Tests.Catalog;

[TestFixture]
public class ExplorerSelectionTests
{
    private static CatalogItem Item(string id, CatalogKind kind = CatalogKind.Trees) =>
        new() { Id = id, Kind = kind };

    [Test]
    public void Select_RaisesAndUpdatesSelected()
    {
        var selection = new ExplorerSelection();
        var raised = 0;
        selection.SelectionChanged += () => raised++;

        var item = Item("alpha");
        selection.Select(item);

        Assert.That(selection.Selected, Is.EqualTo(item));
        Assert.That(raised, Is.EqualTo(1));
    }

    [Test]
    public void Select_SameValue_DoesNotRaise()
    {
        var selection = new ExplorerSelection();
        selection.Select(Item("alpha"));
        var raised = 0;
        selection.SelectionChanged += () => raised++;

        selection.Select(Item("alpha"));

        Assert.That(raised, Is.EqualTo(0));
    }

    [Test]
    public void Select_DifferentKindSameId_Raises()
    {
        var selection = new ExplorerSelection();
        selection.Select(Item("x", CatalogKind.Trees));
        var raised = 0;
        selection.SelectionChanged += () => raised++;

        selection.Select(Item("x", CatalogKind.Views));

        Assert.That(raised, Is.EqualTo(1));
        Assert.That(selection.Selected!.Kind, Is.EqualTo(CatalogKind.Views));
    }

    [Test]
    public void Select_Null_ClearsAndRaises()
    {
        var selection = new ExplorerSelection();
        selection.Select(Item("alpha"));
        var raised = 0;
        selection.SelectionChanged += () => raised++;

        selection.Select(null);

        Assert.That(selection.Selected, Is.Null);
        Assert.That(raised, Is.EqualTo(1));
    }

    [Test]
    public void Select_NullWhenAlreadyNull_DoesNotRaise()
    {
        var selection = new ExplorerSelection();
        var raised = 0;
        selection.SelectionChanged += () => raised++;

        selection.Select(null);

        Assert.That(raised, Is.EqualTo(0));
    }
}
