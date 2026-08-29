using Orleans.Lattice.Explorer.Plugins.History;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The nested-surface seam: how one per-selection surface renders another
/// inline without the two packages referencing each other.
/// <para>
/// This is what keeps the per-key revision timeline reachable exactly where it
/// has always been - from a row on the value drill-down surface - while still
/// letting it ship as its own package. Its edges are therefore worth pinning:
/// an absent contribution must read as "offer no affordance" rather than fail,
/// and a duplicate contribution must not throw on a render path.
/// </para>
/// </summary>
[TestFixture]
public sealed class SelectionNestedSurfaceRegistryTests
{
    [Test]
    public void A_contributed_surface_resolves_by_its_id()
    {
        var registry = new SelectionNestedSurfaceRegistry([new StubSurface("alpha", typeof(string))]);

        Assert.That(registry.Find("alpha"), Is.EqualTo(typeof(string)));
    }

    [Test]
    public void An_absent_id_resolves_to_null_rather_than_throwing()
    {
        // The hosting surface reads null as "render no affordance", so a head
        // that withheld the contributing package gets a working surface with one
        // fewer button rather than a fault on the render path.
        var registry = new SelectionNestedSurfaceRegistry([new StubSurface("alpha", typeof(string))]);

        Assert.That(registry.Find("beta"), Is.Null);
    }

    [Test]
    public void An_empty_registry_resolves_nothing()
    {
        var registry = new SelectionNestedSurfaceRegistry([]);

        Assert.That(registry.Find(SelectionNestedSurfaceKeys.EntryHistory), Is.Null);
    }

    [Test]
    public void Ids_are_compared_ordinally_so_casing_is_significant()
    {
        var registry = new SelectionNestedSurfaceRegistry([new StubSurface("alpha", typeof(string))]);

        Assert.That(registry.Find("ALPHA"), Is.Null);
    }

    [Test]
    public void The_first_contribution_for_an_id_wins_rather_than_throwing()
    {
        // A package that registers itself from more than one composition helper
        // must be idempotent, not a hard failure the first render discovers.
        var registry = new SelectionNestedSurfaceRegistry(
        [
            new StubSurface("alpha", typeof(string)),
            new StubSurface("alpha", typeof(int)),
        ]);

        Assert.That(registry.Find("alpha"), Is.EqualTo(typeof(string)));
    }

    [Test]
    public void A_null_contribution_is_skipped_rather_than_faulting_the_registry()
    {
        var registry = new SelectionNestedSurfaceRegistry([null!, new StubSurface("alpha", typeof(string))]);

        Assert.That(registry.Find("alpha"), Is.EqualTo(typeof(string)));
    }

    [Test]
    public void The_registry_rejects_a_null_contribution_set()
    {
        Assert.That(() => new SelectionNestedSurfaceRegistry(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Find_rejects_a_null_id()
    {
        var registry = new SelectionNestedSurfaceRegistry([]);

        Assert.That(() => registry.Find(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void The_revision_timeline_is_contributed_under_the_shared_entry_history_id()
    {
        // Two packages name this id - the one that contributes the view and the
        // one that renders it - so a drift between them would silently remove the
        // History button.
        var surface = new EntryHistoryNestedSurface();

        Assert.Multiple(() =>
        {
            Assert.That(surface.SurfaceId, Is.EqualTo(SelectionNestedSurfaceKeys.EntryHistory));
            Assert.That(surface.SurfaceId, Is.EqualTo("orleans.lattice.history.entry"));
            Assert.That(surface.ViewType.Name, Is.EqualTo("HistoryTab"));
        });
    }

    private sealed class StubSurface(string surfaceId, Type viewType) : ISelectionNestedSurface
    {
        public string SurfaceId { get; } = surfaceId;

        public Type ViewType { get; } = viewType;
    }
}
