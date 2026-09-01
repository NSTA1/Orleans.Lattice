using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Data;
using Orleans.Lattice.Explorer.Plugins.DeadLetter;
using Orleans.Lattice.Explorer.Plugins.Metrics;
using Orleans.Lattice.Explorer.Plugins.Selection;
using Orleans.Lattice.Explorer.Plugins.Topology;

namespace Orleans.Lattice.Explorer.Tests.Detail;

/// <summary>
/// The order the per-selection surfaces are offered in, which is a statement
/// about what a caller came to do.
/// </summary>
/// <remarks>
/// A tree selection used to open on Metrics, with Data third. Reading the data
/// is the primary task and monitoring it is a secondary one, so the strip
/// ordered the caller's least likely intent first and made the most likely one
/// a two-step. Ordering is the descriptor's own hint, so this fixture pins the
/// relative order rather than the numbers.
/// </remarks>
[TestFixture]
public sealed class SelectionSurfaceOrderTests
{
    [Test]
    public void Data_precedes_metrics_for_a_tree_selection()
    {
        var order = ResolveTreeSurfaceOrder();

        Assert.That(
            order.IndexOf(SelectionPluginKeys.Data),
            Is.LessThan(order.IndexOf(SelectionPluginKeys.Metrics)),
            "the primary task is reading the data, so it is the surface a selection opens on");
    }

    [Test]
    public void The_tree_surfaces_run_from_the_primary_task_to_the_exceptional_one()
    {
        var order = ResolveTreeSurfaceOrder();

        Assert.That(
            order,
            Is.EqualTo(new[]
            {
                SelectionPluginKeys.Data,
                SelectionPluginKeys.Topology,
                SelectionPluginKeys.Metrics,
                SelectionPluginKeys.DeadLetter,
            }),
            "read it, then see how it is laid out, then how it is behaving, then what it rejected");
    }

    [Test]
    public void Every_tree_surface_declares_a_distinct_position()
    {
        // Two surfaces sharing a hint would leave their order decided by
        // registration order, which is a different answer in a head that
        // registers them in a different sequence.
        var hints = TreeSurfaces().Select(plugin => plugin.Descriptor.Order).ToArray();

        Assert.That(hints, Is.Unique);
    }

    private static List<string> ResolveTreeSurfaceOrder() =>
        new ExplorerPluginCatalog(TreeSurfaces())
            .ForSelection(ExplorerPluginSelectionKind.Tree)
            .Select(plugin => plugin.Descriptor.PluginId)
            .ToList();

    // The real descriptors, in an order deliberately unlike the expected one, so
    // the assertion measures the hints rather than this list.
    private static IExplorerPlugin[] TreeSurfaces() =>
    [
        new MetricsSelectionPlugin(),
        new DeadLetterSelectionPlugin(),
        new DataSelectionPlugin(),
        new TopologySelectionPlugin(),
    ];
}
