using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// A deterministic, hand-written <see cref="IExplorerPlugin"/> for the plugin
/// contract tests. Every seam is settable so a fixture can compose exactly the
/// plugin shape it needs without a mocking framework.
/// </summary>
internal sealed class FakeExplorerPlugin : IExplorerPlugin
{
    public FakeExplorerPlugin(
        string pluginId,
        ExplorerPluginSurface surface = ExplorerPluginSurface.Area,
        int order = 0,
        string? label = null,
        IExplorerPluginAccessGate? gate = null,
        Type? domainContract = null,
        Type? viewType = null)
    {
        Descriptor = new ExplorerPluginDescriptor
        {
            PluginId = pluginId,
            Label = label ?? pluginId,
            Surface = surface,
            Order = order,
        };

        AccessGate = gate ?? ExplorerPluginAccessGates.Denied;
        DomainContract = domainContract;
        ViewType = viewType ?? typeof(FakeExplorerPlugin);
    }

    public ExplorerPluginDescriptor Descriptor { get; }

    public Type ViewType { get; }

    public Type? DomainContract { get; }

    public IExplorerPluginAccessGate AccessGate { get; }
}
