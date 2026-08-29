using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Backup;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The Backups area as a plugin: its descriptor, the panel the shell renders
/// for it, and the access gate the Backups feature owns. The shell learns of
/// it only through <see cref="IExplorerPlugin"/>, so registering or withholding
/// this type is the whole of the head's opt-in.
/// </summary>
/// <param name="gate">The Backups feature's own access gate.</param>
public sealed class BackupsAreaPlugin(IBackupCapabilityService gate) : IExplorerPlugin
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = BackupsPluginKeys.PluginId,
        Label = "Backups",
        Surface = ExplorerPluginSurface.Area,
        Order = 100,
    };

    private readonly IBackupCapabilityService _gate = gate ?? throw new ArgumentNullException(nameof(gate));

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(BackupsPanel);

    /// <inheritdoc />
    /// <remarks>
    /// The panel still resolves its own feature services directly, so it
    /// declares no controlled domain contract yet; that lands with its
    /// conversion to a standalone plugin project.
    /// </remarks>
    public Type? DomainContract => null;

    /// <inheritdoc />
    public IExplorerPluginAccessGate AccessGate => _gate;
}
