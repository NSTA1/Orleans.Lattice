using Orleans.Lattice.Explorer.Backup.Components;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The Backups area as a plugin: its descriptor, the panel the shell renders
/// for it, the controlled domain contract it operates against, and the access
/// gate the Backups package owns. The shell learns of it only through
/// <see cref="IExplorerPlugin"/>, so registering or withholding this type is the
/// whole of the head's opt-in.
/// <para>
/// It declares <see cref="IBackupsDomain"/> through
/// <see cref="IExplorerPlugin{TDomain}"/>, so the reach of the whole Backups
/// surface is a compile-time fact stated once in this signature (epic decision
/// D3): the panel resolves that one contract from its bound host context and
/// receives nothing else from the host.
/// </para>
/// </summary>
/// <param name="gate">The Backups package's own access gate.</param>
public sealed class BackupsAreaPlugin(IBackupCapabilityService gate) : IExplorerPlugin<IBackupsDomain>
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
    public IExplorerPluginAccessGate AccessGate => _gate;
}
