using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The Backups plugin's controlled domain model: the single contract the host
/// resolves for the plugin, and the whole of what its view may reach.
/// <para>
/// This is the epic's D3 seam. The Backups panel receives an
/// <see cref="IExplorerPluginHostContext"/> bound to its own plugin id and
/// resolves exactly this type from it; it never takes the cluster connection,
/// the gRPC channel, the Explorer's navigation catalog, or another plugin's
/// services. The plugin's reach is therefore stated in one interface and is
/// reviewable in isolation.
/// </para>
/// <para>
/// It deliberately re-exposes only two things: the backup catalogue view-model
/// the plugin already owns, and the tree list a capture picks from, projected
/// onto the plugin's own <see cref="BackupTreeOption"/>.
/// </para>
/// </summary>
/// <seealso cref="IExplorerPluginHostContext"/>
public interface IBackupsDomain
{
    /// <summary>
    /// The backup catalogue and operations surface. Folds a permission denial or
    /// a transport failure into a status rather than throwing, so the view stays
    /// thin. Never <see langword="null"/>.
    /// </summary>
    IBackupCatalogReader Catalog { get; }

    /// <summary>
    /// The trees a capture may target, in discovery order. A discovery failure
    /// yields an empty list rather than throwing, so the area still lists any
    /// visible backups and the operator can retry.
    /// </summary>
    /// <param name="cancellationToken">Cancels the discovery.</param>
    Task<IReadOnlyList<BackupTreeOption>> LoadTreesAsync(CancellationToken cancellationToken = default);
}
