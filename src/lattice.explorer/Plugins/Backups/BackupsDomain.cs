using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The default <see cref="IBackupsDomain"/>: the host-side adapter that binds
/// the plugin's declared contract to the two services it actually needs - its
/// own backup catalogue reader, and the Explorer's tree catalog projected onto
/// the plugin's <see cref="BackupTreeOption"/>.
/// <para>
/// This type, not the panel, is where the plugin's reach is decided, which is
/// the point of the controlled domain seam: widening what Backups can see is an
/// edit here and nowhere else.
/// </para>
/// </summary>
/// <param name="catalog">The backup catalogue view-model the plugin owns.</param>
/// <param name="trees">The Explorer's cluster catalog reader, used for tree discovery only.</param>
public sealed class BackupsDomain(IBackupCatalogReader catalog, ICatalogReader trees) : IBackupsDomain
{
    /// <summary>
    /// The tree-discovery page size. Large enough that an ordinary cluster is a
    /// single round trip, and the loop below still pages a larger one.
    /// </summary>
    private const int TreePageSize = 200;

    private static readonly IReadOnlyList<BackupTreeOption> NoTrees = Array.Empty<BackupTreeOption>();

    private readonly IBackupCatalogReader _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));

    private readonly ICatalogReader _trees = trees ?? throw new ArgumentNullException(nameof(trees));

    /// <inheritdoc />
    public IBackupCatalogReader Catalog => _catalog;

    /// <inheritdoc />
    public async Task<IReadOnlyList<BackupTreeOption>> LoadTreesAsync(
        CancellationToken cancellationToken = default)
    {
        List<BackupTreeOption>? options = null;
        string? token = null;
        try
        {
            do
            {
                var page = await _trees
                    .LoadAsync(CatalogKind.Trees, token, TreePageSize, cancellationToken)
                    .ConfigureAwait(false);

                var items = page.Items;
                if (items.Count > 0)
                {
                    options ??= new List<BackupTreeOption>(items.Count);
                    for (var i = 0; i < items.Count; i++)
                    {
                        var item = items[i];
                        options.Add(new BackupTreeOption(item.Id, item.RestoreShadowOfTreeId));
                    }
                }

                token = page.NextPageToken;
            }
            while (token is not null);
        }
        catch (Exception)
        {
            // Discovery is best-effort: the area still lists any visible backups
            // and the operator can retry with Refresh. Whatever was already
            // gathered is kept rather than discarded.
        }

        return options ?? NoTrees;
    }
}
