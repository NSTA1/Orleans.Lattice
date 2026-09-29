using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// What the page may say about the app that owns a backed-up tree: its display
/// name and whether it declares the tree rebuildable. Read through the
/// administrator's apps control first and the caller's own workspace second,
/// each fail-closed: a caller who may read neither learns only the slug the
/// tree id already carries.
/// </summary>
/// <remarks>
/// A found app is remembered for the circuit; a miss is not, so a grant that
/// arrives later is seen on the next page.
/// </remarks>
internal sealed class BackupAppTrees
{
    private readonly IServiceProvider _services;
    private readonly ConcurrentDictionary<string, BackupAppInfo> _found = new(StringComparer.Ordinal);

    /// <summary>Creates the lookup.</summary>
    /// <param name="services">The circuit's services, from which the apps facades are resolved when present.</param>
    public BackupAppTrees(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _services = services;
    }

    /// <summary>The app that owns <paramref name="tree"/>, or <see langword="null"/> when no app does or none can be read.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="cancellationToken">Cancels the lookup.</param>
    public async Task<BackupAppInfo?> FindAsync(BackupTreeName tree, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tree);
        if (tree.AppSlug is not { } slug)
        {
            return null;
        }

        if (_found.TryGetValue(slug, out var known))
        {
            return known;
        }

        var info = await FromControlAsync(slug, cancellationToken).ConfigureAwait(false)
            ?? await FromWorkspaceAsync(slug, cancellationToken).ConfigureAwait(false);
        if (info is not null)
        {
            _found[slug] = info;
        }

        return info;
    }

    private async Task<BackupAppInfo?> FromControlAsync(string slug, CancellationToken cancellationToken)
    {
        if (_services.GetService<ILatticeAppsControl>() is not { } control)
        {
            return null;
        }

        try
        {
            var app = await control.DescribeAsync(slug, cancellationToken: cancellationToken).ConfigureAwait(false);
            return app is null
                ? null
                : new BackupAppInfo(
                    app.Slug,
                    app.Presentation?.DisplayName,
                    [.. app.Trees.Where(tree => tree.Rebuildable).Select(tree => tree.Name)]);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            return null;
        }
    }

    private async Task<BackupAppInfo?> FromWorkspaceAsync(string slug, CancellationToken cancellationToken)
    {
        if (_services.GetService<ILatticeAppWorkspace>() is not { } workspace)
        {
            return null;
        }

        try
        {
            var app = await workspace.DescribeMyAppAsync(slug, cancellationToken).ConfigureAwait(false);
            return app is null
                ? null
                : new BackupAppInfo(
                    app.Slug,
                    app.Presentation?.DisplayName,
                    [.. app.Trees.Where(tree => tree.Rebuildable).Select(tree => tree.Name)]);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            return null;
        }
    }
}
