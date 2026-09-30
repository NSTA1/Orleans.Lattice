using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// What the page may say about the app that owns a backed-up tree: its display
/// name and whether it declares the tree rebuildable. Read through the
/// administrator's apps control first and the caller's own workspace second,
/// each fail-closed: a caller who may read neither learns only the slug the
/// tree id already carries.
/// </summary>
/// <remarks>
/// A found app is remembered for the circuit, under the caller it was read for
/// (the sign-in, the endpoint and the asserted tenant), so another caller's or
/// another tenant's app of the same slug is never described from it; a miss is not remembered, so a grant that arrives later is
/// seen on the next page.
/// </remarks>
internal sealed class BackupAppTrees
{
    private readonly IServiceProvider _services;
    private readonly ShellCaller _caller;
    private readonly ConcurrentDictionary<(ShellCallerKey Caller, string Slug), BackupAppInfo> _found = new();

    /// <summary>Creates the lookup.</summary>
    /// <param name="services">The circuit's services, from which the apps facades are resolved when present.</param>
    /// <param name="tenant">The circuit's asserted tenant, read when no <paramref name="caller"/> is given.</param>
    /// <param name="caller">The circuit's caller, which keys what is remembered; when <see langword="null"/>, a caller over <paramref name="tenant"/> alone.</param>
    public BackupAppTrees(IServiceProvider services, ShellAssertedTenant? tenant = null, ShellCaller? caller = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        _services = services;
        _caller = caller ?? new ShellCaller(tenant: tenant);
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

        var key = _caller.Current;
        if (_found.TryGetValue((key, slug), out var known))
        {
            return known;
        }

        var info = await FromControlAsync(slug, cancellationToken).ConfigureAwait(false)
            ?? await FromWorkspaceAsync(slug, cancellationToken).ConfigureAwait(false);
        if (info is not null && _caller.Current == key)
        {
            _found[(key, slug)] = info;
        }

        return info;
    }

    private async Task<BackupAppInfo?> FromControlAsync(string slug, CancellationToken cancellationToken)
    {
        if (_services.GetShellFacade<ILatticeAppsControl>() is not { } control)
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
        if (_services.GetShellFacade<ILatticeAppWorkspace>() is not { } workspace)
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
