using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The Apps area: the single <c>apps</c> stop on the directory spine. It owns
/// "Your apps", the source catalogue, consent review and lifecycle (A1), and its
/// completions and commands cover the app pages A2 renders under
/// <c>/apps/{slug}</c> as well, because exactly one area may hold the key.
/// </summary>
/// <remarks>
/// Visible to every caller whose workspace answers - every signed-in user sees
/// "Your apps" - and to an <c>AppInstall</c> holder through the catalogue probe.
/// Anything else, including a head that serves neither facade, is hidden.
/// </remarks>
/// <param name="access">The circuit's probe of the caller's rights.</param>
/// <param name="completions">The area's address completions.</param>
/// <param name="intents">Where palette commands post lifecycle actions.</param>
/// <param name="services">The circuit's services, for the navigator and tenancy at invocation time.</param>
internal sealed class AppsArea(
    AppsAccess access,
    AppsCompletionSource completions,
    AppsLifecycleIntents intents,
    IServiceProvider services) : IExplorerArea
{
    /// <summary>The area's position on the spine: epic E2 position 2, times ten.</summary>
    public const int Order = 20;

    /// <summary>The install command's id.</summary>
    public const string InstallCommandId = "apps.install";

    /// <summary>The prefix of each per-app upgrade command's id.</summary>
    public const string UpgradeCommandPrefix = "apps.upgrade.";

    /// <summary>The prefix of each per-app disable command's id.</summary>
    public const string DisableCommandPrefix = "apps.disable.";

    /// <inheritdoc />
    public string Key => AppsRoutes.AreaKey;

    /// <inheritdoc />
    public string DisplayName => "Apps";

    /// <inheritdoc />
    public int DirectoryOrder => Order;

    /// <inheritdoc />
    public IAddressCompletionSource? Completions => completions;

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands => access.Current is { } snapshot ? BuildCommands(snapshot) : [];

    /// <inheritdoc />
    public async ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        var snapshot = await access.GetAsync(cancellationToken).ConfigureAwait(false);
        return snapshot.IsVisible ? AreaAvailability.Visible : AreaAvailability.Hidden;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken)
    {
        var snapshot = await access.GetAsync(cancellationToken).ConfigureAwait(false);
        if (snapshot.Control.CanList)
        {
            var parts = new List<string>(3) { Count(snapshot.Installed.Length, "app", "apps") + " installed" };
            var failed = snapshot.FailedActivations.Count();
            if (failed > 0)
            {
                parts.Add($"{failed} failed activation");
            }

            if (snapshot.Updates.Length > 0)
            {
                parts.Add(Count(snapshot.Updates.Length, "update", "updates") + " available");
            }

            return string.Join(", ", parts);
        }

        if (snapshot.WorkspaceServed)
        {
            return snapshot.MyApps.IsEmpty
                ? "No app is assigned to you yet"
                : Count(snapshot.MyApps.Length, "app", "apps") + " available to you";
        }

        return null;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetDirectoryBadgeAsync(CancellationToken cancellationToken)
    {
        var snapshot = await access.GetAsync(cancellationToken).ConfigureAwait(false);
        var count = snapshot.Control.CanList ? snapshot.Installed.Length : snapshot.MyApps.Length;
        return count > 0 ? count.ToString("N0", System.Globalization.CultureInfo.InvariantCulture) : null;
    }

    /// <summary>The upgrade command id for <paramref name="slug"/>.</summary>
    /// <param name="slug">The app slug.</param>
    public static string UpgradeCommandId(string slug) => UpgradeCommandPrefix + slug;

    /// <summary>The disable command id for <paramref name="slug"/>.</summary>
    /// <param name="slug">The app slug.</param>
    public static string DisableCommandId(string slug) => DisableCommandPrefix + slug;

    private IReadOnlyList<ExplorerCommand> BuildCommands(AppsAccessSnapshot snapshot)
    {
        var tenant = services.GetService<ExplorerTenancy>()?.ActiveTenant;
        var commands = new List<ExplorerCommand>();

        if (snapshot.CanInstall)
        {
            commands.Add(new ExplorerCommand(InstallCommandId, "Install app...")
            {
                Detail = "Browse the catalogue of every configured source",
                Target = AppsRoutes.Landing(tenant),
                InvokeAsync = _ =>
                {
                    services.GetRequiredService<ExplorerNavigator>().NavigateTo(
                        AppsRoutes.Catalogue(tenant, AppsCatalogueView.Default with { Filter = AvailableAppFilter.Available }));
                    return ValueTask.CompletedTask;
                },
            });
        }

        if (snapshot.CanInstall)
        {
            foreach (var update in snapshot.Updates)
            {
                var id = UpgradeCommandId(update.Slug);
                if (!ExplorerCommand.IsValidId(id))
                {
                    continue;
                }

                var slug = update.Slug;
                commands.Add(new ExplorerCommand(id, $"Upgrade {slug}")
                {
                    Detail = $"{update.InstalledVersion} to {update.NewestVersion} from {update.SourceKey}",
                    Target = AppsRoutes.Review(tenant, update.SourceKey, slug, update.NewestVersion),
                    InvokeAsync = _ =>
                    {
                        intents.Post(slug, AppLifecycleVerb.Upgrade);
                        return ValueTask.CompletedTask;
                    },
                });
            }
        }

        if (snapshot.Control.CanDisable && snapshot.CanReview)
        {
            foreach (var app in snapshot.Installed.Where(app => app.State == AppLifecycleState.Enabled))
            {
                var id = DisableCommandId(app.Slug);
                if (!ExplorerCommand.IsValidId(id) || string.IsNullOrWhiteSpace(app.Provenance.Source))
                {
                    continue;
                }

                var slug = app.Slug;
                commands.Add(new ExplorerCommand(id, $"Disable {slug}")
                {
                    Detail = "Asks for confirmation first",
                    Target = AppsRoutes.Review(tenant, app.Provenance.Source, slug),
                    InvokeAsync = _ =>
                    {
                        intents.Post(slug, AppLifecycleVerb.Disable);
                        return ValueTask.CompletedTask;
                    },
                });
            }
        }

        return commands;
    }

    private static string Count(int count, string one, string many) =>
        $"{count.ToString("N0", System.Globalization.CultureInfo.InvariantCulture)} {(count == 1 ? one : many)}";
}
