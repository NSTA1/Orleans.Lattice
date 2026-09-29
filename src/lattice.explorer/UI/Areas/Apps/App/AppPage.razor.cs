using System.Collections.Immutable;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.App;

/// <summary>
/// An installed app's manifest-derived page, <c>[/t/{tenant}]/apps/{slug}/{section}</c>:
/// its overview, trees, roles, MCP tools, subscriptions and replication intent for every
/// caller with access; its consent and drift for an <c>AppInstall</c> holder; and, for an
/// app that ships a UI and is among the caller's own apps, the <c>open</c> section hosting
/// that UI in its sandboxed frame.
/// </summary>
/// <remarks>
/// <para>
/// The page loads the app once per tenant and slug and switches sections without
/// reloading. A caller who holds neither a role nor <c>AppInstall</c> gets the not-found
/// page for every address under the slug, exactly as for an app that does not exist; so
/// does an address naming a section the caller does not have.
/// </para>
/// <para>
/// The open section keeps its in-frame path in the address: <c>/apps/{slug}/open/a/b</c>
/// is delivered to the frame as <c>/a/b</c> (<c>nav.changed</c>), and a path the frame
/// reports (<c>nav.sync</c>) becomes the address, so a deep link reopens the app where it
/// was. The routes declare optional parameters, never a catch-all, so a path deeper than
/// <see cref="AppPageAddresses.MaxInAppSegments"/> segments rides in the address's query.
/// </para>
/// </remarks>
public partial class AppPage : IDisposable
{
    /// <summary>How many times a page re-reads an app that is still settling after a lifecycle change.</summary>
    internal const int SettlingRetries = 4;

    /// <summary>How long a page waits between those re-reads.</summary>
    internal static readonly TimeSpan SettlingRetryDelay = TimeSpan.FromSeconds(1);

    private CancellationTokenSource? _loading;
    private AppPageLoad? _load;
    private (string? Tenant, string Slug, string Tab)? _loadedFor;
    private bool _settling;

    [Inject]
    internal AppPageLoader Loader { get; set; } = default!;

    [Inject]
    internal AppsAccess Access { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    /// <summary>The app slug the address names.</summary>
    internal string Slug => Address.Path.Count > 0 ? Address.Path[0] : string.Empty;

    /// <summary>The section the address names; the overview when it names none.</summary>
    internal string Tab => Address.Path.Count > 1 ? Address.Path[1] : AppPageTabs.Overview;

    /// <summary>The in-frame path the address carries for the open section, or <see langword="null"/>.</summary>
    internal string? FramePath => Tab == AppPageTabs.Open ? AppPageAddresses.FramePath(Address) : null;

    private bool IsSectionAddress =>
        string.Equals(Address.Area, AppPageAddresses.AppsArea, StringComparison.Ordinal)
        && Address.Path.Count >= 1
        && (Address.Path.Count <= 2 || string.Equals(Address.Path[1], AppPageTabs.Open, StringComparison.Ordinal));

    /// <summary>Stops any load in flight.</summary>
    public void Dispose()
    {
        Access.Changed -= OnAppsChanged;
        _loading?.Cancel();
        _loading?.Dispose();
        _loading = null;
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnInitialized() => Access.Changed += OnAppsChanged;

    /// <inheritdoc />
    /// <remarks>
    /// Every navigation to a different section reads the app again, so a page reached
    /// after an install, an enable or a binding never shows what was true before it.
    /// Moving between sections of the same app keeps the page on screen while the
    /// new read runs; only a move within the open app's own frame (its in-app path)
    /// reads nothing, because re-reading would tear the running frame down.
    /// </remarks>
    protected override async Task OnParametersSetAsync()
    {
        var key = (Address.Tenant, Slug, Tab);
        if (_loadedFor == key)
        {
            return;
        }

        var sameApp = _loadedFor is { } loaded && loaded.Tenant == key.Tenant && loaded.Slug == key.Slug;
        await LoadAsync(key, keepShowing: sameApp);
    }

    internal static LtStateRole StateRole(AppLifecycleState state) => state switch
    {
        AppLifecycleState.Installed => LtStateRole.Installed,
        AppLifecycleState.Enabled => LtStateRole.Enabled,
        AppLifecycleState.Disabled => LtStateRole.Disabled,
        AppLifecycleState.Uninstalled => LtStateRole.Uninstalled,
        AppLifecycleState.Failed => LtStateRole.Failed,
        _ => LtStateRole.Unknown,
    };

    /// <summary>
    /// An app-supplied documentation URL as a link target, only when it is an absolute
    /// <c>https</c> or <c>http</c> URL; anything else is shown as text and never followed.
    /// </summary>
    /// <param name="url">The untrusted URL.</param>
    /// <returns>The URL to link, or <see langword="null"/>.</returns>
    internal static string? SafeLink(string? url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) && (uri.Scheme == Uri.UriSchemeHttps || uri.Scheme == Uri.UriSchemeHttp)
            ? uri.AbsoluteUri
            : null;

    /// <summary>The wire name an MCP binding gives an app's tool: <c>{slug}_{tool}</c>.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="tool">The tool.</param>
    /// <returns>The wire name.</returns>
    internal static string ToolName(string slug, AppMcpToolDescriptor tool) => slug + "_" + tool.Name;

    private static string Scopes(AppRoleDescriptor role, string slug) =>
        role.Scopes.IsDefaultOrEmpty ? "No scope" : string.Join("; ", role.Scopes.Select(scope => AppPageText.Scope(scope, slug)));

    private static string BoundGroups(AppDescriptor admin, string role)
    {
        var groups = admin.RoleBindings.Where(binding => string.Equals(binding.RoleName, role, StringComparison.Ordinal))
            .Select(binding => binding.GroupId)
            .ToArray();
        return groups.Length == 0 ? "Not bound" : string.Join(", ", groups);
    }

    private static string Provenance(AppProvenanceDescriptor provenance) =>
        string.IsNullOrWhiteSpace(provenance.Reference)
            ? provenance.Source + " / " + provenance.Publisher
            : provenance.Source + " / " + provenance.Publisher + " / " + provenance.Reference;

    private static string TreeSummary(AppPageTree tree)
    {
        var parts = ImmutableArray.CreateBuilder<string>();
        parts.Add(tree.Adopted ? "Adopted" : "Owned");
        if (tree.ShardCount is { } shards)
        {
            parts.Add(AppPageText.Count(shards, "shard"));
        }

        parts.Add("retention " + AppPageText.Retention(tree.SoftDeleteDuration));
        return string.Join(", ", parts);
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private void SelectSection(string section)
    {
        if (!string.IsNullOrEmpty(Slug))
        {
            Navigator.NavigateTo(AppPageAddresses.Page(Address.Tenant, Slug, section));
        }
    }

    private void HandleNavSync(string framePath)
    {
        if (AppPageAddresses.FromFramePath(Address.Tenant, Slug, framePath) is not { } target)
        {
            return;
        }

        var current = Navigator.Canonicalize(Address);
        if (Navigator.Canonicalize(target).Equals(current))
        {
            return;
        }

        // The frame's first report replaces the bare open address, so Back leaves the app
        // rather than stepping into its start page; later reports are history entries.
        Navigator.NavigateTo(target, replace: FramePath is null);
    }

    private async Task RetryAsync()
    {
        _loadedFor = null;
        await LoadAsync((Address.Tenant, Slug, Tab), keepShowing: false);
    }

    // A lifecycle change anywhere in the Apps area: read this app again, keeping the
    // page on screen, unless the change was to another app.
    private void OnAppsChanged() => _ = InvokeAsync(async () =>
    {
        if (string.IsNullOrEmpty(Slug) || _loadedFor is null)
        {
            return;
        }

        await LoadAsync((Address.Tenant, Slug, Tab), keepShowing: true);
        StateHasChanged();
    });

    /// <summary>
    /// Whether a read of an app that changed in this circuit a moment ago is still
    /// settling: not found yet, or enabled on the administrative path while the caller's
    /// own access to it has not caught up.
    /// </summary>
    private bool IsSettling(AppPageLoad load, string slug) =>
        Access.ChangedRecently(slug)
        && (load.Kind == AppPageLoadKind.NotFound
            || load.Model is { Admin.State: AppLifecycleState.Enabled, CallerRoleNames.IsDefaultOrEmpty: true });

    private async Task LoadAsync((string? Tenant, string Slug, string Tab) key, bool keepShowing)
    {
        _loading?.Cancel();
        _loading?.Dispose();
        var loading = _loading = new CancellationTokenSource();

        if (!keepShowing)
        {
            _load = null;
        }

        _loadedFor = key;
        StateHasChanged();

        AppPageLoad load;
        try
        {
            load = await Loader.LoadAsync(key.Slug, loading.Token);

            // Right after an install or an enable the cluster's reads can briefly miss
            // the change. Read again a few times, saying so, before settling on the answer.
            for (var attempt = 0; attempt < SettlingRetries && IsSettling(load, key.Slug); attempt++)
            {
                _settling = true;
                StateHasChanged();
                await Task.Delay(SettlingRetryDelay, Time, loading.Token);
                load = await Loader.LoadAsync(key.Slug, loading.Token);
            }
        }
        catch (OperationCanceledException) when (loading.IsCancellationRequested)
        {
            return;
        }
        finally
        {
            if (ReferenceEquals(loading, _loading))
            {
                _settling = false;
            }
        }

        if (!ReferenceEquals(loading, _loading))
        {
            return;
        }

        _load = load;
        if (load.Kind == AppPageLoadKind.Loaded && Address.Path.Count == 1)
        {
            // The bare /apps/{slug} is the overview; name it, so the address chain ends at
            // the section being shown.
            Navigator.NavigateTo(AppPageAddresses.Page(Address.Tenant, key.Slug, AppPageTabs.Overview), replace: true);
        }
    }
}
