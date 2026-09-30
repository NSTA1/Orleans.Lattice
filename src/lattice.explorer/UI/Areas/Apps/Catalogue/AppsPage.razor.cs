using System.Collections.Immutable;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// "Your apps" (<c>/apps</c>): the apps the caller holds a role in, the Catalogue
/// view for an <c>AppInstall</c> holder, and failed activations called out.
/// </summary>
public partial class AppsPage : IDisposable
{
    private readonly Dictionary<string, string?> _icons = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string?> _installedIcons = new(StringComparer.Ordinal);
    private readonly Dictionary<(string Source, string Slug), AppPresentationDescriptor?> _presentations = [];
    private readonly CancellationTokenSource _lifetime = new();
    private AppsAccessSnapshot? _snapshot;
    private IReadOnlyList<AppSummary> _failed = [];
    private ImmutableArray<AppSummary> _installed = [];

    [Inject]
    internal AppsAccess Access { get; set; } = default!;

    [Inject]
    internal AppsFacades Facades { get; set; } = default!;

    [Inject]
    internal AppsLifecycleIntents Intents { get; set; } = default!;

    private string TenantPhrase => Address.Tenant is { } tenant ? $" in tenant {tenant}" : string.Empty;

    private string Lede => (_snapshot?.Control.CanList == true, _snapshot?.CanBrowseCatalogue == true) switch
    {
        (true, _) => $"The apps you hold a role in, and every app installed{TenantPhrase}, with their install, consent and lifecycle. Sources are configured per cluster.",
        (false, true) => $"The apps you hold a role in{TenantPhrase}, and their install, consent and lifecycle. Sources are configured per cluster.",
        _ => $"The apps you hold a role in{TenantPhrase}.",
    };

    /// <summary>Stops listening for changes and cancels outstanding icon reads.</summary>
    /// <remarks>
    /// The source is cancelled, never disposed: a read already on its way can resume after
    /// this page is gone, and reading a disposed source's token would throw out of a
    /// lifecycle method and end the circuit (issue #4011).
    /// </remarks>
    public void Dispose()
    {
        Access.Changed -= OnAccessChanged;
        _lifetime.Cancel();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        Access.Changed += OnAccessChanged;
        await LoadAsync();
    }

    private async Task LoadAsync()
    {
        try
        {
            _snapshot = await Access.GetAsync(_lifetime.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        _failed = [.. _snapshot.FailedActivations];
        _installed = _snapshot.Control.CanList ? _snapshot.InTenant : [];
        StateHasChanged();
        await LoadIconsAsync(_snapshot);
        await LoadInstalledPresentationAsync(_snapshot);
    }

    private async Task LoadInstalledPresentationAsync(AppsAccessSnapshot snapshot)
    {
        if (_installed.IsEmpty || !snapshot.CanBrowseCatalogue)
        {
            return;
        }

        ImmutableArray<AvailableAppSummary> index;
        try
        {
            index = await Access.GetCompletionIndexAsync(_lifetime.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        foreach (var offer in index)
        {
            _presentations.TryAdd((offer.SourceKey, offer.Slug), offer.Presentation);
        }

        StateHasChanged();
        if (Facades.Catalog is not { } catalog)
        {
            return;
        }

        foreach (var app in _installed.Where(app => InstalledPresentation(app)?.Icon is not null && !_installedIcons.ContainsKey(app.Slug)))
        {
            try
            {
                _installedIcons[app.Slug] = AppsPresentation.IconDataUrl(await catalog.GetIconAsync(app.Provenance.Source, app.Slug, app.Version, _lifetime.Token));
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception)
            {
                _installedIcons[app.Slug] = null;
            }

            StateHasChanged();
        }
    }

    private async Task LoadIconsAsync(AppsAccessSnapshot snapshot)
    {
        if (Facades.Workspace is not { } workspace)
        {
            return;
        }

        foreach (var app in snapshot.MyApps.Where(app => app.Presentation?.Icon is not null && !_icons.ContainsKey(app.Slug)))
        {
            try
            {
                _icons[app.Slug] = AppsPresentation.IconDataUrl(await workspace.GetIconAsync(app.Slug, _lifetime.Token));
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception)
            {
                _icons[app.Slug] = null;
            }

            StateHasChanged();
        }
    }

    private void OnAccessChanged() => _ = InvokeAsync(LoadAsync);

    private static object? RolesText(WorkspaceAppSummary app) => app.Roles.IsDefaultOrEmpty ? "-" : string.Join(", ", app.Roles);

    private string? IconOf(string slug) => _icons.GetValueOrDefault(slug);

    private string? InstalledIconOf(AppSummary app) => _installedIcons.GetValueOrDefault(app.Slug);

    private AppPresentationDescriptor? InstalledPresentation(AppSummary app) =>
        _presentations.GetValueOrDefault((app.Provenance.Source, app.Slug));

    private string InstalledName(AppSummary app) => AppsPresentation.DisplayName(InstalledPresentation(app), app.Slug);

    private bool HasSource(AppSummary app) => _snapshot?.CanReview == true && !string.IsNullOrWhiteSpace(app.Provenance.Source);

    private bool CanDisable(AppSummary app) =>
        HasSource(app) && _snapshot!.Control.CanDisable && app.State == AppLifecycleState.Enabled;

    private bool CanUninstall(AppSummary app) => HasSource(app) && _snapshot!.Control.CanUninstall;

    /// <summary>
    /// The installed version's manage page (its review from the source it was installed from),
    /// or, when the caller cannot review or the source is not recorded, the app's own page.
    /// </summary>
    private string ManageHref(AppSummary app) => HasSource(app)
        ? Href(AppsRoutes.Review(Address.Tenant, app.Provenance.Source, app.Slug, app.Version))
        : Href(AppsRoutes.App(Address.Tenant, app.Slug));

    /// <summary>
    /// Opens the manage page with <paramref name="verb"/> waiting for it, so the change is asked
    /// for there behind the same confirmation that states its consequences; nothing changes here.
    /// </summary>
    private void Manage(AppSummary app, AppLifecycleVerb verb)
    {
        Intents.Post(app.Slug, verb);
        Navigator.NavigateTo(AppsRoutes.Review(Address.Tenant, app.Provenance.Source, app.Slug, app.Version));
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private RenderFragment AppActions(WorkspaceAppSummary app) => builder => BuildActions(builder, app);

    private void BuildActions(RenderTreeBuilder builder, WorkspaceAppSummary app)
    {
        var name = AppsPresentation.DisplayName(app.Presentation, app.Slug);
        if (app.HasUi)
        {
            builder.OpenElement(0, "a");
            builder.AddAttribute(1, "class", "lt-btn");
            builder.AddAttribute(2, "href", Href(AppsRoutes.Open(Address.Tenant, app.Slug)));
            builder.AddAttribute(3, "aria-label", "Open " + name);
            builder.AddContent(4, "Open");
            builder.CloseElement();
        }

        builder.OpenElement(5, "a");
        builder.AddAttribute(6, "class", "lt-btn lt-btn--quiet");
        builder.AddAttribute(7, "href", Href(AppsRoutes.App(Address.Tenant, app.Slug)));
        builder.AddAttribute(8, "aria-label", "Details of " + name);
        builder.AddContent(9, "Details");
        builder.CloseElement();
    }
}
