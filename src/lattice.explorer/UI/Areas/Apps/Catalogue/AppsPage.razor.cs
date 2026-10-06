using System.Collections.Immutable;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Design.Components;

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
    private readonly Dictionary<string, AppDescriptor> _described = new(StringComparer.Ordinal);
    private readonly ComponentLifetime _lifetime = new();
    private AppsAccessSnapshot? _snapshot;
    private IReadOnlyList<AppSummary> _failed = [];
    private ImmutableArray<AppSummary> _installed = [];
    private AppsCallerGroups _caller = AppsCallerGroups.Unknown;

    [Inject]
    internal AppsAccess Access { get; set; } = default!;

    [Inject]
    internal AppsMembership Membership { get; set; } = default!;

    [Inject]
    internal AppInstallFlowStore Flows { get; set; } = default!;

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
    public void Dispose()
    {
        Access.Changed -= OnAccessChanged;
        _lifetime.Leave();
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
        await LoadHoldingAsync(_snapshot);
        await LoadIconsAsync(_snapshot);
        await LoadInstalledPresentationAsync(_snapshot);
    }

    /// <summary>
    /// For each enabled installed app the caller holds no role in, reads its recorded bindings and
    /// the caller's groups, so its row can say why there is no way in and how to get one (issue #4150).
    /// </summary>
    private async Task LoadHoldingAsync(AppsAccessSnapshot snapshot)
    {
        _described.Clear();
        var roleless = _installed.Where(app => !HoldsRole(snapshot, app)).ToArray();
        if (roleless.Length == 0 || !snapshot.Control.CanDescribe || Facades.Control is not { } control)
        {
            return;
        }

        try
        {
            _caller = await Membership.ReadAsync(_lifetime.Token);
            foreach (var app in roleless)
            {
                try
                {
                    if (await control.DescribeAsync(app.Slug, cancellationToken: _lifetime.Token) is { } described)
                    {
                        _described[app.Slug] = described;
                    }
                }
                catch (Exception error) when (error is not OperationCanceledException and not OutOfMemoryException)
                {
                    // Its row simply carries no notice: the bindings could not be read.
                }
            }
        }
        catch (OperationCanceledException)
        {
            return;
        }

        StateHasChanged();
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

    /// <summary>The installs this circuit completed in a tenant other than the one the page is in.</summary>
    private IEnumerable<AppInstallFlow> OtherTenantInstalls =>
        Flows.Flows
            .Where(flow => flow.Stage is AppInstallStage.Installed or AppInstallStage.Enabled
                && flow.Key.Tenant is not null
                && !string.Equals(flow.Key.Tenant, Address.Tenant, StringComparison.Ordinal))
            .OrderBy(flow => flow.Key.Tenant, StringComparer.Ordinal)
            .ThenBy(flow => flow.Key.Slug, StringComparer.Ordinal);

    private static bool HoldsRole(AppsAccessSnapshot snapshot, AppSummary app) =>
        app.State != AppLifecycleState.Enabled
        || snapshot.MyApps.Any(mine => string.Equals(mine.Slug, app.Slug, StringComparison.Ordinal));

    /// <summary>Why the caller cannot open an enabled installed app they hold no role in, or <see langword="null"/>.</summary>
    private AppRoleHoldingAssessment? InstalledHolding(AppSummary app) =>
        _described.TryGetValue(app.Slug, out var described) && !described.Roles.IsDefaultOrEmpty
            ? AppRoleHoldingAssessment.Assess(described, _caller)
            : null;

    private RenderFragment AppActions(WorkspaceAppSummary app) => builder => BuildActions(builder, app);

    private void BuildActions(RenderTreeBuilder builder, WorkspaceAppSummary app)
    {
        var name = AppsPresentation.DisplayName(app.Presentation, app.Slug);
        if (app.HasUi)
        {
            // The app's UI opens in a window of its own, which shares nothing with the console.
            builder.OpenElement(0, "a");
            builder.AddAttribute(1, "class", "lt-btn");
            builder.AddAttribute(2, "href", Href(AppsRoutes.Window(Address.Tenant, app.Slug)));
            builder.AddAttribute(3, "target", AppsRoutes.NewWindowTarget);
            builder.AddAttribute(4, "rel", AppsRoutes.NewWindowRel);
            builder.AddAttribute(5, "aria-label", "Open " + name + " in a new window");
            builder.AddContent(6, "Open");
            builder.CloseElement();
        }

        builder.OpenElement(7, "a");
        builder.AddAttribute(8, "class", "lt-btn lt-btn--quiet");
        builder.AddAttribute(9, "href", Href(AppsRoutes.App(Address.Tenant, app.Slug)));
        builder.AddAttribute(10, "aria-label", "Details of " + name);
        builder.AddContent(11, "Details");
        builder.CloseElement();
    }
}
