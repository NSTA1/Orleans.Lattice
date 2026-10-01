using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The pre-install review of one app version from a named source, and the staged
/// install, upgrade, re-consent or change of role bindings it starts. Shown only to an <c>AppInstall</c>
/// holder; anyone else gets not-found.
/// </summary>
public partial class AppReviewPage : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private AppInstallFlow? _flow;
    private ExplorerAddress? _resolvedFor;
    private string? _slug;
    private AppInstallStage? _announcedStage;
    private AppsCallerGroups _caller = AppsCallerGroups.Unknown;

    [Inject]
    internal AppsAccess Access { get; set; } = default!;

    [Inject]
    internal AppsMembership Membership { get; set; } = default!;

    [Inject]
    internal AppsFacades Facades { get; set; } = default!;

    [Inject]
    internal AppInstallFlowStore Flows { get; set; } = default!;

    [Inject]
    internal AppsLifecycleIntents Intents { get; set; } = default!;

    [Inject]
    internal ExplorerAreaDirectory Directory { get; set; } = default!;

    [Inject]
    private NavigationManager Navigation { get; set; } = default!;

    private bool CanInstall => Access.Current?.CanInstall == true;

    private bool CanRebind => CanInstall && _flow?.CanRebind == true && Facades.RoleBindings is not null;

    /// <summary>
    /// Whether the staged-flow stops are drawn: not while the description is still being read
    /// (it is not yet known whether this is an install or the installed version), and not on the
    /// installed version's manage page until a change to it is started.
    /// </summary>
    private bool ShowsSteps => _flow is { } flow
        && flow.Stage != AppInstallStage.Resolving
        && !(flow.IsManaging && flow.Stage == AppInstallStage.Review);

    private string SourceName => _flow?.Source?.DisplayName ?? _flow?.Key.SourceKey ?? string.Empty;

    private string TenantPhrase => Address.Tenant is { } tenant ? $" in tenant {tenant}" : string.Empty;

    /// <summary>The tenant the flow installs into, named where its outcome is reported.</summary>
    private string InstallTenantPhrase => _flow?.Key.Tenant is { } tenant ? $" in tenant {tenant}" : string.Empty;

    /// <summary>The app's own page in the tenant it was installed into.</summary>
    private string AppHref => _flow is { } flow ? Href(AppsRoutes.App(flow.Key.Tenant, flow.Key.Slug)) : string.Empty;

    /// <summary>The app's address as the address line shows it.</summary>
    private string AppAddressText => "/" + AppHref.TrimStart('/');

    /// <summary>Where the installed version's role bindings are changed, or <see langword="null"/> when the caller may not.</summary>
    private string? RebindHref =>
        CanInstall && Facades.RoleBindings is not null && _flow is { Descriptor: { } descriptor } flow
            ? Href(AppsRoutes.Review(flow.Key.Tenant, flow.Key.SourceKey, flow.Key.Slug, descriptor.Version))
            : null;

    private string Lede => _flow switch
    {
        { Descriptor: null } flow => $"From {SourceName}.",
        { Mode: AppInstallMode.Upgrade } flow => $"v{flow.Consent?.Version} is installed{TenantPhrase}; {SourceName} offers v{flow.Descriptor.Version}.",
        { Mode: AppInstallMode.Reconsent } flow => $"v{flow.Descriptor.Version} is installed{TenantPhrase}, from {SourceName}.",
        { } flow => $"v{flow.Descriptor.Version} from {SourceName}. Review exactly what it asks for; nothing is installed until you approve it{TenantPhrase}.",
        _ => string.Empty,
    };

    /// <summary>Stops listening to the flow; the flow itself keeps running in the circuit.</summary>
    public void Dispose()
    {
        Detach();
        Intents.Posted -= OnIntentPosted;
        _lifetime.Leave();
    }

    /// <inheritdoc />
    protected override void OnInitialized() => Intents.Posted += OnIntentPosted;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Equals(_resolvedFor, Address))
        {
            return;
        }

        _resolvedFor = Address;
        Detach();
        _flow = null;

        var path = Address.Path;
        if (path.Count != 3
            || !string.Equals(path[0], AppsRoutes.CatalogueSegment, StringComparison.Ordinal)
            || !AppsRoutes.TryReadSlugSegment(path[2], out var slug, out var version))
        {
            NotFound();
            return;
        }

        _slug = slug;
        AppsAccessSnapshot snapshot;
        try
        {
            snapshot = await Access.GetAsync(_lifetime.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        if (_lifetime.IsLeft)
        {
            // Left while the probe answered: a page that is gone decides nothing.
            return;
        }

        if (!snapshot.CanReview || Facades.Catalog is not { } catalog)
        {
            NotFound();
            return;
        }

        AppSourceSummary? source = null;
        try
        {
            var sources = await catalog.ListSourcesAsync(_lifetime.Token);
            source = sources.FirstOrDefault(candidate => string.Equals(candidate.Key, path[1], StringComparison.Ordinal));
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            // The description itself still answers; without the summary the flow assumes a static source.
        }

        if (_lifetime.IsLeft)
        {
            // Left while the sources were listed: no flow is started for a page that is gone.
            return;
        }

        Attach(Flows.GetOrCreate(new AppInstallFlowKey(Address.Tenant, path[1], slug, version), source));
    }

    private void Attach(AppInstallFlow flow)
    {
        _flow = flow;
        _announcedStage = flow.Stage is AppInstallStage.Installed or AppInstallStage.Enabled or AppInstallStage.Rebound ? flow.Stage : null;
        flow.Changed += OnFlowChanged;
        _ = flow.LoadAsync();
        _ = ReadCallerAsync();
        TakeUpgradeIntent();
    }

    /// <summary>
    /// Reads the caller's groups, so the outcome of an install or a re-binding can say whether
    /// they hold a role; read again after each, since the caller may have joined a group meanwhile.
    /// </summary>
    private async Task ReadCallerAsync()
    {
        try
        {
            _caller = await Membership.ReadAsync(_lifetime.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        if (!_lifetime.IsLeft)
        {
            await InvokeAsync(StateHasChanged);
        }
    }

    private void Detach()
    {
        if (_flow is not null)
        {
            _flow.Changed -= OnFlowChanged;
        }
    }

    private Task ReloadAsync()
    {
        if (_flow is { } flow)
        {
            Detach();
            Flows.Remove(flow.Key);
            Directory.Invalidate();
            Attach(Flows.GetOrCreate(flow.Key, flow.Source));
        }

        return Task.CompletedTask;
    }

    private void OnFlowChanged() => _ = InvokeAsync(() =>
    {
        // Installing, enabling and re-binding roles are separate changes, and each changes
        // what the rest of the circuit may read about the app, so each is announced once.
        if (_flow is { Stage: AppInstallStage.Installed or AppInstallStage.Enabled or AppInstallStage.Rebound } flow && _announcedStage != flow.Stage)
        {
            _announcedStage = flow.Stage;
            Access.Invalidate(flow.Key.Slug);
            Directory.Invalidate();
            _ = ReadCallerAsync();
        }
        else if (_flow is { Stage: not (AppInstallStage.Installed or AppInstallStage.Enabled or AppInstallStage.Enabling or AppInstallStage.Rebound) })
        {
            _announcedStage = null;
        }

        TakeUpgradeIntent();
        StateHasChanged();
    });

    private void OnIntentPosted() => _ = InvokeAsync(() =>
    {
        TakeUpgradeIntent();
        StateHasChanged();
    });

    private void TakeUpgradeIntent()
    {
        if (_flow is { Stage: AppInstallStage.Review, Mode: AppInstallMode.Upgrade } flow
            && CanInstall
            && Intents.TryTake(flow.Key.Slug, AppLifecycleVerb.Upgrade))
        {
            flow.Begin();
        }
    }

    private void NotFound()
    {
        try
        {
            Navigation.NotFound();
        }
        catch (InvalidOperationException)
        {
            // A renderer without a not-found handler: the page simply renders nothing.
        }
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();
}
