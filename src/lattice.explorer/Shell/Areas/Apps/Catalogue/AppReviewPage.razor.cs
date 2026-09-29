using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.Catalogue;

/// <summary>
/// The pre-install review of one app version from a named source, and the staged
/// install, upgrade or re-consent it starts. Shown only to an <c>AppInstall</c>
/// holder; anyone else gets not-found.
/// </summary>
public partial class AppReviewPage : IDisposable
{
    private readonly CancellationTokenSource _lifetime = new();
    private AppInstallFlow? _flow;
    private ExplorerAddress? _resolvedFor;
    private string? _slug;
    private bool _announcedInstall;

    [Inject]
    internal AppsAccess Access { get; set; } = default!;

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

    private string SourceName => _flow?.Source?.DisplayName ?? _flow?.Key.SourceKey ?? string.Empty;

    private string TenantPhrase => Address.Tenant is { } tenant ? $" in tenant {tenant}" : string.Empty;

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
        _lifetime.Cancel();
        _lifetime.Dispose();
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

        Attach(Flows.GetOrCreate(new AppInstallFlowKey(Address.Tenant, path[1], slug, version), source));
    }

    private void Attach(AppInstallFlow flow)
    {
        _flow = flow;
        _announcedInstall = flow.Stage is AppInstallStage.Installed or AppInstallStage.Enabled;
        flow.Changed += OnFlowChanged;
        _ = flow.LoadAsync();
        TakeUpgradeIntent();
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
        if (_flow is { Stage: AppInstallStage.Installed or AppInstallStage.Enabled } && !_announcedInstall)
        {
            _announcedInstall = true;
            Access.Invalidate();
            Directory.Invalidate();
        }
        else if (_flow is { Stage: not (AppInstallStage.Installed or AppInstallStage.Enabled or AppInstallStage.Enabling) })
        {
            _announcedInstall = false;
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
