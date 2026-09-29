using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Apps.Catalogue;

/// <summary>
/// A scripted <see cref="ILatticeAppsControl"/> that records every lifecycle and
/// consent call, answers from in-memory installations, and can fail or hold any
/// call so each transition and failure can be driven deterministically.
/// </summary>
internal sealed class FakeAppsControl : ILatticeAppsControl
{
    /// <summary>The advisory flags; everything is granted unless a test restricts it.</summary>
    public LatticeAppsCapabilities Capabilities { get; set; } = new()
    {
        CanInstall = true,
        CanEnable = true,
        CanDisable = true,
        CanUninstall = true,
        CanList = true,
        CanDescribe = true,
        CanGetConsent = true,
        CanUpdateConsent = true,
    };

    /// <summary>The installed apps' descriptions, by slug.</summary>
    public Dictionary<string, AppDescriptor> Installed { get; } = new(StringComparer.Ordinal);

    /// <summary>The installed apps' recorded consents, by slug.</summary>
    public Dictionary<string, AppConsentReport> Consents { get; } = new(StringComparer.Ordinal);

    /// <summary>What <see cref="ListAsync"/> reports.</summary>
    public List<AppSummary> Listed { get; } = [];

    /// <summary>Failures to throw, by verb (install, enable, disable, uninstall, consent).</summary>
    public Dictionary<string, Exception> Failures { get; } = new(StringComparer.Ordinal);

    /// <summary>When set, <see cref="InstallAsync"/> waits for it.</summary>
    public TaskCompletionSource? InstallGate { get; set; }

    /// <summary>Every install request.</summary>
    public List<AppInstallRequest> Installs { get; } = [];

    /// <summary>Every consent update.</summary>
    public List<AppConsentUpdate> ConsentUpdates { get; } = [];

    /// <summary>Every lifecycle verb and slug, in order.</summary>
    public List<(string Verb, string Slug)> Calls { get; } = [];

    /// <summary>Records <paramref name="app"/> as installed with a consent covering exactly what it asks for.</summary>
    /// <param name="app">The installed version's description.</param>
    /// <param name="state">The lifecycle state.</param>
    /// <param name="consent">The recorded consent, or <see langword="null"/> for exactly what it asks for.</param>
    public void Install(AppDescriptor app, AppLifecycleState state, AppConsentReport? consent = null)
    {
        Installed[app.Slug] = app with { State = state };
        Consents[app.Slug] = consent ?? new AppConsentReport
        {
            Slug = app.Slug,
            Version = app.Version,
            Ceiling = new AppCapabilityCeilingDescriptor
            {
                AllowedOperations = app.Roles.Aggregate(LatticeOperation.None, (mask, role) => mask | role.Operations),
            },
            BridgeGrants = app.Ui?.Bridge ?? [],
        };
        Listed.Add(new AppSummary { Slug = app.Slug, Version = app.Version, State = state, Provenance = app.Provenance });
    }

    /// <inheritdoc />
    public async Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default)
    {
        Installs.Add(request);
        Calls.Add(("install", request.Slug));
        if (InstallGate is { } gate)
        {
            await gate.Task;
        }

        Throw("install");
        return new AppLifecycleResult { Slug = request.Slug, Version = request.Version, State = AppLifecycleState.Installed, Changed = true };
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default) => Transition("enable", appSlug, AppLifecycleState.Enabled);

    /// <inheritdoc />
    public Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default) => Transition("disable", appSlug, AppLifecycleState.Disabled);

    /// <inheritdoc />
    public Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default) => Transition("uninstall", appSlug, AppLifecycleState.Uninstalled);

    /// <inheritdoc />
    public Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(new AppCatalog { Apps = [.. Listed] });

    /// <inheritdoc />
    public Task<AppDescriptor?> DescribeAsync(string appSlug, string? version = null, CancellationToken cancellationToken = default) =>
        Task.FromResult(Installed.GetValueOrDefault(appSlug));

    /// <inheritdoc />
    public Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default) =>
        Task.FromResult(Consents.GetValueOrDefault(appSlug));

    /// <inheritdoc />
    public Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default)
    {
        ConsentUpdates.Add(request);
        Calls.Add(("consent", request.Slug));
        Throw("consent");
        var report = new AppConsentReport
        {
            Slug = request.Slug,
            Version = request.Version,
            Ceiling = request.Ceiling,
            BridgeGrants = request.BridgeGrants ?? Consents.GetValueOrDefault(request.Slug)?.BridgeGrants,
        };
        Consents[request.Slug] = report;
        return Task.FromResult(report);
    }

    /// <inheritdoc />
    public Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default) => Task.FromResult(Capabilities);

    private Task<AppLifecycleResult> Transition(string verb, string slug, AppLifecycleState state)
    {
        Calls.Add((verb, slug));
        if (Failures.TryGetValue(verb, out var failure))
        {
            return Task.FromException<AppLifecycleResult>(failure);
        }

        var version = Installed.GetValueOrDefault(slug)?.Version ?? Consents.GetValueOrDefault(slug)?.Version ?? "1.0.0";
        if (Installed.TryGetValue(slug, out var installed))
        {
            Installed[slug] = installed with { State = state };
        }

        return Task.FromResult(new AppLifecycleResult { Slug = slug, Version = version, State = state, Changed = true });
    }

    private void Throw(string verb)
    {
        if (Failures.TryGetValue(verb, out var failure))
        {
            throw failure;
        }
    }
}
