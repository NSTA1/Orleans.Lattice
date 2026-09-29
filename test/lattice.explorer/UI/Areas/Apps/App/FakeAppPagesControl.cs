using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// A scriptable <see cref="ILatticeAppsControl"/> for the app pages: the capability probe,
/// the administrative descriptions and the consent an <c>AppInstall</c> holder reads. A
/// caller without <c>AppInstall</c> is modelled as the real facade treats one: the probe
/// says no and every read is refused. Lifecycle verbs are never called by the pages.
/// </summary>
internal sealed class FakeAppPagesControl : ILatticeAppsControl
{
    /// <summary>What the capability probe reports.</summary>
    public LatticeAppsCapabilities Capabilities { get; set; } = new();

    /// <summary>The administrative descriptions, by slug.</summary>
    public Dictionary<string, AppDescriptor> Descriptions { get; } = new(StringComparer.Ordinal);

    /// <summary>The effective consent, by slug.</summary>
    public Dictionary<string, AppConsentReport> Consents { get; } = new(StringComparer.Ordinal);

    /// <summary>When set, the capability probe throws it.</summary>
    public Exception? ProbeThrows { get; set; }

    /// <summary>When set, <see cref="DescribeAsync"/> throws it.</summary>
    public Exception? DescribeThrows { get; set; }

    /// <summary>When set, <see cref="GetConsentAsync"/> throws it.</summary>
    public Exception? ConsentThrows { get; set; }

    /// <summary>The number of <see cref="DescribeAsync"/> calls.</summary>
    public int DescribeCalls { get; private set; }

    /// <summary>The number of <see cref="GetConsentAsync"/> calls.</summary>
    public int ConsentCalls { get; private set; }

    /// <summary>Makes the caller an <c>AppInstall</c> holder who may describe and read consent.</summary>
    /// <param name="descriptor">An installed app's administrative description.</param>
    /// <param name="consent">Its effective consent, or <see langword="null"/> for none recorded.</param>
    /// <returns>This control.</returns>
    public FakeAppPagesControl Administer(AppDescriptor descriptor, AppConsentReport? consent)
    {
        Capabilities = new LatticeAppsCapabilities { CanDescribe = true, CanGetConsent = true, CanList = true };
        Descriptions[descriptor.Slug] = descriptor;
        if (consent is not null)
        {
            Consents[descriptor.Slug] = consent;
        }

        return this;
    }

    /// <inheritdoc />
    public Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default) =>
        ProbeThrows is { } exception ? Task.FromException<LatticeAppsCapabilities>(exception) : Task.FromResult(Capabilities);

    /// <inheritdoc />
    public Task<AppDescriptor?> DescribeAsync(string appSlug, string? version = null, CancellationToken cancellationToken = default)
    {
        DescribeCalls++;
        if (DescribeThrows is { } exception)
        {
            return Task.FromException<AppDescriptor?>(exception);
        }

        if (!Capabilities.CanDescribe)
        {
            return Task.FromException<AppDescriptor?>(new UnauthorizedAccessException("AppInstall is required."));
        }

        return Task.FromResult<AppDescriptor?>(Descriptions.GetValueOrDefault(appSlug)
            ?? new AppDescriptor
            {
                Slug = appSlug,
                Version = "0.0.0",
                State = AppLifecycleState.NotInstalled,
                Provenance = new AppProvenanceDescriptor { Source = "none", Publisher = "none" },
            });
    }

    /// <inheritdoc />
    public Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ConsentCalls++;
        return ConsentThrows is { } exception
            ? Task.FromException<AppConsentReport?>(exception)
            : Task.FromResult(Consents.GetValueOrDefault(appSlug));
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default) => Never<AppLifecycleResult>();

    /// <inheritdoc />
    public Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default) => Never<AppLifecycleResult>();

    /// <inheritdoc />
    public Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default) => Never<AppLifecycleResult>();

    /// <inheritdoc />
    public Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default) => Never<AppLifecycleResult>();

    /// <inheritdoc />
    public Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default) => Never<AppCatalog>();

    /// <inheritdoc />
    public Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default) => Never<AppConsentReport>();

    private static Task<T> Never<T>() =>
        throw new NotSupportedException("The app pages are read-only; lifecycle and consent changes belong to the catalogue.");
}
