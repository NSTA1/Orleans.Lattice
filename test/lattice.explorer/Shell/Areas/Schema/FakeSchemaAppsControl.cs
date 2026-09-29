using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Schema;

/// <summary>
/// The two <see cref="ILatticeAppsControl"/> reads the Schema area makes - the
/// installed apps and each app's description - scripted; every other verb is not
/// the Schema area's business and throws.
/// </summary>
internal sealed class FakeSchemaAppsControl : ILatticeAppsControl
{
    /// <summary>The installed apps, with the schema declarations each describes.</summary>
    public List<(string Slug, string Version, ImmutableArray<AppSchemaDescriptor> Schema)> Apps { get; } = [];

    /// <summary>When set, listing the apps throws it.</summary>
    public Exception? ListFailure { get; set; }

    /// <summary>Adds an installed app declaring schema for <paramref name="trees"/>.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="version">The installed version.</param>
    /// <param name="trees">The app-local tree names it declares schema for.</param>
    public void Add(string slug, string version, params string[] trees) =>
        Apps.Add((slug, version, [.. trees.Select(tree => new AppSchemaDescriptor { Tree = tree, Family = tree + "-family", Version = 2, StrictIngest = true })]));

    /// <inheritdoc />
    public Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default)
    {
        if (ListFailure is not null)
        {
            return Task.FromException<AppCatalog>(ListFailure);
        }

        return Task.FromResult(new AppCatalog
        {
            Apps = [.. Apps.Select(app => new AppSummary
            {
                Slug = app.Slug,
                Version = app.Version,
                State = AppLifecycleState.Enabled,
                Provenance = Provenance(),
            })],
        });
    }

    /// <inheritdoc />
    public Task<AppDescriptor?> DescribeAsync(string appSlug, string? version = null, CancellationToken cancellationToken = default)
    {
        var app = Apps.FirstOrDefault(candidate => candidate.Slug == appSlug);
        return Task.FromResult<AppDescriptor?>(app.Slug is null
            ? null
            : new AppDescriptor
            {
                Slug = app.Slug,
                Version = app.Version,
                Provenance = Provenance(),
                State = AppLifecycleState.Enabled,
                Schema = app.Schema,
            });
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    private static AppProvenanceDescriptor Provenance() => new() { Source = "in-image", Publisher = "Contoso" };
}
