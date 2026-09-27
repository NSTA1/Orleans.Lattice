using System.Collections.Frozen;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The in-image <see cref="IAppSource"/>: resolves apps compiled into the image by ordinary package
/// reference and declared through <see cref="InImageAppSourceOptions"/>. It performs no assembly
/// scanning, assembly loading, download or NuGet protocol, and reads only each registration's named
/// embedded manifest resource.
/// </summary>
/// <remarks>
/// <para>
/// Each registration's manifest is read, parsed and validated at most once, lazily on first resolution
/// and thread-safely, and the outcome is cached. Repeated resolution of a registered slug at the version
/// present therefore returns the same cached <see cref="AppSourceResult"/> synchronously, without
/// allocation. Construction reads no resources, so an invalid manifest cannot fail host start.
/// </para>
/// <para>
/// Provenance is always <see cref="SourceKey"/> with the registration's
/// <see cref="InImageAppRegistration.Publisher"/> and a reference of <c>embedded:{resource name}</c>.
/// The in-image source holds exactly one version per slug; a registered slug requested at any other
/// version yields <see cref="AppSourceStatus.VersionMismatch"/>.
/// </para>
/// </remarks>
public sealed class InImageAppSource : IAppSource
{
    /// <summary>The provenance source key reported for every in-image app.</summary>
    public const string SourceKey = "in-image";

    private readonly FrozenDictionary<AppSlug, Lazy<AppSourceResult>> entries;

    /// <summary>Creates the source from the declared registrations, which are read once.</summary>
    /// <param name="options">The in-image registrations.</param>
    public InImageAppSource(IOptions<InImageAppSourceOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Value);
        var registrations = options.Value.Registrations;
        var map = new Dictionary<AppSlug, Lazy<AppSourceResult>>(registrations.Count);
        foreach (var registration in registrations)
        {
            if (registration is null)
                continue;
            var slug = registration.Slug;
            map[slug] = map.ContainsKey(slug)
                ? new Lazy<AppSourceResult>(AppSourceResult.DuplicateRegistration(slug))
                : new Lazy<AppSourceResult>(() => Load(registration), LazyThreadSafetyMode.ExecutionAndPublication);
        }

        entries = map.ToFrozenDictionary();
    }

    /// <inheritdoc />
    public ValueTask<AppSourceResult> ResolveAsync(
        AppSlug slug,
        AppVersion? version = null,
        CancellationToken cancellationToken = default)
    {
        if (!entries.TryGetValue(slug, out var entry))
            return new(AppSourceResult.NotFound(slug));

        var result = entry.Value;
        if (version is { } requested && result.Manifest is { } manifest && manifest.Identity.Version != requested)
            return new(AppSourceResult.VersionMismatch(slug, requested, manifest.Identity.Version));

        return new(result);
    }

    private static AppSourceResult Load(InImageAppRegistration registration)
    {
        AppManifestResult loaded;
        try
        {
            loaded = AppManifestResources.Load(registration.Assembly, registration.ManifestResourceName);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            // A resource provider may throw beyond the IOException the loader already maps; keep it structured.
            return AppSourceResult.InvalidManifest(registration.Slug, [new("resource", "$", exception.Message)]);
        }

        if (loaded.Manifest is not { } manifest)
        {
            return loaded.Errors.Count > 0
                ? AppSourceResult.InvalidManifest(registration.Slug, loaded.Errors)
                : AppSourceResult.InvalidManifest(registration.Slug, [new("required", "$", "No manifest was produced.")]);
        }

        if (manifest.Identity.Slug != registration.Slug)
            return AppSourceResult.IdentityMismatch(registration.Slug, manifest.Identity.Slug);

        var provenance = new AppProvenance
        {
            Source = SourceKey,
            Publisher = registration.Publisher,
            Reference = "embedded:" + registration.ManifestResourceName,
        };
        return AppSourceResult.Resolved(
            manifest,
            provenance,
            new InImageAppActivationHandle(manifest.Identity, registration.Assembly));
    }
}
