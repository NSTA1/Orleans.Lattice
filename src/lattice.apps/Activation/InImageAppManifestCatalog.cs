using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The valid manifests of the apps registered in the image, as known at configuration time.
/// Consulted by the configuration-time integrations (replication intent, per-tree options) that
/// cannot wait for an activation. Resolved lazily and once; an app whose manifest does not
/// resolve synchronously, or fails validation, is left out and never throws.
/// </summary>
internal sealed class InImageAppManifestCatalog
{
    private readonly Lazy<IReadOnlyList<AppManifest>> _manifests;

    public InImageAppManifestCatalog(IOptions<InImageAppSourceOptions> options, IAppSource source)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(source);
        _manifests = new Lazy<IReadOnlyList<AppManifest>>(
            () => Load(options.Value, source),
            LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public IReadOnlyList<AppManifest> Manifests => _manifests.Value;

    private static IReadOnlyList<AppManifest> Load(InImageAppSourceOptions options, IAppSource source)
    {
        List<AppManifest>? manifests = null;
        HashSet<AppSlug>? seen = null;
        foreach (var registration in options.Registrations)
        {
            if (registration is null || !(seen ??= []).Add(registration.Slug))
            {
                continue;
            }

            try
            {
                var pending = source.ResolveAsync(registration.Slug);
                if (!pending.IsCompletedSuccessfully)
                {
                    continue;
                }

                if (pending.Result is { IsResolved: true, Manifest: { } manifest }
                    && AppManifestValidator.Validate(manifest).IsValid)
                {
                    (manifests ??= []).Add(manifest);
                }
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                // A configuration-time consumer must never fail because one app is broken; the
                // activation pipeline reports that app's failure instead.
            }
        }

        return manifests is null ? Array.Empty<AppManifest>() : manifests;
    }
}
