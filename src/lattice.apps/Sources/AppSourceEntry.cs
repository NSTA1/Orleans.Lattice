namespace Orleans.Lattice.Apps.Sources;

/// <summary>
/// One app a source offers, as listed by <see cref="IAppCatalogSource.ListAsync"/>: its slug, the versions
/// available (newest first), and the manifest and provenance of the newest version. An entry is built
/// without activating or loading any app code.
/// </summary>
/// <remarks>
/// An app the source holds but cannot describe (for example an in-image registration whose manifest does
/// not parse, or a slug registered twice) is still listed, as an unavailable entry that carries
/// <see cref="Errors"/> instead of a manifest, so the catalogue shows what is broken rather than hiding it.
/// </remarks>
public sealed class AppSourceEntry
{
    private AppSourceEntry(
        AppSlug slug,
        IReadOnlyList<AppVersion> versions,
        AppManifest? manifest,
        AppProvenance? provenance,
        IReadOnlyList<AppManifestError> errors)
    {
        Slug = slug;
        Versions = versions;
        Manifest = manifest;
        Provenance = provenance;
        Errors = errors;
    }

    /// <summary>The slug the entry offers.</summary>
    public AppSlug Slug { get; }

    /// <summary>Whether the entry describes an installable app; when false, <see cref="Errors"/> explains why not.</summary>
    public bool IsAvailable => Manifest is not null;

    /// <summary>The versions the source offers, newest first; empty for an unavailable entry.</summary>
    public IReadOnlyList<AppVersion> Versions { get; }

    /// <summary>The validated manifest of the newest version, or null for an unavailable entry.</summary>
    public AppManifest? Manifest { get; }

    /// <summary>The provenance the source vouches for the newest version, or null for an unavailable entry.</summary>
    public AppProvenance? Provenance { get; }

    /// <summary>Read-only diagnostics; empty for an available entry and non-empty otherwise.</summary>
    public IReadOnlyList<AppManifestError> Errors { get; }

    /// <summary>Creates an entry for an installable app.</summary>
    /// <param name="versions">The available versions, newest first; copied. The first must be the manifest's version.</param>
    /// <param name="manifest">The validated manifest of the newest version.</param>
    /// <param name="provenance">The provenance the source vouches for.</param>
    /// <exception cref="ArgumentNullException">An argument, or the manifest's identity, is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="versions"/> is empty, carries an uninitialised or repeated version, or does not start with
    /// the manifest's version.
    /// </exception>
    public static AppSourceEntry Available(IReadOnlyList<AppVersion> versions, AppManifest manifest, AppProvenance provenance)
    {
        ArgumentNullException.ThrowIfNull(versions);
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(manifest.Identity);
        ArgumentNullException.ThrowIfNull(provenance);
        if (versions.Count == 0)
            throw new ArgumentException("An available entry requires at least one version.", nameof(versions));

        var copy = new AppVersion[versions.Count];
        for (var i = 0; i < copy.Length; i++)
        {
            var version = versions[i];
            if (version.Value is null)
                throw new ArgumentException("Versions cannot contain an uninitialised value.", nameof(versions));
            for (var j = 0; j < i; j++)
                if (copy[j] == version)
                    throw new ArgumentException($"Version '{version}' is listed more than once.", nameof(versions));
            copy[i] = version;
        }

        if (copy[0] != manifest.Identity.Version)
            throw new ArgumentException("The newest version must be the manifest's version.", nameof(versions));

        return new(manifest.Identity.Slug, copy, manifest, provenance, []);
    }

    /// <summary>Creates an entry for an app the source holds but cannot describe.</summary>
    /// <param name="slug">The slug the source holds.</param>
    /// <param name="errors">The non-empty diagnostics explaining why; copied.</param>
    /// <exception cref="ArgumentNullException"><paramref name="errors"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="slug"/> is uninitialised, or <paramref name="errors"/> is empty or contains null.
    /// </exception>
    public static AppSourceEntry Unavailable(AppSlug slug, IReadOnlyList<AppManifestError> errors)
    {
        ArgumentNullException.ThrowIfNull(errors);
        if (slug.Value is null)
            throw new ArgumentException("A parsed app slug is required.", nameof(slug));
        if (errors.Count == 0)
            throw new ArgumentException("An unavailable entry requires at least one error.", nameof(errors));
        var copy = new AppManifestError[errors.Count];
        for (var i = 0; i < copy.Length; i++)
            copy[i] = errors[i] ?? throw new ArgumentException("Errors cannot contain null.", nameof(errors));
        return new(slug, [], null, null, copy);
    }
}
