using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Resolves an app through <see cref="IAppSource"/> from the source an install came from, so a second
/// source offering the same slug can never make an installed app ambiguous or substitute its manifest.
/// </summary>
/// <remarks>
/// When the registered source is the composed <see cref="AppSourceSet"/>, a named key asks only that source;
/// an unknown key reports <see cref="AppSourceStatus.NotFound"/> rather than falling back to any other source
/// (fail closed). A host-supplied <see cref="IAppSource"/> that is not a set has no notion of named sources,
/// so it is asked exactly as before.
/// </remarks>
internal static class AppSourceResolution
{
    /// <summary>Resolves an app from the named source, or across every source when the key is null.</summary>
    /// <param name="source">The registered app source.</param>
    /// <param name="slug">The app slug.</param>
    /// <param name="version">The exact version, or null for the version present.</param>
    /// <param name="sourceKey">The source key to resolve from, or null to require exactly one offering source.</param>
    /// <param name="cancellationToken">Cancels a source that performs asynchronous work.</param>
    /// <returns>The structured source outcome.</returns>
    public static ValueTask<AppSourceResult> ResolveFromAsync(
        this IAppSource source,
        AppSlug slug,
        AppVersion? version,
        string? sourceKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source is AppSourceSet set
            ? set.ResolveAsync(slug, version, sourceKey, cancellationToken)
            : source.ResolveAsync(slug, version, cancellationToken);
    }

    /// <summary>
    /// Resolves the installed version of <paramref name="record"/> from the source its provenance names.
    /// </summary>
    /// <param name="source">The registered app source.</param>
    /// <param name="record">The install record whose version and provenance source key are used.</param>
    /// <param name="cancellationToken">Cancels a source that performs asynchronous work.</param>
    /// <returns>The structured source outcome.</returns>
    public static ValueTask<AppSourceResult> ResolveInstalledAsync(
        this IAppSource source,
        AppRegistryRecord record,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        return source.ResolveFromAsync(record.Slug, record.Version, record.Provenance?.Source, cancellationToken);
    }
}
