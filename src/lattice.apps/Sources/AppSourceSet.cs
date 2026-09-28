using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;

namespace Orleans.Lattice.Apps.Sources;

/// <summary>
/// The composite of every named <see cref="IAppCatalogSource"/> a host registered, in registration order. It
/// is the single <see cref="IAppSource"/> the add-on consumes: install, activation and subscriptions resolve
/// through it, and the catalogue browses its <see cref="Sources"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Resolution never picks a source.</b> With a source key, only that source is asked. Without one, every
/// source is asked; the call succeeds only when exactly one source offers the slug (reports anything other
/// than <see cref="AppSourceStatus.NotFound"/>), and otherwise reports
/// <see cref="AppSourceStatus.Ambiguous"/> with the offering source keys, or
/// <see cref="AppSourceStatus.NotFound"/>. The inherited <see cref="IAppSource.ResolveAsync"/> is resolution
/// without a source key, so with only the in-image source registered it behaves exactly as that source does.
/// </para>
/// <para>
/// <b>Misconfiguration fails closed, never at startup.</b> Two sources sharing a key, a null source or a source
/// with no descriptor is recorded in <see cref="CompositionErrors"/> rather than thrown, because the set is
/// built during host start. Every resolution then reports <see cref="AppSourceStatus.SourceMisconfigured"/>
/// carrying those errors, so the problem surfaces at activation of each app instead of wedging the silo. A
/// resolved result whose provenance names a key other than the answering source's is refused the same way.
/// </para>
/// </remarks>
public sealed class AppSourceSet : IAppSource
{
    private readonly IAppCatalogSource[] sources;
    private readonly string[] keys;
    private readonly FrozenDictionary<string, int> byKey;
    private readonly AppManifestError[] compositionErrors;

    /// <summary>Composes the given sources, in order. Misconfiguration is recorded, never thrown.</summary>
    /// <param name="sources">The sources to compose.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <c>null</c>.</exception>
    public AppSourceSet(IEnumerable<IAppCatalogSource> sources)
    {
        ArgumentNullException.ThrowIfNull(sources);
        var composed = new List<IAppCatalogSource>();
        var composedKeys = new List<string>();
        var errors = new List<AppManifestError>();
        var seen = new Dictionary<string, int>(StringComparer.Ordinal);
        var reportedDuplicates = new HashSet<string>(StringComparer.Ordinal);
        var position = 0;
        foreach (var source in sources)
        {
            var path = $"$.sources[{position++}]";
            if (source is null)
            {
                errors.Add(new("null-source", path, "A null app source was registered."));
                continue;
            }

            if (source.Descriptor is not { } descriptor)
            {
                errors.Add(new("missing-descriptor", path + ".descriptor", "An app source has no descriptor."));
                continue;
            }

            var key = descriptor.Key;
            if (!seen.TryAdd(key, composed.Count) && reportedDuplicates.Add(key))
                errors.Add(new("duplicate-source", path + ".key", $"App source key '{key}' is registered more than once."));

            composed.Add(source);
            composedKeys.Add(key);
        }

        foreach (var duplicate in reportedDuplicates)
            seen.Remove(duplicate);

        this.sources = [.. composed];
        keys = [.. composedKeys];
        byKey = seen.ToFrozenDictionary(StringComparer.Ordinal);
        compositionErrors = [.. errors];
        CompositionErrors = Array.AsReadOnly(compositionErrors);
        Sources = Array.AsReadOnly(this.sources);
    }

    /// <summary>Every composed source, in registration order, including any that share a key.</summary>
    public IReadOnlyList<IAppCatalogSource> Sources { get; }

    /// <summary>The composition diagnostics; empty when the set is well formed.</summary>
    public IReadOnlyList<AppManifestError> CompositionErrors { get; }

    /// <summary>Whether the set is well formed and therefore resolves.</summary>
    public bool IsValid => compositionErrors.Length == 0;

    /// <summary>
    /// Finds the source with <paramref name="key"/>. A key shared by more than one source matches none of them,
    /// because the set cannot tell which one is meant.
    /// </summary>
    /// <param name="key">The source key.</param>
    /// <param name="source">The source, when found.</param>
    /// <returns>Whether exactly one composed source has the key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <c>null</c>.</exception>
    public bool TryGet(string key, [NotNullWhen(true)] out IAppCatalogSource? source)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (byKey.TryGetValue(key, out var index))
        {
            source = sources[index];
            return true;
        }

        source = null;
        return false;
    }

    /// <summary>
    /// Resolves an app without activating it, from the named source or, when <paramref name="sourceKey"/> is
    /// null, from the one source that offers it.
    /// </summary>
    /// <param name="slug">The app slug to resolve.</param>
    /// <param name="version">The exact version required, or null for the version present.</param>
    /// <param name="sourceKey">The source to resolve from, or null to require exactly one offering source.</param>
    /// <param name="cancellationToken">Cancels a source that performs asynchronous work.</param>
    /// <returns>
    /// A structured outcome; <see cref="AppSourceStatus.Ambiguous"/> when several sources offer the slug and no
    /// key was named, and <see cref="AppSourceStatus.SourceMisconfigured"/> when the set is misconfigured.
    /// </returns>
    public ValueTask<AppSourceResult> ResolveAsync(
        AppSlug slug,
        AppVersion? version = null,
        string? sourceKey = null,
        CancellationToken cancellationToken = default)
    {
        if (compositionErrors.Length > 0)
            return new(AppSourceResult.SourceMisconfigured(slug, compositionErrors));

        if (sourceKey is not null)
        {
            return byKey.TryGetValue(sourceKey, out var index)
                ? Checked(index, sources[index].ResolveAsync(slug, version, cancellationToken))
                : new(AppSourceResult.UnknownSource(slug, sourceKey));
        }

        return sources.Length switch
        {
            0 => new(AppSourceResult.NotFound(slug)),
            1 => Checked(0, sources[0].ResolveAsync(slug, version, cancellationToken)),
            _ => ResolveAcrossAsync(slug, version, cancellationToken),
        };
    }

    /// <inheritdoc />
    ValueTask<AppSourceResult> IAppSource.ResolveAsync(AppSlug slug, AppVersion? version, CancellationToken cancellationToken) =>
        ResolveAsync(slug, version, null, cancellationToken);

    private async ValueTask<AppSourceResult> ResolveAcrossAsync(AppSlug slug, AppVersion? version, CancellationToken cancellationToken)
    {
        AppSourceResult? offered = null;
        var offeredIndex = -1;
        List<string>? offering = null;
        for (var i = 0; i < sources.Length; i++)
        {
            var result = await sources[i].ResolveAsync(slug, version, cancellationToken).ConfigureAwait(false);
            if (result.Status == AppSourceStatus.NotFound)
                continue;
            if (offered is null)
            {
                offered = result;
                offeredIndex = i;
                continue;
            }

            offering ??= [keys[offeredIndex]];
            offering.Add(keys[i]);
        }

        if (offering is not null)
            return AppSourceResult.Ambiguous(slug, offering);

        return offered is null ? AppSourceResult.NotFound(slug) : Check(offeredIndex, offered);
    }

    private ValueTask<AppSourceResult> Checked(int index, ValueTask<AppSourceResult> pending) =>
        pending.IsCompletedSuccessfully ? new(Check(index, pending.Result)) : CheckedAsync(index, pending);

    private async ValueTask<AppSourceResult> CheckedAsync(int index, ValueTask<AppSourceResult> pending) =>
        Check(index, await pending.ConfigureAwait(false));

    private AppSourceResult Check(int index, AppSourceResult result)
    {
        if (!result.IsResolved || string.Equals(result.Provenance!.Source, keys[index], StringComparison.Ordinal))
            return result;

        return AppSourceResult.SourceMisconfigured(result.Slug,
        [
            new("provenance-mismatch", "$.provenance.source",
                $"App source '{keys[index]}' vouched for source key '{result.Provenance.Source}' instead of its own."),
        ]);
    }
}
