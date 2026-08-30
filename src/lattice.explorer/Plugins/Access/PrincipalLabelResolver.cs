using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// Resolves principal (user or group) ids to friendly directory display names for
/// the Access area, mirroring the subject picker's display-name-primary,
/// id-on-hover convention. Each resolved id is cached for the resolver's lifetime
/// (so repeated renders and repeated members do not re-hit the directory), and
/// every lookup falls back to the raw id when no directory is configured, the id
/// does not resolve, or its display name is blank. The resolver never throws on a
/// directory failure and never enumerates the tenant: it resolves ids one at a
/// time, exactly the ones in view. Register it per panel so its cache is scoped to
/// a single Access view.
/// </summary>
public sealed class PrincipalLabelResolver
{
    private readonly IMembershipAdminService _membership;
    private readonly Dictionary<string, string> _cache = new(StringComparer.Ordinal);

    /// <summary>Creates a resolver over the membership admin service it resolves through.</summary>
    /// <param name="membership">The membership admin service. Must not be <see langword="null"/>.</param>
    public PrincipalLabelResolver(IMembershipAdminService membership)
    {
        ArgumentNullException.ThrowIfNull(membership);
        _membership = membership;
    }

    /// <summary>
    /// Returns the cached friendly label for <paramref name="id"/>, or the id
    /// itself when it has not yet been resolved. Allocation-free on a cache hit and
    /// never issues a directory call, so a render path can call it inline; call
    /// <see cref="ResolveLabelAsync"/> (or <see cref="ResolveManyAsync"/>) on data
    /// load to warm the cache so the label upgrades from id to name on the next
    /// render.
    /// </summary>
    /// <param name="id">The principal id to label. Must not be <see langword="null"/>.</param>
    /// <returns>The cached display name, or <paramref name="id"/> when not yet resolved.</returns>
    public string Label(string id)
    {
        ArgumentNullException.ThrowIfNull(id);
        return _cache.TryGetValue(id, out var label) ? label : id;
    }

    /// <summary>
    /// Resolves <paramref name="id"/> to its friendly directory display name,
    /// caching the result for the resolver's lifetime and falling back to the raw
    /// id when no directory is configured, the id does not resolve, or its display
    /// name is blank. Never throws: a directory fault or cancellation folds into the
    /// id fallback (and is left uncached so a later render can retry).
    /// </summary>
    /// <param name="id">The principal id to resolve. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The friendly display name, or <paramref name="id"/> on any fallback.</returns>
    public async Task<string> ResolveLabelAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);
        if (_cache.TryGetValue(id, out var cached))
        {
            return cached;
        }

        if (id.Length == 0)
        {
            // An empty id resolves to itself and is never queried against the
            // directory; cache it so the sync peek is stable.
            _cache[id] = id;
            return id;
        }

        try
        {
            var descriptor = await _membership
                .ResolveDirectoryPrincipalAsync(id, cancellationToken)
                .ConfigureAwait(false);
            var label = descriptor is not null && !string.IsNullOrWhiteSpace(descriptor.DisplayName)
                ? descriptor.DisplayName
                : id;
            _cache[id] = label;
            return label;
        }
        catch
        {
            // The service is documented to fold failures into a null descriptor, but
            // stay defensive: never let a directory fault (or a cancellation) escape
            // into a render. Leave the id uncached so a later render can retry.
            return id;
        }
    }

    /// <summary>
    /// Resolves every not-yet-cached id in <paramref name="ids"/> so a single data
    /// load warms the cache for every principal about to be rendered. Already-cached
    /// and <see langword="null"/> ids are skipped, and no id is resolved more than
    /// once. Never throws.
    /// </summary>
    /// <param name="ids">The principal ids in view. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task ResolveManyAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(ids);
        foreach (var id in ids)
        {
            if (id is null || _cache.ContainsKey(id))
            {
                continue;
            }

            await ResolveLabelAsync(id, cancellationToken).ConfigureAwait(false);
        }
    }
}
