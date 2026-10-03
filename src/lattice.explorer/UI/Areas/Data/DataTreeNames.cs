using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Turns the ids the state API answers with into the logical ids the Data area
/// shows. A tenant-owned tree is stored as <c>t/{tenant}/{name}</c>; with tenancy
/// on its logical id is <c>{name}</c> and its tenant becomes the address's root
/// node, so the composed id never reaches the page. Platform-internal trees
/// (reserved <c>_</c>-prefixed and <c>sys-</c> ids) are never shown at all.
/// </summary>
internal static class DataTreeNames
{
    /// <summary>The prefix of an app-owned tree's logical id.</summary>
    public const string AppPrefix = "a/";

    /// <summary>The prefix the state API gives a view's logical tree.</summary>
    public const string ViewPrefix = "view-";

    private const string TenantPrefix = ExplorerTenantTrees.SegmentPrefix;

    private const string SystemDataPrefix = "sys-";

    /// <summary>
    /// Describes <paramref name="stateId"/> for display. Returns <see langword="false"/>
    /// for an id the Data area never shows: empty, platform-internal, or a
    /// malformed tenant-composed id.
    /// </summary>
    /// <param name="stateId">The id the state API answered with.</param>
    /// <param name="tenancyActive">Whether the Explorer is tenant-scoped.</param>
    /// <param name="logicalId">The logical id to show and address.</param>
    /// <param name="tenant">The owning tenant when tenancy is on, else <see langword="null"/>.</param>
    public static bool TryDescribe(string? stateId, bool tenancyActive, out string logicalId, out string? tenant)
    {
        logicalId = string.Empty;
        tenant = null;
        if (string.IsNullOrEmpty(stateId) || IsPlatformInternal(stateId))
        {
            return false;
        }

        if (stateId.StartsWith(TenantPrefix, StringComparison.Ordinal))
        {
            var rest = stateId.AsSpan(TenantPrefix.Length);
            var slash = rest.IndexOf('/');
            if (slash <= 0 || slash >= rest.Length - 1)
            {
                return false;
            }

            var name = rest[(slash + 1)..].ToString();
            if (IsPlatformInternal(name))
            {
                return false;
            }

            logicalId = name;
            tenant = tenancyActive ? rest[..slash].ToString() : null;
            return true;
        }

        logicalId = stateId;
        tenant = tenancyActive ? ExplorerTenantTrees.DefaultTenantId : null;
        return true;
    }

    /// <summary>The owning app's slug for an <c>a/{slug}/{tree}</c> id, else <see langword="null"/>.</summary>
    /// <param name="logicalId">The logical tree id.</param>
    public static string? AppSlugOf(string logicalId)
    {
        ArgumentNullException.ThrowIfNull(logicalId);
        if (!logicalId.StartsWith(AppPrefix, StringComparison.Ordinal))
        {
            return null;
        }

        var slash = logicalId.IndexOf('/', AppPrefix.Length);
        return slash > AppPrefix.Length && slash < logicalId.Length - 1
            ? logicalId[AppPrefix.Length..slash]
            : null;
    }

    /// <summary>
    /// The exclusive upper bound of every key that starts with <paramref name="prefix"/>,
    /// or <see langword="null"/> when the prefix has no finite bound.
    /// </summary>
    /// <remarks>
    /// The bound is well-formed text whenever the prefix is: the state API carries a
    /// string as UTF-8, which replaces a lone surrogate with U+FFFD, so a bound that
    /// split a surrogate pair would arrive far above the prefix and admit keys outside
    /// it. A last character of U+D7FF moves to U+10000, the next text in code-unit
    /// order, and a last character outside the Basic Multilingual Plane moves to the
    /// next code point, or to U+E000 after U+10FFFF.
    /// </remarks>
    /// <param name="prefix">The key prefix.</param>
    public static string? PrefixUpperBound(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        for (var i = prefix.Length - 1; i >= 0; i--)
        {
            var last = prefix[i];
            if (last == char.MaxValue)
            {
                continue;
            }

            if (char.IsLowSurrogate(last) && i > 0 && char.IsHighSurrogate(prefix[i - 1]))
            {
                var high = prefix[i - 1];
                return last < '\uDFFF'
                    ? Bound(prefix, i, (char)(last + 1))
                    : high < '\uDBFF'
                        ? Bound(prefix, i - 1, (char)(high + 1), '\uDC00')
                        : Bound(prefix, i - 1, '\uE000');
            }

            return last == '\uD7FF'
                ? Bound(prefix, i, '\uD800', '\uDC00')
                : Bound(prefix, i, (char)(last + 1));
        }

        return null;
    }

    private static string Bound(string prefix, int keep, char first, char? second = null) =>
        string.Create(keep + (second is null ? 1 : 2), (prefix, keep, first, second), static (span, state) =>
        {
            state.prefix.AsSpan(0, state.keep).CopyTo(span);
            span[state.keep] = state.first;
            if (state.second is { } next)
            {
                span[state.keep + 1] = next;
            }
        });

    private static bool IsPlatformInternal(string id) =>
        id[0] == '_' || id.StartsWith(SystemDataPrefix, StringComparison.Ordinal);
}
