using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Shell.Areas.Data;

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
    /// <param name="prefix">The key prefix.</param>
    public static string? PrefixUpperBound(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        for (var i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] < char.MaxValue)
            {
                return string.Create(i + 1, (prefix, i), static (span, state) =>
                {
                    state.prefix.AsSpan(0, state.i + 1).CopyTo(span);
                    span[state.i]++;
                });
            }
        }

        return null;
    }

    private static bool IsPlatformInternal(string id) =>
        id[0] == '_' || id.StartsWith(SystemDataPrefix, StringComparison.Ordinal);
}
