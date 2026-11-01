using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Core.Catalog;

/// <summary>
/// Which catalogue entries are physical shadows a logical tree hides behind its alias,
/// rather than trees in their own right.
/// </summary>
/// <remarks>
/// A resize or a schema remediation copies a logical tree into a physical tree named
/// beneath it (<c>{tree}/resized/{operation}</c>, <c>{tree}/remediated/{suffix}</c>) and
/// points the logical tree's alias at the copy; a shadow-cutover restore stamps its copy
/// with <see cref="TreeCatalogEntry.RestoreShadowOfTreeId"/>. Those copies are shadows. A
/// tree an operator aliased by hand (a second name, such as <c>orders</c> for
/// <c>factory-orders</c>) is not: it was created as itself and stays listed.
/// </remarks>
internal static class TreeCatalogShadows
{
    /// <summary>The ids of the entries in <paramref name="entries"/> that are physical shadows.</summary>
    /// <param name="entries">The raw catalogue.</param>
    /// <returns>The shadow tree ids.</returns>
    public static HashSet<string> Of(IEnumerable<TreeCatalogEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var shadows = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in entries)
        {
            if (entry.RestoreShadowOfTreeId is not null)
            {
                shadows.Add(entry.TreeId);
            }
            else if (entry.IsAlias && entry.PhysicalTreeId is { } physical && IsDerivedCopy(entry.TreeId, physical))
            {
                shadows.Add(physical);
            }
        }

        return shadows;
    }

    /// <summary>Whether <paramref name="physicalTreeId"/> is a copy derived from <paramref name="logicalTreeId"/>, named beneath it.</summary>
    /// <param name="logicalTreeId">The aliasing tree.</param>
    /// <param name="physicalTreeId">The tree its alias resolves to.</param>
    public static bool IsDerivedCopy(string logicalTreeId, string physicalTreeId) =>
        physicalTreeId.Length > logicalTreeId.Length + 1
        && physicalTreeId[logicalTreeId.Length] == '/'
        && physicalTreeId.StartsWith(logicalTreeId, StringComparison.Ordinal);
}
