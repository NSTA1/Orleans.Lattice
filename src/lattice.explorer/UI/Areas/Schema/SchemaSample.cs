namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// A bounded sample of one tree's current values, read once when the builder
/// opens, so the shape it shows and every live check it makes are local and
/// instant. Remembered with the tenant it was read under, and only ever used
/// while the circuit still asserts that tenant.
/// </summary>
/// <param name="TreeId">The tree.</param>
/// <param name="Tenant">The tenant asserted when it was read, or <see langword="null"/> for none.</param>
/// <param name="Values">The values, in key order.</param>
/// <param name="Unread">Values skipped because their full bytes could not be read.</param>
/// <param name="More">Whether the tree holds more values than were sampled.</param>
internal sealed record SchemaSample(string TreeId, string? Tenant, IReadOnlyList<SchemaSampleValue> Values, int Unread, bool More)
{
    /// <summary>The sample of a tree with nothing in it, or none that could be read.</summary>
    /// <param name="treeId">The tree.</param>
    /// <param name="tenant">The tenant.</param>
    /// <returns>The empty sample.</returns>
    public static SchemaSample Empty(string treeId, string? tenant) => new(treeId, tenant, [], 0, false);
}
