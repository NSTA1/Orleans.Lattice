namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>One schema family's registered versions and the hops between them.</summary>
/// <param name="SchemaId">The family number.</param>
/// <param name="IsConfigured">Whether the tree stamps new writes with this family.</param>
/// <param name="Versions">Every version the catalog names, in ascending order, plus the target when it is in this family.</param>
/// <param name="Hops">The registered hops, by the version they upgrade from.</param>
internal sealed record SchemaVersionFamily(uint SchemaId, bool IsConfigured, IReadOnlyList<SchemaVersionRow> Versions, IReadOnlyList<SchemaVersionHop> Hops);
