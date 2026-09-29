namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>One sampled value: its key and its body bytes (a versioned envelope already stripped).</summary>
/// <param name="Key">The key.</param>
/// <param name="Value">The value bytes, as a policy judges them.</param>
internal sealed record SchemaSampleValue(string Key, byte[] Value);
