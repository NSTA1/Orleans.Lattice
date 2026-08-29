namespace Orleans.Lattice.Explorer.Schema.Domain;

/// <summary>
/// One governable tree as the Schema plugin sees it: the id it addresses the
/// control plane with, the label it renders, and the two badges the selection
/// list shows inline.
/// <para>
/// This is the plugin's own projection, not the Explorer's catalog record. The
/// plugin declares a single domain contract and receives nothing else, so the
/// shape it renders is stated in its own source rather than inherited from a
/// shared navigation type it does not own.
/// </para>
/// </summary>
/// <param name="Id">The opaque tree id used for every schema control call.</param>
/// <param name="Label">The human-readable label the selection list renders.</param>
/// <param name="Lifecycle">The tree's lifecycle state, or <see langword="null"/> when the catalog reported none.</param>
/// <param name="ShardCount">The tree's configured shard count, or <see langword="null"/> when the catalog reported none.</param>
public readonly record struct SchemaTreeSummary(
    string Id,
    string Label,
    string? Lifecycle,
    int? ShardCount);
