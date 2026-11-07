namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// What a fenced range-delete walk carries to a shard root (epic #5605, member 2):
/// the intent it belongs to and the tree range-delete generation that intent handed
/// out. The shard root trusts the generation and stamps the walk's leaf appends with
/// it; only a rejection makes it ask the fence whether its own intent is still open.
/// </summary>
/// <param name="IntentId">The range delete's intent id at the tree's range-delete fence.</param>
/// <param name="Generation">The generation the intent handed out; at least 1.</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.RangeDeleteFenceTicket)]
internal readonly record struct RangeDeleteFenceTicket(
    [property: Id(0)] Guid IntentId,
    [property: Id(1)] long Generation);
