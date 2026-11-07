namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// One open range-delete intent on a tree's range-delete fence (epic #5605, member
/// 2; design note section 2, "Range delete: fenced at the tree"): the operation, the
/// range it deletes, the deadline after which its reminder closes it, and the shard
/// roots whose markers it opened.
/// </summary>
/// <remarks>
/// Part of <see cref="TreeRangeDeleteFenceState"/>; every member is a public property
/// of a concrete type so the JSON grain-storage serializer round trips it too (#5579).
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.TreeRangeDeleteIntent)]
internal sealed class TreeRangeDeleteIntent
{
    /// <summary>The range delete's operation id.</summary>
    [Id(0)]
    public Guid IntentId { get; set; }

    /// <summary>The inclusive start of the deleted range.</summary>
    [Id(1)]
    public string StartInclusive { get; set; } = string.Empty;

    /// <summary>The exclusive end of the deleted range.</summary>
    [Id(2)]
    public string EndExclusive { get; set; } = string.Empty;

    /// <summary>
    /// The UTC ticks after which the fence's reminder closes the intent: the
    /// call's response timeout less a margin, taken once when the call started
    /// (the <c>LatticeGrain.SagaDecisionDeadline</c> form). It affects liveness only.
    /// </summary>
    [Id(3)]
    public long DeadlineUtcTicks { get; set; }

    /// <summary>The physical shard indices whose markers the intent opened.</summary>
    [Id(4)]
    public int[] Shards { get; set; } = [];

    /// <summary>
    /// Set durably in the turn that begins the intent's close, before its first
    /// re-claim (rule 1): from then on the open check answers "not open".
    /// </summary>
    [Id(5)]
    public bool Closing { get; set; }
}
