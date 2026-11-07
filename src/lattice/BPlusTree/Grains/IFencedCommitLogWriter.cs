namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A commit-log writer that can append under a leaf's append fence (epic #5605).
/// The default <see cref="WalCommitLogWriter"/> implements it; a writer that does
/// not is used unstamped, which the WAL shard always admits.
/// </summary>
internal interface IFencedCommitLogWriter : ICommitLogWriter
{
    /// <summary>
    /// <see cref="ICommitLogWriter.AppendAsync"/>, appending through
    /// <paramref name="fence"/>'s stamp when its view of the gate is open.
    /// </summary>
    Task<long> AppendAsync(WalRecord entry, ILeafAppendFence fence, CancellationToken cancellationToken);

    /// <summary>
    /// <see cref="ICommitLogWriter.AppendManyAsync"/>, appending every partition's
    /// slice through <paramref name="fence"/>'s stamp when its view of the gate is open.
    /// </summary>
    Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, ILeafAppendFence fence, CancellationToken cancellationToken);

    /// <summary>
    /// <see cref="AppendAsync(WalRecord, ILeafAppendFence, CancellationToken)"/>, with
    /// the leaf's stamp extended by <paramref name="routing"/>, the shard-level
    /// generation the write was admitted under (epic #5605, member 2).
    /// </summary>
    Task<long> AppendAsync(WalRecord entry, ILeafAppendFence fence, WalRoutingStamp routing, CancellationToken cancellationToken);

    /// <summary>
    /// <see cref="AppendManyAsync(IReadOnlyList{WalRecord}, ILeafAppendFence, CancellationToken)"/>,
    /// with the leaf's stamp extended by <paramref name="routing"/> (epic #5605, member 2).
    /// </summary>
    Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, ILeafAppendFence fence, WalRoutingStamp routing, CancellationToken cancellationToken);
}
