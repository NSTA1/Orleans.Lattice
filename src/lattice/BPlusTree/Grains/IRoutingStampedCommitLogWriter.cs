namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A leaf's commit-log writer that can carry a write's routing stamp to its WAL
/// append (epic #5605, member 2): the shard-level generation a stamped leaf method
/// was called under, or <see cref="WalRoutingStamp.Unstamped"/> for a write that
/// reached the leaf through an existing, unstamped method (design note section 2,
/// S3). A writer that does not implement it appends as
/// <see cref="ICommitLogWriter"/> does.
/// </summary>
internal interface IRoutingStampedCommitLogWriter : ICommitLogWriter
{
    /// <summary><see cref="ICommitLogWriter.AppendAsync"/> under <paramref name="routing"/>.</summary>
    Task<long> AppendAsync(WalRecord entry, WalRoutingStamp routing, CancellationToken cancellationToken);

    /// <summary><see cref="ICommitLogWriter.AppendManyAsync"/> under <paramref name="routing"/>.</summary>
    Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, WalRoutingStamp routing, CancellationToken cancellationToken);
}
