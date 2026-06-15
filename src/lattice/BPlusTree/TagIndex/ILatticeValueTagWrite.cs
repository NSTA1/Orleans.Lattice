namespace Orleans.Lattice;

/// <summary>
/// A staged combined value-plus-tags write opened by
/// <see cref="ILatticeTagIndex.SetValueWithTags(string, byte[], string[])"/>.
/// The value is written to the subject tree and the supplied tags are added to
/// the index. Call <see cref="Atomic"/> to couple the two durably; the default
/// is <see cref="TagConsistency.Eventual"/>.
/// </summary>
public interface ILatticeValueTagWrite
{
    /// <summary>
    /// Opts the write into <see cref="TagConsistency.Atomic"/>: the value and
    /// every tag-membership add commit together through a single cross-tree
    /// atomic-write saga.
    /// </summary>
    ILatticeValueTagWrite Atomic();

    /// <summary>
    /// Opts the write back into <see cref="TagConsistency.Eventual"/> (the
    /// default): the value and the tag rows are two independent durable writes.
    /// </summary>
    ILatticeValueTagWrite Eventual();

    /// <summary>Commits the staged value and tags under the selected consistency.</summary>
    /// <param name="cancellationToken">Cancels the write(s) before they are dispatched.</param>
    Task CommitAsync(CancellationToken cancellationToken = default);
}
