namespace Orleans.Lattice;

/// <summary>
/// Thrown by a read on a tree whose replica is being <b>bootstrapped from a
/// snapshot in place</b> (issue #4526). A bootstrap normally stages the import on an
/// online shadow copy and cuts the alias over when it completes, leaving the current
/// tree readable and never raising this exception. Only an in-place import - a
/// legacy bootstrap, or one resuming a partial import an earlier failed attempt left
/// fenced - arms the read fence. Such a drain applies the snapshot's rows one at a
/// time into the tree, so a read part-way through could observe some of a committed
/// atomic batch's keys and not the rest.
/// Every read of a fenced tree - point reads, multi-key reads, scans, counts, the
/// read-modify-write verbs (<c>GetOrSetAsync</c>, conditional and predicate writes)
/// and snapshot-cursor and backup captures - is therefore refused until the drain
/// has applied every snapshot entry, at which point the whole import becomes
/// visible at once.
/// <para>
/// <b>Caller contract.</b> This is a transient refusal, not a failure: no state
/// was read or changed. Back off and retry. The fence lasts for the duration of
/// the snapshot drain, roughly proportional to the size of the tree. Plain writes
/// are not refused. If a bootstrap fails part-way, the fence stays up, because the
/// tree then holds a partial import; the bootstrap coordinator retries the
/// bootstrap automatically and the fence lifts when one succeeds. An operator can
/// lift it early only through an explicit, audited override that accepts reading
/// the partial import.
/// </para>
/// <para>
/// Derives directly from <see cref="Exception"/>, so a broad
/// <c>catch (InvalidOperationException)</c> written for a framework failure never
/// absorbs it, and no same-silo deep copier is needed.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTreeBootstrapping)]
public sealed class LatticeTreeBootstrappingException : Exception
{
    /// <summary>
    /// The physical tree id whose read fence refused the read. Empty on the
    /// parameterless constructors.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>Initialises a new instance with no message and an empty tree id.</summary>
    public LatticeTreeBootstrappingException()
    {
        TreeId = string.Empty;
    }

    /// <summary>Initialises a new instance with a message and an empty tree id.</summary>
    /// <param name="message">Diagnostic context describing the refused read.</param>
    public LatticeTreeBootstrappingException(string message) : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>Initialises a new instance with a message, an inner exception and an empty tree id.</summary>
    /// <param name="message">Diagnostic context describing the refused read.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public LatticeTreeBootstrappingException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
    }

    /// <summary>Initialises a new instance attributed to <paramref name="treeId"/>. The production throw shape.</summary>
    /// <param name="message">Diagnostic context describing the refused read.</param>
    /// <param name="treeId">The tree whose read fence refused the read.</param>
    public LatticeTreeBootstrappingException(string message, string treeId) : base(message)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
    }
}
