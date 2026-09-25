namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Signal that a cross-tree sub-saga's prepare-and-pause park step (the
/// registry delegation or the paused-phase persist) failed on a
/// <b>retryable</b> fault. Distinguished from a genuine staging failure so the
/// coordinator retries prepare instead of aborting the whole cross-tree
/// transaction: every prepared write is still staged, and the phase was
/// reverted so the sub-saga re-parks on the next attempt.
/// <para>
/// It crosses a grain boundary (from the sub-saga to the coordinator, and from
/// the coordinator to the caller), so it is serializable (issue #3572). The
/// underlying fault is summarised in the message and in <see cref="FaultType"/>
/// rather than carried as the inner exception, because a storage provider's
/// exception type is not loadable in every client. It derives directly from
/// <see cref="Exception"/> so the same-silo deep copier Orleans registers for
/// <see cref="Exception"/> covers its base slice.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CrossTreeParkRetry)]
internal sealed class CrossTreeParkRetryException : Exception
{
    /// <summary>Creates a new <see cref="CrossTreeParkRetryException"/> summarising <paramref name="fault"/>.</summary>
    /// <param name="fault">The retryable fault that failed the park step.</param>
    public CrossTreeParkRetryException(Exception fault)
        : base($"Cross-tree sub-saga park step failed on a retryable fault ({fault.GetType().Name}: {fault.Message}).")
    {
        FaultType = fault.GetType().FullName ?? fault.GetType().Name;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public CrossTreeParkRetryException() { }

    /// <summary>The full type name of the fault that failed the park step.</summary>
    [Id(0)] public string FaultType { get; set; } = string.Empty;
}
