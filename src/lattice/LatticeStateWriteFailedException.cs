namespace Orleans.Lattice;

/// <summary>
/// Thrown when a Lattice saga grain (an atomic-write saga, a cross-tree atomic
/// write coordinator, or a WAL materialiser pin shard) fails to persist its own
/// durable state. It replaces the storage provider's exception at the grain
/// boundary, so a caller always receives an attributable, serializable Lattice
/// exception (issue #3572).
/// <para>
/// The provider exception is summarised in the message and in
/// <see cref="FaultType"/> rather than carried as the inner exception. A storage
/// provider's exception type (for example the Azure Table
/// <c>TableStorageUpdateConditionNotSatisfiedException</c>) is not loadable in a
/// client that does not reference that provider, and an unloadable inner
/// exception surfaces there as a <see cref="TypeLoadException"/> that masks the
/// real fault.
/// </para>
/// <para>
/// When <see cref="Conflict"/> is <see langword="true"/> the write lost an
/// optimistic-concurrency (ETag) check. The usual cause is a storage SDK
/// transport retry: the first attempt landed, the retry saw the row's new ETag
/// and reported a conflict, and the activation's cached ETag is stale from then
/// on. The grain stops writing and requests its own deactivation, so the next
/// call lands on a fresh activation that reloads the row and resumes from what
/// is actually durable. Every step the saga grains persist is idempotent on
/// resume, so retrying the same operation (with the same operation id) is safe
/// and neither double-applies nor loses a commit or abort decision.
/// </para>
/// <para>
/// This exception derives directly from <see cref="Exception"/> so the
/// same-silo deep copier Orleans registers for <see cref="Exception"/> covers
/// its base slice.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeStateWriteFailed)]
public sealed class LatticeStateWriteFailedException : Exception
{
    /// <summary>Creates a new <see cref="LatticeStateWriteFailedException"/>.</summary>
    public LatticeStateWriteFailedException() { }

    /// <summary>Creates a new <see cref="LatticeStateWriteFailedException"/> with a message.</summary>
    /// <param name="message">The exception message.</param>
    public LatticeStateWriteFailedException(string message) : base(message) { }

    /// <summary>
    /// Creates a new <see cref="LatticeStateWriteFailedException"/> with a message
    /// and an inner exception. Lattice never passes a storage provider's exception
    /// here; see the type remarks.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public LatticeStateWriteFailedException(string message, Exception innerException)
        : base(message, innerException) { }

    /// <summary>
    /// Creates a new <see cref="LatticeStateWriteFailedException"/> that
    /// attributes a failed state write to the grain that issued it.
    /// </summary>
    /// <param name="grainType">A short name for the grain whose write failed (for example <c>atomic-write</c>).</param>
    /// <param name="grainKey">The key of the grain whose write failed.</param>
    /// <param name="fault">The storage fault that failed the write. Summarised, never carried.</param>
    /// <param name="conflict">Whether the fault was an optimistic-concurrency conflict.</param>
    public LatticeStateWriteFailedException(string grainType, string grainKey, Exception fault, bool conflict)
        : base(BuildMessage(grainType, grainKey, fault, conflict))
    {
        ArgumentNullException.ThrowIfNull(fault);
        GrainType = grainType ?? string.Empty;
        GrainKey = grainKey ?? string.Empty;
        FaultType = fault.GetType().FullName ?? fault.GetType().Name;
        Conflict = conflict;
    }

    /// <summary>A short name for the grain whose write failed.</summary>
    [Id(0)] public string GrainType { get; set; } = string.Empty;

    /// <summary>The key of the grain whose write failed.</summary>
    [Id(1)] public string GrainKey { get; set; } = string.Empty;

    /// <summary>The full type name of the storage fault that failed the write.</summary>
    [Id(2)] public string FaultType { get; set; } = string.Empty;

    /// <summary>
    /// Whether the write lost an optimistic-concurrency (ETag) check, after which
    /// the grain deactivated so the next call reloads its durable state. A
    /// conflicted operation is safe to retry with the same operation id.
    /// </summary>
    [Id(3)] public bool Conflict { get; set; }

    private static string BuildMessage(string grainType, string grainKey, Exception fault, bool conflict)
    {
        ArgumentNullException.ThrowIfNull(fault);
        return conflict
            ? $"Lattice {grainType} grain '{grainKey}' lost an optimistic-concurrency check on its state write ({fault.GetType().Name}: {fault.Message}). The write may have landed; the grain is reloading its durable state, so retry the operation with the same operation id."
            : $"Lattice {grainType} grain '{grainKey}' failed its state write ({fault.GetType().Name}: {fault.Message}). Retry the operation with the same operation id.";
    }
}
