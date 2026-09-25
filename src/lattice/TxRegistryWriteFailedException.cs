namespace Orleans.Lattice;

/// <summary>
/// Thrown by <c>TxRegistryGrain</c> when a group-committed state write of a saga
/// decision registry row fails (issue #3501). Every mutation the write carried
/// has been rolled back in memory, so nothing it covered is durable and the
/// caller may retry the same registry operation: every registry mutator is
/// idempotent.
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
/// optimistic-concurrency (ETag) check: storage holds a row this activation
/// never read. The registry requests its own deactivation, so a retry lands on a
/// fresh activation that reloads the row before applying anything.
/// </para>
/// <para>
/// This exception derives directly from <see cref="Exception"/> so the
/// same-silo deep copier Orleans registers for <see cref="Exception"/> covers
/// its base slice. It is part of the internal coordination protocol between the
/// registry and its callers, which retry it through <c>TxRegistryWriteRetry</c>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxRegistryWriteFailed)]
internal sealed class TxRegistryWriteFailedException : Exception
{
    /// <summary>Creates a new <see cref="TxRegistryWriteFailedException"/>.</summary>
    /// <param name="registryKey">The grain key of the registry whose write failed.</param>
    /// <param name="fault">The storage fault that failed the write.</param>
    /// <param name="conflict">Whether the fault was an optimistic-concurrency conflict.</param>
    public TxRegistryWriteFailedException(string registryKey, Exception fault, bool conflict)
        : base(
            conflict
                ? $"Saga decision registry '{registryKey}' lost an optimistic-concurrency check on its state write ({fault.GetType().Name}: {fault.Message}). Nothing the write carried is durable; the registry is reloading, so retry the operation."
                : $"Saga decision registry '{registryKey}' failed its state write ({fault.GetType().Name}: {fault.Message}). Nothing the write carried is durable; retry the operation.")
    {
        RegistryKey = registryKey;
        FaultType = fault.GetType().FullName ?? fault.GetType().Name;
        Conflict = conflict;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public TxRegistryWriteFailedException() { }

    /// <summary>The grain key of the registry whose write failed.</summary>
    [Id(0)] public string RegistryKey { get; set; } = string.Empty;

    /// <summary>The full type name of the storage fault that failed the write.</summary>
    [Id(1)] public string FaultType { get; set; } = string.Empty;

    /// <summary>
    /// Whether the write lost an optimistic-concurrency (ETag) check, after which
    /// the registry deactivated so the next call reloads its row.
    /// </summary>
    [Id(2)] public bool Conflict { get; set; }
}
