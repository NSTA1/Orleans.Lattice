namespace Orleans.Lattice.Testing;

/// <summary>
/// Consumer-supplied adapter over one WAL storage provider, used by
/// <see cref="WalStorageProviderContractTestsBase"/>.
/// <para>
/// Where <see cref="IWalOffsetAllocationProbe"/> is deliberately narrow (offsets
/// only, one fixed shard), this probe exposes every member of the provider seam,
/// takes the tree id and shard index as parameters, and carries payloads, so the
/// suite can pin append atomicity, read bounds, the encoded paths, trim, byte
/// accounting, isolation, and argument validation. Each member forwards to the
/// provider method of the same name; <see langword="null"/> tree ids are passed
/// through unchanged so the suite can check the provider rejects them.
/// </para>
/// </summary>
public interface IWalStorageProviderContractProbe : IAsyncDisposable
{
    /// <summary>
    /// The provider's entry-shaped append, then whatever durability barrier the
    /// provider's real caller crosses before treating the append as durable.
    /// </summary>
    Task AppendAsync(string treeId, int shardIndex, IReadOnlyList<WalContractEntry> entries, CancellationToken cancellationToken);

    /// <summary>
    /// The provider's pre-encoded (bytes-shaped) append, then the same
    /// durability barrier as <see cref="AppendAsync"/>.
    /// </summary>
    Task AppendEncodedAsync(string treeId, int shardIndex, IReadOnlyList<WalContractEntry> entries, CancellationToken cancellationToken);

    /// <summary>
    /// Calls the provider's pre-encoded append with one encoded segment and two
    /// offsets, a length mismatch the contract requires it to reject.
    /// </summary>
    Task AppendEncodedWithMismatchedOffsetsAsync(string treeId, int shardIndex, CancellationToken cancellationToken);

    /// <summary>The provider's entry-shaped read, fully enumerated.</summary>
    Task<IReadOnlyList<WalContractEntry>> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken);

    /// <summary>The provider's bytes-shaped read, decoded.</summary>
    Task<WalContractEncodedPage> ReadEncodedAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken);

    /// <summary>
    /// The provider's filtered read (issue #3565) with a key-range filter owning
    /// <c>[lowKeyInclusive, highKeyExclusive)</c>, fully enumerated. A
    /// routing-only entry maps to an empty <see cref="WalContractEntry.Value"/>.
    /// </summary>
    Task<IReadOnlyList<WalContractEntry>> ReadFilteredAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int maxEntries,
        string? lowKeyInclusive,
        string? highKeyExclusive,
        CancellationToken cancellationToken);

    /// <summary>Provider's <c>GetHighestOffsetAsync</c>.</summary>
    Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken);

    /// <summary>Provider's <c>GetLowestOffsetAsync</c>.</summary>
    Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken);

    /// <summary>Provider's <c>TrimAsync</c>.</summary>
    Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken);

    /// <summary>Provider's <c>EvaluateCompactionAsync</c>.</summary>
    Task EvaluateCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken);

    /// <summary>Provider's <c>ReconcileAsync</c>.</summary>
    Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken);

    /// <summary>Provider's <c>GetRetainedByteSizeAsync</c> (<c>-1</c> = unsupported).</summary>
    Task<long> GetRetainedByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken);

    /// <summary>Provider's <c>GetPhysicalByteSizeAsync</c> (<c>-1</c> = unsupported).</summary>
    Task<long> GetPhysicalByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken);

    /// <summary>
    /// Models a process restart: a durable provider drops all in-memory state and
    /// re-reads its backing store; a volatile provider implements this as a
    /// no-op.
    /// </summary>
    Task ReopenAsync(CancellationToken cancellationToken);
}
