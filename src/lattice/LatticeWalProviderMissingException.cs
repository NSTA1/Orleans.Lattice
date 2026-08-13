using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown when a WAL partition's pinned placement references a provider
/// <see cref="IWalStorageProviderCatalog">catalog</see> key that the resolving
/// silo cannot resolve. This is a <b>fail-closed</b> signal: rather than
/// silently re-routing the partition's log to the baseline provider (which
/// would split a single logical log across two storage backends and risk
/// double-assigned offsets), the WAL shard refuses to activate and the caller's
/// append fails.
/// <para>
/// The usual cause is configuration drift - a tree was moved to a provider key
/// that one silo registered (through
/// <see cref="LatticeServiceCollectionExtensions.AddLatticeWalStorageProvider"/>)
/// but another did not. Every silo in a cluster must register an identical set
/// of catalog keys. Recovery is to register the missing key on the affected
/// silo and restart it, or to move the partition back to a key all silos can
/// resolve through the <see cref="ILatticeAdmin"/> move surface.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeWalProviderMissing)]
public sealed class LatticeWalProviderMissingException : InvalidOperationException
{
    /// <summary>
    /// Initialises a new instance with no diagnostic context. Provided to
    /// satisfy the framework's exception construction contract; production
    /// throw sites use the message or context overload.
    /// </summary>
    public LatticeWalProviderMissingException() { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message.
    /// </summary>
    /// <param name="message">Diagnostic context describing the unresolved key.</param>
    public LatticeWalProviderMissingException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception.
    /// </summary>
    /// <param name="message">Diagnostic context describing the unresolved key.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public LatticeWalProviderMissingException(string message, Exception innerException)
        : base(message, innerException) { }

    /// <summary>
    /// Initialises a new instance describing the specific tree, partition, and
    /// catalog key that failed to resolve.
    /// </summary>
    /// <param name="treeId">The tree whose WAL partition could not be placed.</param>
    /// <param name="partition">The WAL partition index.</param>
    /// <param name="providerKey">The unresolved catalog key.</param>
    public LatticeWalProviderMissingException(string treeId, int partition, string providerKey)
        : base($"WAL partition {treeId}/{partition} is pinned to provider key '{providerKey}', which is not registered on this silo. Every silo must register the same WAL storage provider keys.")
    {
        TreeId = treeId;
        Partition = partition;
        ProviderKey = providerKey;
    }

    /// <summary>The tree whose WAL partition could not be placed.</summary>
    [Id(0)]
    public string? TreeId { get; }

    /// <summary>The WAL partition index that could not be placed.</summary>
    [Id(1)]
    public int Partition { get; }

    /// <summary>The unresolved catalog key.</summary>
    [Id(2)]
    public string? ProviderKey { get; }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeWalProviderMissingException"/>. Orleans deep-copies a grain result
/// across an in-process (co-located) boundary instead of serialising it, and the
/// generated copier for a <c>[GenerateSerializer]</c> exception deriving from a BCL
/// exception subclass requests a copier for that base type, which Orleans does not
/// provide - so a same-silo throw would fail with an opaque <c>KeyNotFoundException</c>
/// ("Could not find a base type copier for ...") and mask the real, actionable fault.
/// An exception is immutable once constructed, so returning the same instance is a
/// correct deep copy and keeps the typed exception intact (the cross-silo serialise
/// path is unaffected).
/// </summary>
[RegisterCopier]
internal sealed class LatticeWalProviderMissingExceptionCopier : IDeepCopier<LatticeWalProviderMissingException>
{
    /// <inheritdoc />
    public LatticeWalProviderMissingException DeepCopy(LatticeWalProviderMissingException input, CopyContext context) => input;
}
