using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown when a write reaches a tree whose purge still holds the tree id
/// (issue #5458). A purge keeps the tree's registry entry until the last step of
/// its tail. While that step is owed, the registry still names the id, but the
/// entry belongs to the purged tree. Admitting a write in that window would
/// re-seed the purged shard under the deleted tree's lineage, and the purge's
/// re-driven tail would then remove it. So the write is refused until the purge
/// releases the id.
/// <para>
/// <b>Caller contract.</b> This is a transient refusal, not a failure: the write
/// was never committed. The purge's keepalive reminder re-drives its tail about
/// once a minute until the tail settles, so a retry after about one minute
/// succeeds. Once the purge has released the id, a write is the deliberate reuse
/// of the id and creates the tree again.
/// </para>
/// <para>
/// Derives from <see cref="InvalidOperationException"/> because that is the type
/// this refusal has always surfaced as, so existing catch sites keep working. It
/// implements <see cref="ILatticeDomainFault"/>, so a broad
/// <c>catch (InvalidOperationException ex) when (ex is not ILatticeDomainFault)</c>
/// written for a genuine misuse declines it, and the routing tier does not treat
/// it as a stale alias: refreshing the alias does not end a purge.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTreePurgeSettling)]
public sealed class LatticeTreePurgeSettlingException : InvalidOperationException, ILatticeDomainFault
{
    /// <summary>
    /// The tree id the purge still holds. Empty on the parameterless constructors.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>Initialises a new instance with no message and an empty tree id.</summary>
    public LatticeTreePurgeSettlingException()
    {
        TreeId = string.Empty;
    }

    /// <summary>Initialises a new instance with a message and an empty tree id.</summary>
    /// <param name="message">Diagnostic context describing the refused write.</param>
    public LatticeTreePurgeSettlingException(string message) : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>Initialises a new instance with a message, an inner exception and an empty tree id.</summary>
    /// <param name="message">Diagnostic context describing the refused write.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public LatticeTreePurgeSettlingException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
    }

    /// <summary>Initialises a new instance attributed to <paramref name="treeId"/>. The production throw shape.</summary>
    /// <param name="message">Diagnostic context describing the refused write.</param>
    /// <param name="treeId">The tree id the purge still holds.</param>
    public LatticeTreePurgeSettlingException(string message, string treeId) : base(message)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
    }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeTreePurgeSettlingException"/>. The
/// generated copier for a <c>[GenerateSerializer]</c> exception deriving from a BCL
/// exception subclass requests a copier for that base type, which Orleans does not
/// provide, so a co-located throw would otherwise fail with an opaque
/// <c>KeyNotFoundException</c>. An exception is immutable once constructed, so
/// returning the same instance is a correct deep copy.
/// </summary>
[RegisterCopier]
internal sealed class LatticeTreePurgeSettlingExceptionCopier : IDeepCopier<LatticeTreePurgeSettlingException>
{
    /// <inheritdoc />
    public LatticeTreePurgeSettlingException DeepCopy(LatticeTreePurgeSettlingException input, CopyContext context) => input;
}
