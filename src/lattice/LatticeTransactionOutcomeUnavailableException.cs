using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown by a read when the value it would return depends on the outcome of an
/// atomic-write saga that the per-tree transaction registry could not be reached
/// to establish. A key written by a saga carries a prepared mutation until the
/// saga's terminal lands on its leaf, and while it does the read must ask the
/// registry whether the saga committed. When that call fails in transport (a
/// timeout, an unavailable silo, a rejected message) the read throws this
/// exception rather than guessing.
/// <para>
/// <b>The visibility rule.</b> A read never serves a value for a key whose
/// prepare outcome it could not establish:
/// <list type="bullet">
///   <item><description>
///     the registry answers that the outcome is no longer known (an
///     indeterminate answer, for example an aged-out decision):
///     the key reads as absent, because that condition is permanent;
///   </description></item>
///   <item><description>
///     the registry cannot be reached: this exception, because that condition is
///     transient;
///   </description></item>
///   <item><description>
///     never a guessed value - neither the prepared value nor the pre-saga value
///     is served on the strength of a registry that did not answer.
///   </description></item>
/// </list>
/// A multi-key read raises it only when its result actually depended on the
/// registry - some key it read carried a prepared mutation. <c>GetManyAsync</c>
/// and the counts raise it only after their own bounded snapshot retry
/// (<see cref="LatticeOptions.MaxScanRetries"/>) is exhausted. Key and entry
/// enumeration does not retry: a scan resolves every page against a single
/// registry view - the one it takes when it starts, or the one a point-in-time
/// cursor captured at open - and when it could not obtain that view it raises
/// this exception as soon as a page reaches a prepared key.
/// The pages it has already yielded held no prepared key, so what the caller
/// received is consistent. A count narrowed by an access-gate key filter is
/// computed by enumerating keys, so it behaves like enumeration. A read over
/// keys that carry no prepared mutation completes normally while the registry
/// is unreachable.
/// </para>
/// <para>
/// <b>The exception is retriable.</b> The condition clears as soon as the
/// registry is reachable again, and a read is side-effect free, so the caller
/// re-issues the same request after a backoff. No retry happens inside the leaf
/// that raised it: a grain call has already spent up to its response timeout by
/// the time the failure surfaces, so retry policy belongs to the caller.
/// </para>
/// <para>
/// It derives from <see cref="System.TimeoutException"/> so an existing
/// <c>catch (TimeoutException)</c> handler keeps observing the failure it
/// observed before this type existed, when the raw registry timeout propagated.
/// It is nonetheless a domain condition the store decided to report, so it
/// implements <see cref="ILatticeDomainFault"/>, and a handler that must tell the
/// two apart declines it with
/// <c>catch (TimeoutException ex) when (ex is not ILatticeDomainFault)</c>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TransactionOutcomeUnavailable)]
public sealed class LatticeTransactionOutcomeUnavailableException : TimeoutException, ILatticeDomainFault
{
    /// <summary>
    /// Initialises a new instance with no diagnostic context. Provided to
    /// satisfy the framework's exception construction contract; production
    /// throw sites use the message + inner-exception overload.
    /// </summary>
    public LatticeTransactionOutcomeUnavailableException() { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message.
    /// </summary>
    public LatticeTransactionOutcomeUnavailableException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception (typically the registry call's transport
    /// failure).
    /// </summary>
    public LatticeTransactionOutcomeUnavailableException(string message, Exception innerException)
        : base(message, innerException) { }

    /// <summary>
    /// The tree whose read could not establish a saga outcome.
    /// </summary>
    [Id(0)] public string TreeId { get; set; } = string.Empty;

    /// <summary>
    /// The key whose prepared mutation could not be resolved, for a single-key
    /// read; <see langword="null"/> for a multi-key read, which reports
    /// <see cref="KeyCount"/> instead.
    /// </summary>
    [Id(1)] public string? Key { get; set; }

    /// <summary>
    /// The number of keys carrying a prepared mutation whose outcome could not be
    /// established. One for a single-key read.
    /// </summary>
    [Id(2)] public int KeyCount { get; set; }

    /// <summary>
    /// The saga transaction ids whose outcome could not be established.
    /// </summary>
    [Id(3)] public IReadOnlyList<Guid> TransactionIds { get; set; } = Array.Empty<Guid>();

    /// <summary>
    /// Builds the exception with its attribution slots and a diagnostic
    /// message. The key itself is carried on <see cref="Key"/> only and is kept
    /// out of the message, so a logged message never discloses key content.
    /// </summary>
    internal static LatticeTransactionOutcomeUnavailableException Create(
        string treeId,
        string? key,
        int keyCount,
        IReadOnlyList<Guid> transactionIds,
        Exception? innerException)
    {
        var message =
            $"The outcome of {transactionIds.Count} atomic-write saga(s) on tree '{treeId}' could not be "
            + $"established because the transaction registry could not be reached, and {keyCount} key(s) read "
            + "depend on it. No value was guessed; the read is retryable once the registry is reachable.";
        var ex = innerException is null
            ? new LatticeTransactionOutcomeUnavailableException(message)
            : new LatticeTransactionOutcomeUnavailableException(message, innerException);
        ex.TreeId = treeId;
        ex.Key = key;
        ex.KeyCount = keyCount;
        ex.TransactionIds = transactionIds;
        return ex;
    }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeTransactionOutcomeUnavailableException"/>.
/// Orleans deep-copies a grain result across an in-process (co-located) boundary
/// instead of serialising it, and the generated copier for a
/// <c>[GenerateSerializer]</c> exception deriving from a BCL exception subclass
/// requests a copier for that base type, which Orleans does not provide - so a
/// same-silo throw would fail with an opaque <c>KeyNotFoundException</c> and mask
/// the real fault. An exception is immutable once constructed, so returning the
/// same instance is a correct deep copy.
/// </summary>
[RegisterCopier]
internal sealed class LatticeTransactionOutcomeUnavailableExceptionCopier
    : IDeepCopier<LatticeTransactionOutcomeUnavailableException>
{
    /// <inheritdoc />
    public LatticeTransactionOutcomeUnavailableException DeepCopy(
        LatticeTransactionOutcomeUnavailableException input,
        CopyContext context) => input;
}
