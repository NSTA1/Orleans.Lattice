namespace Orleans.Lattice;

/// <summary>
/// Thrown by any public <see cref="ILattice"/> operator (and by the
/// internal saga coordinator on its caller-facing throw path) when
/// the operation cannot complete because the owning silo's
/// write-ahead-log writer is draining as part of host shutdown.
/// Callers should treat this as back-pressure, not as a real failure -
/// the entries the operation carried were never durably committed,
/// but the silo refused to accept them because the host is going away
/// rather than because the storage layer rejected them.
/// <para>
/// <b>Caller contract.</b> A caller observing this exception should
/// abandon the operation rather than retry it - every subsequent
/// attempt against the same silo activation in this lifetime will
/// fail with the same exception, because the writer drain is a one-
/// way transition. Long-lived clients should either fail over to a
/// peer silo (if the cluster is multi-node) or surface the back-
/// pressure to upstream callers (drop the request, queue it to a
/// side outbox, or rate-limit). Re-issuing the same operation after
/// the host restarts is the normal recovery path; the previously
/// failed entries are not durable, so the re-issue is a fresh
/// attempt against a fresh silo activation.
/// </para>
/// <para>
/// <b>Sources.</b> Surfaces from three distinct shutdown failure
/// shapes that share the same operational meaning ("this silo is
/// going away; the operation was refused"):
/// </para>
/// <list type="bullet">
///   <item><description>The writer-side drain refusal from
///   <c>WalCommitLogWriter.DrainAsync</c>, raised inline on any new
///   <c>AppendAsync</c> / <c>AppendBatchAsync</c> dispatch after the
///   drain flag flips.</description></item>
///   <item><description>The Orleans runtime's refusal to re-activate
///   a grain that has been deactivated as part of the same shutdown
///   ("Unable to create local activation" / "invalid activation"
///   reported by <c>OrleansMessageRejectionException</c>).</description></item>
///   <item><description>The saga coordinator's own short-circuit
///   when either of the above is observed mid-saga; rather than
///   surfacing the inner shape verbatim and forcing every caller to
///   parse exception messages, the saga wraps the cause in this
///   typed exception so consumers can detect the regime via a
///   single <see langword="is"/> check.</description></item>
/// </list>
/// <para>
/// Derives from <see cref="System.InvalidOperationException"/> so
/// existing catch handlers that match on
/// <see cref="System.InvalidOperationException"/> continue to absorb
/// it; the typed slot lets callers that care about the shutdown
/// regime explicitly distinguish it from genuine
/// <see cref="System.InvalidOperationException"/> failures (which
/// are not back-pressure and should be retried per the caller's
/// normal policy).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeShuttingDown)]
public sealed class LatticeShuttingDownException : InvalidOperationException
{
    /// <summary>
    /// Initialises a new instance with no diagnostic message. Provided to
    /// satisfy the framework's exception construction contract; production
    /// throw sites use the message or message + inner-exception overload.
    /// </summary>
    public LatticeShuttingDownException() { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message.
    /// </summary>
    /// <param name="message">Diagnostic context describing which operation was refused and why.</param>
    public LatticeShuttingDownException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message
    /// and wrapped inner exception. Production throw sites use this
    /// overload to preserve the underlying refusal shape (the writer
    /// drain refusal, the Orleans activation rejection, or the inner
    /// saga failure) for log diagnostics while still letting callers
    /// detect the shutdown regime by type rather than by message
    /// parsing.
    /// </summary>
    /// <param name="message">Diagnostic context describing which operation was refused and why.</param>
    /// <param name="innerException">The underlying cause (writer drain refusal, Orleans activation rejection, or saga inner failure).</param>
    public LatticeShuttingDownException(string message, Exception innerException)
        : base(message, innerException) { }
}