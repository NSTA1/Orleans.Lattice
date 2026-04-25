namespace Orleans.Lattice.Replication;

/// <summary>
/// Sink for change-feed records captured at commit time. The default
/// registration is a no-op so the rest of the replication pipeline can
/// be wired up in isolation; later phases register a write-ahead-log
/// implementation that durably persists every <see cref="ReplogEntry"/>
/// before the originating grain''s write returns.
/// <para>
/// Implementations are invoked synchronously inside the grain''s
/// scheduler via the core <see cref="IMutationObserver"/> hook, so every
/// millisecond spent inside <see cref="WriteAsync"/> is added to the
/// caller''s write latency. Implementations must complete quickly or
/// enqueue the entry onto a background drain.
/// </para>
/// </summary>
internal interface IReplogSink
{
    /// <summary>
    /// Persists or forwards a captured <see cref="ReplogEntry"/>.
    /// </summary>
    /// <param name="entry">The captured mutation record.</param>
    /// <param name="cancellationToken">Cancellation token propagated from the grain call.</param>
    Task WriteAsync(ReplogEntry entry, CancellationToken cancellationToken);
}
