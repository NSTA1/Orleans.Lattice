namespace Orleans.Lattice.Replication;

/// <summary>
/// Inbound apply seam. Installs a single <see cref="ReplogEntry"/>
/// authored on a remote cluster onto the local tree, preserving the
/// remote cluster's <see cref="Primitives.HybridLogicalClock"/> and
/// origin id end-to-end.
/// <para>
/// Implementations are responsible for:
/// </para>
/// <list type="bullet">
/// <item>filtering re-delivery via the per-origin high-water-mark
/// (an entry whose timestamp is at or below
/// <c>HWM[(treeId, originClusterId)]</c> is a no-op),</item>
/// <item>routing the entry through the apply seam exposed by
/// <c>Orleans.Lattice</c> so the persisted
/// <c>LwwValue&lt;byte[]&gt;</c> carries the source HLC and origin
/// verbatim,</item>
/// <item>advancing the per-origin HWM after a successful apply so
/// subsequent re-delivery is suppressed.</item>
/// </list>
/// <para>
/// The applier deliberately does not subscribe to a transport — it is
/// the seam custom transports, integration tests, and the future
/// inbound replication pipeline plug into.
/// </para>
/// </summary>
public interface IReplicationApplier
{
    /// <summary>
    /// Applies <paramref name="entry"/> to the local tree.
    /// </summary>
    /// <param name="entry">The captured remote mutation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// An <see cref="ApplyResult"/> indicating whether the entry was
    /// merged and where the per-origin high-water-mark stands after
    /// the call.
    /// </returns>
    Task<ApplyResult> ApplyAsync(ReplogEntry entry, CancellationToken cancellationToken = default);
}
