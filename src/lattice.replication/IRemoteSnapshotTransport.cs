using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Transport-shaped seam that delivers a snapshot stream from a sender
/// cluster to a receiver cluster. The interface is the wire contract
/// behind a cross-cluster
/// <see cref="ISnapshotProvider"/> adapter: the receiver's bootstrap
/// state machine drives the local <see cref="ISnapshotProvider"/>,
/// which in turn calls this transport to fetch the snapshot from the
/// sender's own <see cref="ISnapshotProvider"/>.
/// <para>
/// <see cref="IRemoteSnapshotTransport"/> is deliberately separate from
/// <see cref="IReplicationTransport"/>. The latter ships
/// live-incremental WAL entries; this seam ships a point-in-time
/// snapshot export. Keeping them split lets a host plug different
/// bindings for snapshot vs. live (for example, HTTP / blob-store for
/// bulk snapshot, gRPC streaming for the live tail) and lets a host
/// register a custom snapshot transport without disturbing the
/// live-incremental pipeline.
/// </para>
/// <para>
/// Implementations are expected to:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>
///       Validate that <c>treeName</c> and <c>sourceClusterId</c> are
///       non-null and non-empty, throwing
///       <see cref="ArgumentException"/> when they are not.
///     </description>
///   </item>
///   <item>
///     <description>
///       Capture the snapshot cut-point on the sender atomically with
///       the start of the entry stream. The
///       <see cref="GetMetadataAsync"/> return value MUST describe the
///       same snapshot that any concurrent
///       <see cref="RequestSnapshotAsync"/> call returns - the entry
///       stream is a point-in-time view at the returned
///       <see cref="RemoteSnapshotMetadata.AsOfHlc"/>, not a moving
///       target.
///     </description>
///   </item>
///   <item>
///     <description>
///       Be safe to invoke concurrently across distinct
///       <c>(treeName, sourceClusterId)</c> pairs. Concurrent
///       invocation against the same pair is implementation-defined;
///       receivers serialise per pair through the bootstrap
///       coordinator.
///     </description>
///   </item>
/// </list>
/// </summary>
public interface IRemoteSnapshotTransport
{
    /// <summary>
    /// Captures the sender's current snapshot cut-point for
    /// <paramref name="treeName"/> as observed at
    /// <paramref name="sourceClusterId"/>. The returned metadata
    /// describes the same snapshot that a paired call to
    /// <see cref="RequestSnapshotAsync"/> with the same
    /// <paramref name="treeName"/> /
    /// <paramref name="sourceClusterId"/> /
    /// <paramref name="fromAsOfHlc"/> tuple will stream.
    /// </summary>
    /// <param name="treeName">
    /// The logical tree id to snapshot. Must be non-null and non-empty.
    /// </param>
    /// <param name="sourceClusterId">
    /// The sender-cluster identifier the snapshot is captured on.
    /// Must be non-null and non-empty.
    /// </param>
    /// <param name="fromAsOfHlc">
    /// Strict upper-bound timestamp the receiver wishes to pin. Pass
    /// <see cref="HybridLogicalClock.Zero"/> to let the sender choose
    /// its own cut-point (the common case for a fresh peer).
    /// Implementations may clamp the returned
    /// <see cref="RemoteSnapshotMetadata.AsOfHlc"/> to a sender-side
    /// upper bound when the value is non-zero.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<RemoteSnapshotMetadata> GetMetadataAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams every live entry in the sender-side tree
    /// <paramref name="treeName"/> whose
    /// <see cref="SnapshotEntry.Timestamp"/> is less than or equal to
    /// <paramref name="fromAsOfHlc"/>. Pass
    /// <see cref="HybridLogicalClock.Zero"/> to stream every live
    /// entry irrespective of timestamp.
    /// <para>
    /// The stream's cut-point is the same value carried on
    /// <see cref="RemoteSnapshotMetadata.AsOfHlc"/>, so receivers MUST
    /// invoke <see cref="GetMetadataAsync"/> first and pin
    /// <see cref="RemoteSnapshotMetadata.CausalStableFrontier"/>
    /// before draining the stream; otherwise the snapshot/incremental
    /// handoff loses its causal-stable starting point and the
    /// receiver-side causal-dependency check on the first incremental
    /// entry runs from the empty frontier.
    /// </para>
    /// </summary>
    /// <param name="treeName">
    /// The logical tree id to stream. Must be non-null and non-empty.
    /// </param>
    /// <param name="sourceClusterId">
    /// The sender-cluster identifier. Must be non-null and non-empty.
    /// </param>
    /// <param name="fromAsOfHlc">
    /// Strict upper-bound timestamp on emitted entries.
    /// <see cref="HybridLogicalClock.Zero"/> disables the filter.
    /// </param>
    /// <param name="cancellationToken">
    /// Observed on every yielded entry.
    /// </param>
    IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        CancellationToken cancellationToken = default);
}
