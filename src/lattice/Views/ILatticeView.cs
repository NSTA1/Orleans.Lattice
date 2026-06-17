namespace Orleans.Lattice;

/// <summary>
/// A read handle over a materialised view maintained asynchronously off a source
/// tree's write-ahead log. Query methods delegate to the underlying
/// <c>view-{name}</c> <see cref="ILattice"/>; the view converges toward the
/// source as the maintainer applies projected writes, so reads are
/// read-your-writes only after the relevant source mutations have been applied
/// (use <see cref="GetLagAsync"/> to observe how far behind the view is).
/// </summary>
public interface ILatticeView
{
    /// <summary>The logical view name; the backing tree is <c>view-{ViewName}</c>.</summary>
    string ViewName { get; }

    /// <summary>Gets the view value for <paramref name="key"/>, or <see langword="null"/> when absent.</summary>
    Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Returns the number of live entries in the view across all shards.</summary>
    Task<int> CountAsync(CancellationToken cancellationToken = default);

    /// <summary>Streams the view's live keys in lexicographic order over the optional range.</summary>
    IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default);

    /// <summary>Streams the view's live key-value entries in lexicographic key order over the optional range.</summary>
    IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the view's apply lag as the number of source WAL entries committed
    /// but not yet applied to the view, summed across every source shard. Zero
    /// means the view has caught up to the source head as of this call.
    /// </summary>
    Task<long> GetLagAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Rebuilds the view in place by scanning the current source entries through
    /// the projection and applying the resulting writes. Phase 1 performs an
    /// in-place rebuild (no shadow tree / atomic swap); concurrent live entries
    /// that are superseded by a later source mutation resolve by source HLC.
    /// </summary>
    Task RebuildAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Read-your-writes barrier: completes once the maintainer has applied every
    /// source entry up to <paramref name="target"/> (its highest applied source
    /// HLC reaches <paramref name="target"/>), or throws
    /// <see cref="TimeoutException"/> if that does not happen within
    /// <paramref name="timeout"/>. Drives drains while it waits, so a caller can
    /// observe its own write in the view without depending on the background
    /// timer. This is an opt-in barrier; the default contract remains best-effort
    /// lag (see <see cref="GetLagAsync"/>).
    /// <para>
    /// The applied position is tracked as the maximum applied source HLC across
    /// the view's shard cursors. For a single source shard this is exact; across
    /// multiple shards a lagging shard may still hold an unapplied entry below
    /// <paramref name="target"/>, so prefer <see cref="WaitForSourceHeadAsync"/>
    /// for a write-then-wait barrier that is exact regardless of shard count.
    /// </para>
    /// </summary>
    /// <param name="target">The source HLC the view must catch up to.</param>
    /// <param name="timeout">Maximum time to wait before throwing <see cref="TimeoutException"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed while waiting.</param>
    Task WaitForSourceHlcAsync(HybridLogicalClock target, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Convenience read-your-writes barrier that captures the source head HLC
    /// (the highest committed source HLC across all shards) at the moment of the
    /// call, then waits for the view to apply up to it - the natural
    /// "write, then wait until my write is visible" pattern. Throws
    /// <see cref="TimeoutException"/> if the view does not catch up within
    /// <paramref name="timeout"/>.
    /// </summary>
    /// <param name="timeout">Maximum time to wait before throwing <see cref="TimeoutException"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed while waiting.</param>
    Task WaitForSourceHeadAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
}
