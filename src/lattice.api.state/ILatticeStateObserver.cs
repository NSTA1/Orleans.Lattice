namespace Orleans.Lattice.Api.State;

/// <summary>
/// Transport-agnostic live change-observation facade over a cluster's lattice
/// state. Yields <see cref="StateChangeNotification"/> values as changes
/// commit, scoped to one tree and an optional key range, with a resumable
/// cursor. Every transport binding (the gRPC server-streaming subscription
/// and the Orleans.Lattice.Api.Mcp MCP server) is a thin adapter over this
/// single surface.
/// </summary>
/// <remarks>
/// <para>
/// The observe model tails the tree's durable write-ahead log by cursor rather
/// than buffering notifications in memory. This gives the binding its
/// back-pressure story for free: there is no per-subscription buffer to
/// overflow, a slow consumer simply tails the WAL more slowly, and a foreground
/// writer is never blocked by an observer. A consumer that falls so far behind
/// that its cursor is trimmed from the WAL retention window observes an
/// explicit <see cref="LatticeStateCursorExpiredException"/> on its next read
/// rather than a silent gap.
/// </para>
/// <para>
/// Delivery is at-least-once. Notifications are ordered by WAL sequence within
/// each WAL partition; with a single WAL partition this is a strict total order
/// per tree, and with multiple partitions notifications are globally ordered
/// only within a drain cycle (use each notification's
/// <see cref="StateChangeNotification.Hlc"/> for a stable client-side order).
/// </para>
/// </remarks>
internal interface ILatticeStateObserver
{
    /// <summary>
    /// Opens a live subscription to the changes on the requested tree and key
    /// range. The returned sequence yields notifications as changes commit and
    /// completes only when <paramref name="cancellationToken"/> is cancelled.
    /// </summary>
    /// <param name="request">Scope (tree, optional key range), resume cursor, and category filter.</param>
    /// <param name="cancellationToken">Cancellation token that tears the subscription down.</param>
    /// <exception cref="LatticeStateCursorExpiredException">
    /// The supplied resume cursor has fallen outside the WAL retention window.
    /// </exception>
    IAsyncEnumerable<StateChangeNotification> ObserveAsync(
        StateObserveRequest request,
        CancellationToken cancellationToken = default);
}
