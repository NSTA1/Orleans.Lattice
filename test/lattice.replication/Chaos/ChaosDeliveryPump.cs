using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Per-edge delivery pump that drives the chaos suite's "wire format
/// is in your head" loop: for every <c>(sender, receiver)</c> pair
/// where <c>sender != receiver</c>, polls
/// <see cref="IChangeFeed.Subscribe"/> on the sender (with
/// <c>includeLocalOrigin: true</c> so locally-originated entries are
/// observed and the per-target cycle-break filter below decides
/// whether to forward them), applies each entry through the receiver's
/// <see cref="ReplicationApplier"/>, and advances the per-edge cursor.
/// <para>
/// Network partitions are simulated by gating delivery on a
/// <c>(sender, receiver)</c> partition matrix. While partitioned,
/// the pump still polls the sender's WAL but does not advance the
/// cursor - when the partition heals, the next iteration ships
/// every entry that accumulated during the outage. This faithfully
/// models a transport whose connection drops and resumes from the
/// last acked offset.
/// </para>
/// </summary>
internal sealed class ChaosDeliveryPump : IAsyncDisposable
{
    private readonly MultiSiteClusterFixture _fixture;
    private readonly string _treeName;
    private readonly TimeSpan _pollInterval;
    private readonly bool[,] _partitioned;
    // Phase D1c: cursor storage migrated from HybridLogicalClock to
    // ChangeFeedCursor (per-partition WAL offset). The HLC cursor
    // shape silently dropped low-HLC entries arriving after higher-HLC
    // entries on the same WAL partition under parallel cross-leaf
    // appends; the offset cursor is monotonic-per-partition by
    // construction and survives those interleavings.
    private readonly ChangeFeedCursor[,] _cursors;
    private readonly Task[,] _tasks;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _gate = new();

    /// <summary>
    /// Per-receiver content-hash cache keyed by <c>(receiverIdx, key)</c>
    /// holding the last value bytes the pump applied to that receiver.
    /// Typed-CRDT state-merge dispatch goes through
    /// <see cref="ILattice.SetIfVersionAsync"/>, which generates a fresh
    /// local HLC at the receiver and emits a new WAL entry stamped with
    /// the foreign origin. That entry then ships onward, gets applied on
    /// the next hop (HWM dedupe sees a newer HLC and lets it through),
    /// and the cycle repeats - an infinite ping-pong even after values
    /// have converged. Skipping bytes-identical re-deliveries breaks the
    /// loop without touching production dispatch: convergent CRDT merge
    /// is value-idempotent, so identical bytes carry no new state.
    /// </summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<(int Receiver, string Key), byte[]> _lastAppliedBytes = new();

    /// <summary>
    /// Errors surfaced by background pump tasks (one entry per occurrence,
    /// in arrival order). Surfaced for diagnostic assertions; chaos tests
    /// proper assert on convergence rather than on the absence of errors.
    /// </summary>
    public System.Collections.Concurrent.ConcurrentQueue<Exception> PumpErrors { get; } = new();

    /// <summary>
    /// Creates a delivery pump bound to <paramref name="fixture"/>'s
    /// sites. <paramref name="pollInterval"/> defaults to 50 ms - short
    /// enough that chaos workloads run in seconds, long enough to
    /// avoid grain-RPC saturation.
    /// </summary>
    public ChaosDeliveryPump(MultiSiteClusterFixture fixture, string treeName, TimeSpan? pollInterval = null)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        ArgumentNullException.ThrowIfNull(treeName);
        _fixture = fixture;
        _treeName = treeName;
        _pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(50);

        var n = fixture.SiteCount;
        _partitioned = new bool[n, n];
        _cursors = new ChangeFeedCursor[n, n];
        _tasks = new Task[n, n];

        for (var i = 0; i < n; i++)
        {
            for (var j = 0; j < n; j++)
            {
                _cursors[i, j] = ChangeFeedCursor.Initial;
            }
        }
    }

    /// <summary>Starts every <c>(i, j)</c> pump where <c>i != j</c>.</summary>
    public void Start()
    {
        var n = _fixture.SiteCount;
        for (var i = 0; i < n; i++)
        {
            for (var j = 0; j < n; j++)
            {
                if (i == j)
                {
                    continue;
                }

                var sender = i;
                var receiver = j;
                _tasks[i, j] = Task.Run(() => RunPumpAsync(sender, receiver, _cts.Token));
            }
        }
    }

    /// <summary>Drops every <c>(sender, *)</c> and <c>(*, sender)</c> edge for site <paramref name="site"/>.</summary>
    public void IsolateSite(int site)
    {
        lock (_gate)
        {
            for (var k = 0; k < _fixture.SiteCount; k++)
            {
                if (k == site)
                {
                    continue;
                }

                _partitioned[site, k] = true;
                _partitioned[k, site] = true;
            }
        }
    }

    /// <summary>Re-enables every edge involving site <paramref name="site"/>.</summary>
    public void HealSite(int site)
    {
        lock (_gate)
        {
            for (var k = 0; k < _fixture.SiteCount; k++)
            {
                if (k == site)
                {
                    continue;
                }

                _partitioned[site, k] = false;
                _partitioned[k, site] = false;
            }
        }
    }

    /// <summary>Drops the directed edge <c>(senderIdx → receiverIdx)</c>.</summary>
    public void Partition(int senderIdx, int receiverIdx)
    {
        lock (_gate)
        {
            _partitioned[senderIdx, receiverIdx] = true;
        }
    }

    /// <summary>Heals the directed edge <c>(senderIdx → receiverIdx)</c>.</summary>
    public void Heal(int senderIdx, int receiverIdx)
    {
        lock (_gate)
        {
            _partitioned[senderIdx, receiverIdx] = false;
        }
    }

    /// <summary>
    /// Heals every edge and waits until every <c>(sender → receiver)</c>
    /// cursor catches up to the sender's WAL tail HLC. Times out after
    /// <paramref name="timeout"/> with an explicit failure so a stuck
    /// pump does not hang the test forever.
    /// </summary>
    public async Task HealAllAndDrainAsync(TimeSpan timeout)
    {
        lock (_gate)
        {
            for (var i = 0; i < _fixture.SiteCount; i++)
            {
                for (var j = 0; j < _fixture.SiteCount; j++)
                {
                    _partitioned[i, j] = false;
                }
            }
        }

        await DrainAsync(timeout);
    }

    /// <summary>
    /// Waits for every healed edge to ship every committed entry from its
    /// sender. The drain criterion is "every edge's cursor has reached its
    /// sender's WAL tail HLC for <see cref="DrainStabilityWindow"/>
    /// consecutive polls" - multi-poll stability guards against a transient
    /// race where the (i→j) pump's <c>ApplyAsync</c> has returned (so
    /// <c>cursor[i,j]</c> caught up to the entry's HLC) but the entry has
    /// not yet been re-emitted on site <c>j</c>'s change feed (so the
    /// gossip-forward pumps <c>(j→k)</c> have not yet observed it). Without
    /// the window, a single lucky poll where every <c>(i,j)</c> happens to
    /// satisfy <c>cursor &gt;= tailHlc</c> would short-circuit the drain
    /// before forward propagation has a chance to start. Throws on
    /// <paramref name="timeout"/>.
    /// </summary>
    public async Task DrainAsync(TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        var n = _fixture.SiteCount;
        var consecutiveStable = 0;
        while (DateTime.UtcNow < deadline)
        {
            var drained = true;
            for (var i = 0; i < n && drained; i++)
            {
                // Phase D1c: drain predicate is now "every edge's
                // ChangeFeedCursor equals the sender's current WAL
                // high-water-mark cursor". The HLC-based comparison
                // (cursor < tailHlc) is unsafe under the offset cursor
                // shape; we capture the sender's current cursor via
                // IChangeFeed.GetCurrentCursorAsync and structural-equal
                // it against each edge's reported cursor.
                var tailCursor = await _fixture.ChangeFeedOf(i).GetCurrentCursorAsync(_treeName);
                for (var j = 0; j < n; j++)
                {
                    if (i == j)
                    {
                        continue;
                    }

                    bool partitioned;
                    ChangeFeedCursor cursor;
                    lock (_gate)
                    {
                        partitioned = _partitioned[i, j];
                        cursor = _cursors[i, j];
                    }

                    if (partitioned)
                    {
                        // Partitioned edges are not a drain blocker - heal first.
                        continue;
                    }

                    if (cursor != tailCursor)
                    {
                        drained = false;
                        break;
                    }
                }
            }

            if (drained)
            {
                consecutiveStable++;
                if (consecutiveStable >= DrainStabilityWindow)
                {
                    return;
                }
            }
            else
            {
                consecutiveStable = 0;
            }

            await Task.Delay(_pollInterval);
        }

        throw new TimeoutException($"ChaosDeliveryPump.DrainAsync timed out after {timeout}.");
    }

    /// <summary>
    /// Number of consecutive polls during which every edge must satisfy
    /// <c>cursor &gt;= tailHlc</c> before <see cref="DrainAsync"/> returns.
    /// At <see cref="_pollInterval"/> = 50 ms this gives a 150 ms settle
    /// window, which empirically covers the worst-case gap between a
    /// receiver's <c>ApplyAsync</c> returning and the entry becoming
    /// visible on its outbound change feed for forward gossip.
    /// </summary>
    private const int DrainStabilityWindow = 3;

    private async Task RunPumpAsync(int senderIdx, int receiverIdx, CancellationToken ct)
    {
        var feed = _fixture.ChangeFeedOf(senderIdx);
        var applier = _fixture.ApplierOf(receiverIdx);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                bool partitioned;
                ChangeFeedCursor cursor;
                lock (_gate)
                {
                    partitioned = _partitioned[senderIdx, receiverIdx];
                    cursor = _cursors[senderIdx, receiverIdx];
                }

                if (partitioned)
                {
                    await Task.Delay(_pollInterval, ct).ConfigureAwait(false);
                    continue;
                }

                // Phase D1c: capture the sender's current cursor BEFORE
                // we start consuming, so we know what to advance to
                // after Subscribe drains the snapshot. Using the captured
                // cursor (rather than the highest per-entry offset we
                // saw) is safe because Subscribe takes a WAL snapshot
                // at call time; entries committed after the capture
                // simply land in the next poll iteration.
                var nextCursor = await feed.GetCurrentCursorAsync(_treeName, ct).ConfigureAwait(false);
                var receiverClusterId = MultiSiteClusterFixture.ClusterIdFor(receiverIdx);
                // Truncation guard: track whether the inner foreach
                // exited via the mid-iteration partition check so the
                // post-loop cursor advance only fires when Subscribe
                // ran to completion. Advancing the cursor to
                // `nextCursor` on a partition-truncated iteration
                // would silently drop every entry between the last
                // applied entry and the captured `nextCursor`, which
                // is exactly the data-loss shape that motivated
                // Phase D1c (the regression here: under the old
                // per-entry HLC-advance model, a mid-stream break
                // left `newCursor` at the last applied entry's HLC,
                // so partition cycles never skipped entries; the
                // pre-captured `nextCursor` model needs an explicit
                // guard).
                var truncatedByPartition = false;
                await foreach (var entry in feed.Subscribe(_treeName, cursor, includeLocalOrigin: true, ct).ConfigureAwait(false))
                {
                    // Re-check the partition gate per entry so a partition that
                    // opens mid-stream truncates the in-flight delivery rather
                    // than blocking until the next poll cycle.
                    bool partitionedNow;
                    lock (_gate)
                    {
                        partitionedNow = _partitioned[senderIdx, receiverIdx];
                    }

                    if (partitionedNow)
                    {
                        truncatedByPartition = true;
                        break;
                    }

                    // Per-target cycle-break: never forward an entry back
                    // to the cluster that originally authored it. Mirrors
                    // the production ship loop's per-peer origin filter.
                    // The receiver-side ReplicationApplier also enforces
                    // this as defence-in-depth.
                    if (string.Equals(entry.OriginClusterId, receiverClusterId, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    // Value-idempotent dedupe: if the receiver's last
                    // applied bytes for this key match the incoming
                    // entry's bytes, skip the apply. See _lastAppliedBytes.
                    //
                    // The dedupe is bypassed for atomic-batch entries
                    // (AtomicBatchSize > 0). The atomic-batch wire metadata
                    // (AtomicBatchSize/AtomicBatchIndex/TransactionId) is
                    // preserved on every emit so the receiver-side
                    // prepared-Set / prepared-Delete / terminal-mark
                    // primitive can route Set/Delete entries through the
                    // per-tx pending bucket. The bypass is kept because
                    // (a) value-bytes dedupe on a write whose apply path
                    // routes through the pending bucket would short-circuit
                    // before the terminal mark fires, and (b) atomic-batch
                    // correctness on the current apply path is provided by
                    // RecentApplyCache's causal-identity dedupe and the
                    // per-origin HWM cursor. The pump-level value-bytes
                    // dedupe is sound only on the non-atomic point-apply
                    // path, where it breaks the typed-CRDT ping-pong cycle
                    // documented on
                    // _lastAppliedBytes.
                    if (entry.AtomicBatchSize == 0
                        && entry.Value is { } incomingBytes
                        && _lastAppliedBytes.TryGetValue((receiverIdx, entry.Key), out var lastBytes)
                        && BytesEqual(lastBytes, incomingBytes))
                    {
                        continue;
                    }

                    await applier.ApplyAsync(entry, ct).ConfigureAwait(false);
                    if (entry.AtomicBatchSize == 0 && entry.Value is { } applied)
                    {
                        _lastAppliedBytes[(receiverIdx, entry.Key)] = applied;
                    }
                }

                // Phase D1c: advance the edge cursor to the pre-loop
                // captured nextCursor (the sender's WAL high-water-mark
                // at the time we started the Subscribe call). Entries
                // that landed in the WAL during our consume are picked
                // up on the next poll iteration. Using the captured
                // snapshot avoids edge cases where an entry was skipped
                // (origin-filter or value-bytes dedupe) and we never
                // saw its offset to advance to.
                //
                // Skip the advance entirely when the inner foreach
                // exited via the mid-iteration partition check: the
                // captured `nextCursor` is necessarily AT OR AFTER the
                // entry that triggered the truncation, so advancing
                // would skip every still-unshipped entry between the
                // last applied entry and `nextCursor`. The next
                // iteration (after the partition heals) re-runs
                // Subscribe from the unchanged cursor and re-delivers
                // the truncated tail.
                if (!truncatedByPartition && !nextCursor.Equals(cursor))
                {
                    lock (_gate)
                    {
                        _cursors[senderIdx, receiverIdx] = nextCursor;
                    }
                }

                await Task.Delay(_pollInterval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                // Chaos pumps are best-effort - a transient grain failure
                // simply retries on the next iteration. Surface the error
                // to PumpErrors for diagnostic inspection without aborting
                // the loop. The convergence assertion at the end of the
                // test is the source of truth.
                PumpErrors.Enqueue(ex);
                await Task.Delay(_pollInterval, ct).ConfigureAwait(false);
            }
        }
    }

    private static bool BytesEqual(byte[] a, byte[] b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }
        if (a.Length != b.Length)
        {
            return false;
        }
        return a.AsSpan().SequenceEqual(b);
    }

    /// <summary>Cancels every running pump and awaits termination.</summary>
    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        var n = _fixture.SiteCount;
        for (var i = 0; i < n; i++)
        {
            for (var j = 0; j < n; j++)
            {
                var task = _tasks[i, j];
                if (task is null)
                {
                    continue;
                }
                try
                {
                    await task.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
        _cts.Dispose();
    }
}
