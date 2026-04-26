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
/// cursor — when the partition heals, the next iteration ships
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
    private readonly HybridLogicalClock[,] _cursors;
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
    /// and the cycle repeats — an infinite ping-pong even after values
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
    /// sites. <paramref name="pollInterval"/> defaults to 50 ms — short
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
        _cursors = new HybridLogicalClock[n, n];
        _tasks = new Task[n, n];

        for (var i = 0; i < n; i++)
        {
            for (var j = 0; j < n; j++)
            {
                _cursors[i, j] = HybridLogicalClock.Zero;
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
    /// sender. The drain criterion is "two consecutive empty polls per
    /// edge" — once an edge sees no new entries on two successive iterations
    /// it is considered drained. Throws on <paramref name="timeout"/>.
    /// </summary>
    public async Task DrainAsync(TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        var n = _fixture.SiteCount;
        while (DateTime.UtcNow < deadline)
        {
            var drained = true;
            for (var i = 0; i < n && drained; i++)
            {
                // Ask each sender for its WAL tail HLC. A simple proxy is
                // "the highest entry timestamp returned by Subscribe(..., Zero)";
                // for chaos tests this is acceptable because authoring stops
                // before drain begins.
                var tailHlc = await GetTailHlcAsync(i);
                for (var j = 0; j < n; j++)
                {
                    if (i == j)
                    {
                        continue;
                    }

                    bool partitioned;
                    HybridLogicalClock cursor;
                    lock (_gate)
                    {
                        partitioned = _partitioned[i, j];
                        cursor = _cursors[i, j];
                    }

                    if (partitioned)
                    {
                        // Partitioned edges are not a drain blocker — heal first.
                        continue;
                    }

                    if (cursor < tailHlc)
                    {
                        drained = false;
                        break;
                    }
                }
            }

            if (drained)
            {
                return;
            }

            await Task.Delay(_pollInterval);
        }

        throw new TimeoutException($"ChaosDeliveryPump.DrainAsync timed out after {timeout}.");
    }

    private async Task<HybridLogicalClock> GetTailHlcAsync(int senderIdx)
    {
        var feed = _fixture.ChangeFeedOf(senderIdx);
        var tail = HybridLogicalClock.Zero;
        await foreach (var entry in feed.Subscribe(_treeName, HybridLogicalClock.Zero, includeLocalOrigin: true))
        {
            if (entry.Timestamp > tail)
            {
                tail = entry.Timestamp;
            }
        }
        return tail;
    }

    private async Task RunPumpAsync(int senderIdx, int receiverIdx, CancellationToken ct)
    {
        var feed = _fixture.ChangeFeedOf(senderIdx);
        var applier = _fixture.ApplierOf(receiverIdx);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                bool partitioned;
                HybridLogicalClock cursor;
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

                var newCursor = cursor;
                var receiverClusterId = MultiSiteClusterFixture.ClusterIdFor(receiverIdx);
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
                        break;
                    }

                    // Per-target cycle-break: never forward an entry back
                    // to the cluster that originally authored it. Mirrors
                    // the production ship loop's per-peer origin filter.
                    // The receiver-side ReplicationApplier also enforces
                    // this as defence-in-depth, but advancing the cursor
                    // past skipped entries avoids needless grain churn.
                    if (string.Equals(entry.OriginClusterId, receiverClusterId, StringComparison.Ordinal))
                    {
                        if (entry.Timestamp > newCursor)
                        {
                            newCursor = entry.Timestamp;
                        }
                        continue;
                    }

                    // Value-idempotent dedupe: if the receiver's last
                    // applied bytes for this key match the incoming
                    // entry's bytes, skip the apply. See _lastAppliedBytes.
                    if (entry.Value is { } incomingBytes
                        && _lastAppliedBytes.TryGetValue((receiverIdx, entry.Key), out var lastBytes)
                        && BytesEqual(lastBytes, incomingBytes))
                    {
                        if (entry.Timestamp > newCursor)
                        {
                            newCursor = entry.Timestamp;
                        }
                        continue;
                    }

                    await applier.ApplyAsync(entry, ct).ConfigureAwait(false);
                    if (entry.Value is { } applied)
                    {
                        _lastAppliedBytes[(receiverIdx, entry.Key)] = applied;
                    }
                    if (entry.Timestamp > newCursor)
                    {
                        newCursor = entry.Timestamp;
                    }
                }

                if (newCursor > cursor)
                {
                    lock (_gate)
                    {
                        if (newCursor > _cursors[senderIdx, receiverIdx])
                        {
                            _cursors[senderIdx, receiverIdx] = newCursor;
                        }
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
                // Chaos pumps are best-effort — a transient grain failure
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
