using System.Net.Http;
using Microsoft.Extensions.Logging;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace MultiSiteManufacturing.Host.Replication.Grains;

/// <summary>
/// Reminder-driven replicator. See <see cref="IReplicatorGrain"/> for
/// the contract and plan §13.4 for the overall design.
/// </summary>
/// <remarks>
/// <para>
/// Persistent state is stored via Orleans grain storage on the
/// <c>msmfgGrainState</c> table — <i>not</i> via the lattice — because
/// the cursor is operational state with a different lifecycle than
/// domain data. A wipe of the grain state wouldn't lose any lattice
/// data; the replicator just re-ships from the start of the replog
/// (bounded O(N), same behaviour as the anti-entropy sweep).
/// </para>
/// </remarks>
internal sealed class ReplicatorGrain(
    [PersistentState("replicator", "msmfgGrainState")] IPersistentState<ReplicatorState> state,
    IGrainFactory grains,
    ReplicationTopology topology,
    ReplicationHttpClient http,
    ILogger<ReplicatorGrain> logger) : Grain, IReplicatorGrain, IRemindable
{
    private const int BatchSize = 128;
    // Orleans reminders have a 1-minute minimum period, so we use the
    // reminder purely as a durable keep-alive: it re-activates the
    // grain after silo restart / idle deactivation. The actual work
    // cadence is driven by a non-durable grain timer registered in
    // OnActivateAsync — grain timers have no minimum period and are
    // disposed automatically when the grain deactivates.
    private const string KeepAliveReminder = "keepalive";
    private static readonly TimeSpan KeepAlivePeriod = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan TickPeriod = TimeSpan.FromSeconds(3);

    private string _tree = string.Empty;
    private string _peerName = string.Empty;
    private ReplicationPeer? _peer;
    private IDisposable? _tickTimer;

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var key = this.GetPrimaryKeyString();
        var pipe = key.IndexOf('|');
        if (pipe <= 0 || pipe == key.Length - 1)
        {
            throw new InvalidOperationException($"ReplicatorGrain key must be '{{tree}}|{{peer}}' (got '{key}').");
        }
        _tree = key[..pipe];
        _peerName = key[(pipe + 1)..];
        _peer = topology.Peers.FirstOrDefault(p => string.Equals(p.Name, _peerName, StringComparison.Ordinal));

        if (_peer is null)
        {
            logger.LogWarning(
                "Replicator for tree {Tree} peer {Peer}: peer not in topology — grain will be idle",
                _tree, _peerName);
        }
        else
        {
            await this.RegisterOrUpdateReminder(KeepAliveReminder, dueTime: KeepAlivePeriod, period: KeepAlivePeriod);
            _tickTimer = this.RegisterGrainTimer(
                TickAsync,
                dueTime: TimeSpan.FromSeconds(1),
                period: TickPeriod);
        }

        await base.OnActivateAsync(cancellationToken);
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _tickTimer?.Dispose();
        _tickTimer = null;
        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    public async Task TickAsync()
    {
        if (_peer is null)
        {
            return;
        }

        try
        {
            var shipped = await ShipOneBatchAsync(CancellationToken.None);
            if (shipped > 0 && state.State.ConsecutiveErrors != 0)
            {
                state.State = state.State with { ConsecutiveErrors = 0 };
                await state.WriteStateAsync();
            }

            // If the batch was full there's likely more pending —
            // kick the next tick immediately instead of waiting for
            // the reminder.
            if (shipped >= BatchSize)
            {
                _ = Task.Run(async () =>
                {
                    try { await TickAsync(); }
                    catch (Exception ex) { logger.LogWarning(ex, "Cascading tick failed"); }
                });
            }
        }
        catch (Exception ex)
        {
            state.State = state.State with { ConsecutiveErrors = state.State.ConsecutiveErrors + 1 };
            await state.WriteStateAsync();
            logger.LogWarning(ex,
                "Replicator for tree {Tree} peer {Peer} tick failed (consecutive errors = {Count})",
                _tree, _peerName, state.State.ConsecutiveErrors);
        }
    }

    public Task<ReplicatorStatus> GetStatusAsync() => Task.FromResult(new ReplicatorStatus
    {
        Tree = _tree,
        Peer = _peerName,
        Cursor = state.State.Cursor,
        LastContactUtc = state.State.LastContactUtc,
        TotalRowsShipped = state.State.TotalRowsShipped,
        ConsecutiveErrors = state.State.ConsecutiveErrors,
    });

    public Task ReceiveReminder(string reminderName, TickStatus status) =>
        string.Equals(reminderName, KeepAliveReminder, StringComparison.Ordinal)
            ? TickAsync()
            : Task.CompletedTask;

    private async Task<int> ShipOneBatchAsync(CancellationToken cancellationToken)
    {
        if (_peer is null)
        {
            return 0;
        }

        var replogTreeId = ReplicationConstants.ReplogTreePrefix + _tree;
        var replog = grains.GetGrain<ILattice>(replogTreeId);
        var primary = grains.GetGrain<ILattice>(_tree);

        var start = ReplogKeyCodec.StartAfter(state.State.Cursor, topology.LocalCluster);
        var end = ReplogKeyCodec.ReplogEndBound;

        // Collect up to BatchSize raw replog entries.
        var raw = new List<(string ReplogKey, HybridLogicalClock Hlc, ReplicationOp Op, string OriginalKey)>(BatchSize);
        await foreach (var kvp in replog.ScanEntriesAsync(start, end, cancellationToken: cancellationToken))
        {
            if (ReplogKeyCodec.TryDecode(kvp.Key, out var hlc, out _, out var op, out var origKey))
            {
                raw.Add((kvp.Key, hlc, op, origKey));
                if (raw.Count >= BatchSize)
                {
                    break;
                }
            }
        }

        if (raw.Count == 0)
        {
            return 0;
        }

        // Dedupe by original key, keep highest HLC (latest write wins
        // in a single batch — the peer still applies one entry per
        // distinct key).
        var latestPerKey = new Dictionary<string, (HybridLogicalClock Hlc, ReplicationOp Op)>(StringComparer.Ordinal);
        var highestInBatch = state.State.Cursor;
        foreach (var r in raw)
        {
            if (!latestPerKey.TryGetValue(r.OriginalKey, out var existing) || r.Hlc.CompareTo(existing.Hlc) > 0)
            {
                latestPerKey[r.OriginalKey] = (r.Hlc, r.Op);
            }
            if (r.Hlc.CompareTo(highestInBatch) > 0)
            {
                highestInBatch = r.Hlc;
            }
        }

        // Materialise the shipping batch — fetch the current value
        // for each key from the primary tree.
        var entries = new List<ReplicationEntry>(latestPerKey.Count);
        foreach (var (origKey, meta) in latestPerKey)
        {
            byte[]? value = null;
            if (meta.Op == ReplicationOp.Set)
            {
                value = await primary.GetAsync(origKey, cancellationToken);
            }
            entries.Add(new ReplicationEntry
            {
                Key = origKey,
                Op = value is null ? ReplicationOp.Delete : meta.Op,
                Value = value,
                SourceHlc = meta.Hlc,
            });
        }

        entries.Sort((a, b) => a.SourceHlc.CompareTo(b.SourceHlc));

        var batch = new ReplicationBatch
        {
            SourceCluster = topology.LocalCluster,
            Tree = _tree,
            Entries = entries,
        };

        var ack = await http.SendAsync(_peer, batch, cancellationToken);

        state.State = state.State with
        {
            Cursor = highestInBatch,
            LastContactUtc = DateTimeOffset.UtcNow,
            TotalRowsShipped = state.State.TotalRowsShipped + ack.Applied,
        };
        await state.WriteStateAsync();

        logger.LogInformation(
            "Replicator tree={Tree} peer={Peer} shipped={Shipped} applied={Applied} cursor={Cursor}",
            _tree, _peerName, entries.Count, ack.Applied, state.State.Cursor);

        return raw.Count;
    }
}

/// <summary>
/// Persistent state for <see cref="ReplicatorGrain"/>. Stored on the
/// <c>msmfgGrainState</c> Orleans grain storage (Azure Table Storage
/// in dev), not via the lattice.
/// </summary>
[GenerateSerializer]
internal sealed record ReplicatorState
{
    [Id(0)] public HybridLogicalClock Cursor { get; init; }
    [Id(1)] public DateTimeOffset LastContactUtc { get; init; }
    [Id(2)] public long TotalRowsShipped { get; init; }
    [Id(3)] public int ConsecutiveErrors { get; init; }
}
