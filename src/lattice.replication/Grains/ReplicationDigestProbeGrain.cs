using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;
using System.Globalization;

namespace Orleans.Lattice.Replication.Grains;
/// <summary>
/// Default <see cref="IReplicationDigestProbeGrain"/> implementation - the
/// first detection stage of the cross-cluster anti-entropy chain. On a
/// low-frequency jittered cadence it reads each shard's local
/// <see cref="LeafProjectionDigest"/> via
/// <see cref="ILattice.GetLeafProjectionDigestAsync(int, CancellationToken)"/>,
/// fetches the corresponding digest from every peer over the
/// <see cref="IReplicationDigestProbeTransport"/>, and classifies the
/// comparison through <see cref="DigestProbeComparer"/>, emitting the
/// <see cref="LatticeReplicationMetrics.DigestProbeCompared"/> and
/// <see cref="LatticeReplicationMetrics.DigestProbeMismatch"/> counters.
/// <para>
/// The pass is strictly read-only: it never mutates data or advances any
/// cursor. It ships dark - gated on
/// <see cref="LatticeReplicationOptions.DigestProbeEnabled"/> (default
/// off) - so an un-opted host pays nothing beyond the grain activation.
/// </para>
/// <para>
/// Trees whose configured
/// <see cref="LatticeOptions.MaintainProjectionDigest"/> is
/// <see langword="false"/> are skipped for the pass (the skip is not
/// permanent because the option can be flipped back). When the local
/// digest read throws because projection-digest maintenance is latched
/// permanently disabled for the tree, the scheduler stops probing for the
/// lifetime of the activation.
/// </para>
/// </summary>
internal sealed class ReplicationDigestProbeGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    ILogger<ReplicationDigestProbeGrain> logger,
    IOptionsMonitor<LatticeReplicationOptions> replicationOptions,
    IOptionsMonitor<LatticeOptions> latticeOptions,
    IReplicationTopology topology,
    IReplicationDigestProbeTransport probeTransport,
    IShardCountProvider shardCounts,
    IGrainFactory grainFactory,
    [PersistentState("replication-digest-probe", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationDigestProbeState> state)
    : CoordinatorGrain<ReplicationDigestProbeGrain>(context, reminderRegistry, logger),
      IReplicationDigestProbeGrain
{
    private readonly IOptionsMonitor<LatticeReplicationOptions> _replicationOptions =
        replicationOptions ?? throw new ArgumentNullException(nameof(replicationOptions));
    private readonly IOptionsMonitor<LatticeOptions> _latticeOptions =
        latticeOptions ?? throw new ArgumentNullException(nameof(latticeOptions));
    private readonly IReplicationTopology _topology =
        topology ?? throw new ArgumentNullException(nameof(topology));
    private readonly IReplicationDigestProbeTransport _probeTransport =
        probeTransport ?? throw new ArgumentNullException(nameof(probeTransport));
    private readonly IShardCountProvider _shardCounts =
        shardCounts ?? throw new ArgumentNullException(nameof(shardCounts));
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly Random _random = new();

    /// <summary>
    /// Set once the local digest read reports projection-digest
    /// maintenance is permanently disabled (latched) for the tree. While
    /// set, the phase pump short-circuits so no further probe passes run
    /// for this activation.
    /// </summary>
    private bool _permanentlyDisabled;

    /// <summary>
    /// Cached jittered interval (ticks). Rolled once per probe pass so the
    /// next pass fires at a randomised offset around
    /// <see cref="LatticeReplicationOptions.DigestProbeInterval"/>. Zero
    /// means "not yet rolled".
    /// </summary>
    private long _jitteredIntervalTicks;

    private string _treeName = "";
    private bool _treeNameResolved;

    private string TreeName
    {
        get
        {
            if (!_treeNameResolved)
            {
                _treeName = Context.GrainId.Key.ToString() ?? "";
                _treeNameResolved = true;
            }
            return _treeName;
        }
    }

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "digest-probe-keepalive";

    /// <inheritdoc />
    protected override TimeSpan KeepaliveReminderPeriod => TimeSpan.FromSeconds(60);

    /// <inheritdoc />
    protected override TimeSpan PhaseTimerPeriod => TimeSpan.FromSeconds(30);

    /// <inheritdoc />
    protected override bool InProgress => true; // Always running.

    /// <inheritdoc />
    protected override string LogContext => $"digest-probe {TreeName}";

    /// <inheritdoc />
    public async Task EnsureActiveAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (string.IsNullOrEmpty(TreeName))
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationDigestProbeGrain)} activation key is empty; expected the replicated tree name.");
        }
        await StartCoordinatorAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (_permanentlyDisabled)
        {
            return;
        }

        var options = _replicationOptions.Get(TreeName);
        if (!options.DigestProbeEnabled)
        {
            // Dark by default: an un-opted host never runs a comparison
            // pass. The grain stays activated (cheap) so flipping the
            // option on at runtime starts probing without a restart.
            return;
        }

        var nowTicks = DateTime.UtcNow.Ticks;
        var interval = TimeSpan.FromTicks(EnsureJitteredInterval(options));
        if (!ShouldRunCadence(nowTicks, state.State.LastProbeTicks, interval))
        {
            return;
        }

        // Configured opt-out: the tree has digest maintenance disabled.
        // Skip this pass but advance the cadence so we re-check on the
        // next interval (the option can be flipped back, unlike the
        // permanent latch handled below).
        if (!_latticeOptions.Get(TreeName).MaintainProjectionDigest)
        {
            await AdvanceCadenceAsync(nowTicks).ConfigureAwait(true);
            return;
        }

        var peers = _topology.CurrentPeers;
        if (peers.Count == 0)
        {
            await AdvanceCadenceAsync(nowTicks).ConfigureAwait(true);
            return;
        }

        int shardCount;
        try
        {
            shardCount = await _shardCounts.GetShardCountAsync(TreeName, CancellationToken.None).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            // Could not resolve the shard count this tick; do not advance
            // the cadence so the next phase tick retries.
            Logger.LogWarning(ex,
                "Resolving shard count failed for {Context}; will retry the digest probe on the next phase tick",
                LogContext);
            return;
        }

        var lattice = _grainFactory.GetGrain<ILattice>(TreeName);
        var latched = false;

        for (var shard = 0; shard < shardCount && !latched; shard++)
        {
            LeafProjectionDigest local;
            try
            {
                local = await lattice.GetLeafProjectionDigestAsync(shard, CancellationToken.None).ConfigureAwait(true);
            }
            catch (InvalidOperationException)
            {
                // Projection-digest maintenance is disabled/latched for
                // the tree. Stop probing permanently for this activation.
                latched = true;
                break;
            }
            catch (Exception ex)
            {
                // Transient local read failure for this shard; skip it
                // this pass and continue with the next shard.
                Logger.LogWarning(ex,
                    "Reading local projection digest for shard {Shard} failed for {Context}; skipping the shard this pass",
                    shard, LogContext);
                continue;
            }

            foreach (var peer in peers)
            {
                if (string.IsNullOrEmpty(peer))
                {
                    continue;
                }

                try
                {
                    var response = await _probeTransport
                        .ProbeDigestAsync(
                            peer,
                            new DigestProbeRequest { TreeName = TreeName, ShardIndex = shard },
                            CancellationToken.None)
                        .ConfigureAwait(true);

                    var outcome = DigestProbeComparer.Compare(local, response);
                    RecordCompared(TreeName, shard, peer, outcome);
                    if (outcome == DigestProbeOutcome.Mismatch)
                    {
                        RecordMismatch(TreeName, shard, peer);
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex,
                        "Digest probe to peer {Peer} for shard {Shard} failed for {Context}; will retry on the next cadence",
                        peer, shard, LogContext);
                }
            }
        }

        if (latched)
        {
            _permanentlyDisabled = true;
            Logger.LogInformation(
                "Projection-digest maintenance is disabled for {Context}; the anti-entropy digest probe will not run again for this activation",
                LogContext);
            return;
        }

        await AdvanceCadenceAsync(nowTicks).ConfigureAwait(true);
    }

    private long EnsureJitteredInterval(LatticeReplicationOptions options)
    {
        if (_jitteredIntervalTicks <= 0)
        {
            _jitteredIntervalTicks = DigestProbeScheduling
                .ApplyJitter(options.DigestProbeInterval, options.DigestProbeJitter, _random)
                .Ticks;
        }
        return _jitteredIntervalTicks;
    }

    private async Task AdvanceCadenceAsync(long nowTicks)
    {
        var prev = state.State.LastProbeTicks;
        state.State.LastProbeTicks = nowTicks;
        // Re-roll the jittered interval so the next pass fires at a fresh
        // randomised offset.
        _jitteredIntervalTicks = 0;
        try
        {
            await state.WriteStateAsync().ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            // Restore the in-memory cadence stamp so the next phase tick
            // retries rather than skipping a full interval on a transient
            // persist failure.
            state.State.LastProbeTicks = prev;
            Logger.LogWarning(ex,
                "Persisting the digest-probe cadence stamp failed for {Context}; will retry on the next phase tick",
                LogContext);
        }
    }

    private static void RecordCompared(string tree, int shard, string peer, DigestProbeOutcome outcome)
    {
        LatticeReplicationMetrics.DigestProbeCompared.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, tree),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagShard, shard.ToString(CultureInfo.InvariantCulture)),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peer),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, LatticeReplicationMetrics.DigestProbeOutcomeTag(outcome)));
    }

    private static void RecordMismatch(string tree, int shard, string peer)
    {
        LatticeReplicationMetrics.DigestProbeMismatch.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, tree),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagShard, shard.ToString(CultureInfo.InvariantCulture)),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peer));
    }

    private static bool ShouldRunCadence(long nowTicks, long lastTicks, TimeSpan interval)
    {
        if (lastTicks == 0)
        {
            return true; // Never run before - fire on first tick.
        }
        return nowTicks - lastTicks >= interval.Ticks;
    }
}
