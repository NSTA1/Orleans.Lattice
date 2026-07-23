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
    IReplicationTransport replicationTransport,
    IReplicationBatchEncoder batchEncoder,
    IShardCountProvider shardCounts,
    IGrainFactory grainFactory,
    ISnapshotProvider snapshotProvider,
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
    private readonly IReplicationTransport _replicationTransport =
        replicationTransport ?? throw new ArgumentNullException(nameof(replicationTransport));
    private readonly IReplicationBatchEncoder _batchEncoder =
        batchEncoder ?? throw new ArgumentNullException(nameof(batchEncoder));
    private readonly IShardCountProvider _shardCounts =
        shardCounts ?? throw new ArgumentNullException(nameof(shardCounts));
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly ISnapshotProvider _snapshotProvider =
        snapshotProvider ?? throw new ArgumentNullException(nameof(snapshotProvider));

    private readonly Random _random = new();

    /// <summary>
    /// Per-activation guard that enforces the per-(tree, peer) remediation
    /// traffic budget and the per-(tree, peer) remediation circuit breaker, and
    /// drives the process-wide
    /// <see cref="LatticeReplicationMetrics.DigestRemediationDisabledName"/>
    /// gauge. Accounting is in-process and scoped to this digest-probe
    /// activation, which is per shard/tree.
    /// </summary>
    private readonly RemediationGuard _remediationGuard = new();

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

        // The anti-entropy probe is trusted in-silo infrastructure, not a user
        // operation, yet its local reads (the per-shard projection digest and the
        // read-only Merkle-walk descent) funnel through the same fail-closed
        // data-plane access gate as user reads. Absent an ambient identity they
        // resolve to the anonymous subject and a deny-by-default tree refuses
        // them - which would silently disable remediation on exactly the secured
        // estates that need it. Mark the pass system-origin so the gate's
        // documented infrastructure bypass applies; the flag flows on outgoing
        // in-silo grain calls (it deliberately does not cross the replication
        // transport, so each peer re-establishes its own scope server-side).
        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();

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

                        // Localise stage: when the host has opted into the
                        // read-only Merkle-walk drift localisation, narrow the
                        // shard-level mismatch to a leaf (or small leaf set) by
                        // walking the local internal-node tree by separator-key
                        // range. Strictly read-only and best-effort - a failure
                        // never disturbs the detect-stage cadence.
                        if (options.MerkleWalkEnabled)
                        {
                            await TryLocaliseDriftAsync(lattice, options, shard, peer)
                                .ConfigureAwait(true);
                        }
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

    /// <summary>
    /// Runs the read-only Merkle-walk drift-localisation pass for a single
    /// shard-level mismatch against a single peer. Resolves the physical tree
    /// id once, builds a local-tree adapter over the shard's internal-node
    /// grains, and delegates the descent and metric emission to
    /// <see cref="MerkleWalkLocaliser.WalkAsync"/>. Strictly read-only and
    /// best-effort: any failure is logged and swallowed so the detect-stage
    /// cadence is never disturbed.
    /// </summary>
    private async Task TryLocaliseDriftAsync(ILattice lattice, LatticeReplicationOptions options, int shard, string peer)
    {
        try
        {
            var physicalTreeId = await EnsurePhysicalTreeIdAsync(lattice).ConfigureAwait(true);
            var localTree = new GrainMerkleWalkLocalTree(_grainFactory, physicalTreeId, shard);
            var outcome = await MerkleWalkLocaliser.WalkAsync(
                TreeName,
                shard,
                peer,
                localTree,
                _probeTransport,
                options.MerkleWalkMaxDepth,
                options.MerkleWalkMaxBytes,
                CancellationToken.None).ConfigureAwait(true);

            // Repair stage: when the walk localised at least one diverging
            // leaf, optionally re-ship the relevant retained WAL key ranges to
            // the diverged peer through the ordinary causal-stable apply
            // pipeline. The repair is gated behind the operator opt-in master
            // flag, a per-(tree, peer) traffic budget, and a per-(tree, peer)
            // circuit breaker; detection (the Merkle walk above) is never
            // gated. Strictly opt-in and best-effort.
            if (outcome.Localised && outcome.LocalisedRanges is { Count: > 0 } ranges)
            {
                await TryRemediateAsync(options, peer, ranges).ConfigureAwait(true);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Merkle-walk drift localisation failed for shard {Shard} peer {Peer} on {Context}; the read-only walk is best-effort and will retry on the next cadence",
                shard, peer, LogContext);
        }
    }

    /// <summary>
    /// Orchestrates the gated repair stage for a localised drift. Applies, in
    /// order, the operator opt-in master gate
    /// (<see cref="LatticeReplicationOptions.AutoRemediateOnDigestMismatch"/>),
    /// the per-(tree, peer) circuit breaker, and the per-(tree, peer) traffic
    /// budget; only when all three permit does it run the targeted leaf
    /// re-replay (and, when re-replay cannot reach the localised divergence -
    /// a WAL-trimmed or below-cursor empty selection - the scoped
    /// bootstrap-snapshot fallback). A pass that throws or whose re-ship sink reports zero entries
    /// shipped despite candidates having been selected counts as a circuit
    /// breaker failure; any other outcome resets the breaker. Each skip records
    /// the <see cref="LatticeReplicationMetrics.DigestRemediationSkipped"/>
    /// counter and reports the
    /// <see cref="LatticeReplicationMetrics.DigestRemediationDisabledName"/>
    /// gauge with the matching reason. Best-effort: nothing here escapes to
    /// disturb the detect/localise cadence.
    /// </summary>
    /// <param name="options">The resolved per-tree replication options.</param>
    /// <param name="peer">The diverged peer cluster id.</param>
    /// <param name="ranges">The localised diverging leaf covering ranges.</param>
    private async Task TryRemediateAsync(
        LatticeReplicationOptions options,
        string peer,
        IReadOnlyList<LeafReReplayRange> ranges)
    {
        // Master gate: all automatic remediation is opt-in. Detection has
        // already fired; this gate only suppresses the repair actions.
        if (!options.AutoRemediateOnDigestMismatch)
        {
            SkipRemediation(peer, RemediationDisabledReason.OptOut);
            return;
        }

        var nowTicks = DateTime.UtcNow.Ticks;

        // Circuit breaker: a tree/peer whose breaker is open and still cooling
        // down skips remediation entirely until the cooldown elapses.
        if (_remediationGuard.IsCircuitBlocking(peer, options.RemediationCircuitResetInterval.Ticks, nowTicks))
        {
            SkipRemediation(peer, RemediationDisabledReason.CircuitOpen);
            return;
        }

        // Rate cap: skip when this tree/peer has already spent its per-window
        // remediation budget.
        var windowBudget = RemediationTrafficBudget(options);
        if (!_remediationGuard.TryBeginRemediation(peer, windowBudget, options.RemediationTrafficWindow.Ticks, nowTicks))
        {
            SkipRemediation(peer, RemediationDisabledReason.BudgetExhausted);
            return;
        }

        var entriesShipped = 0;
        var failed = false;
        try
        {
            var reReplay = await TryReReplayLeavesAsync(options, peer, ranges).ConfigureAwait(true);
            entriesShipped += reReplay.EntriesReReplayed;

            // A pass that selected candidates but re-shipped nothing is a
            // failure (the sink rejected the repair traffic).
            failed = reReplay.Attempted && reReplay.EntriesReReplayed == 0;

            // Escalate to the scoped bootstrap-snapshot fallback whenever the
            // cursor-bounded WAL re-replay could not reach the divergence the
            // Merkle walk localised. Two skip reasons signal that:
            //   - WalTrimmed: the retained WAL was garbage-collected past the
            //     divergence point, so the missing entries are gone from the log.
            //   - RangeEmpty: the walk localised genuinely divergent leaf ranges
            //     (this method is only reached with a non-empty range set) yet
            //     re-replay selected no eligible entries. That is the below-
            //     cursor blind spot - a later write already advanced the peer's
            //     high-water-mark past an older gap of never-shipped entries, so
            //     the `Timestamp > peerCursor` selection filtered them all out.
            // The fallback re-derives the committed projection of just the
            // divergent ranges from the live tree, which is immune to both the
            // WAL trim and the cursor filter, so it ships those orphans; the
            // receiver applies them because its only drop threshold is the
            // snapshot-pinned causal floor, never the incremental per-origin
            // diagonal.
            if (reReplay.SkipReason is LeafReReplaySkipReason.WalTrimmed
                or LeafReReplaySkipReason.RangeEmpty)
            {
                var fallback = await TryBootstrapFallbackAsync(options, peer, ranges).ConfigureAwait(true);
                entriesShipped += fallback.EntriesShipped;
                failed |= fallback.Attempted && fallback.EntriesShipped == 0;
            }
        }
        catch (Exception ex)
        {
            // The inner repair helpers already swallow-and-log their own faults;
            // this guard is defence-in-depth so a fault anywhere in the
            // remediation orchestration counts as a circuit-breaker failure
            // rather than escaping to disturb the cadence.
            Logger.LogWarning(ex,
                "Remediation orchestration failed for peer {Peer} on {Context}; counting as a circuit-breaker failure",
                peer, LogContext);
            failed = true;
        }

        _remediationGuard.RecordEntriesShipped(peer, entriesShipped);

        if (failed)
        {
            if (_remediationGuard.RecordFailure(peer, options.RemediationFailureThreshold, nowTicks))
            {
                RemediationGuard.PublishDisabled(TreeName, peer, RemediationDisabledReason.CircuitOpen);
            }
        }
        else
        {
            _remediationGuard.RecordSuccess(peer);
            RemediationGuard.ClearDisabled(TreeName, peer);
        }
    }

    /// <summary>
    /// Records a remediation skip: increments the
    /// <see cref="LatticeReplicationMetrics.DigestRemediationSkipped"/> counter
    /// and reports the
    /// <see cref="LatticeReplicationMetrics.DigestRemediationDisabledName"/>
    /// gauge for this tree/peer with the supplied reason.
    /// </summary>
    private void SkipRemediation(string peer, RemediationDisabledReason reason)
    {
        RemediationGuard.PublishDisabled(TreeName, peer, reason);
        LatticeReplicationMetrics.DigestRemediationSkipped.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, TreeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peer),
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagReason,
                LatticeReplicationMetrics.DigestRemediationDisabledReasonTag(reason)));
    }

    /// <summary>
    /// Derives the per-(tree, peer) per-window remediation entry budget from the
    /// configured fraction of <see cref="LatticeReplicationOptions.ShipBatchSize"/>,
    /// flooring at one entry so a configured fraction always permits at least
    /// the first pass.
    /// </summary>
    private static int RemediationTrafficBudget(LatticeReplicationOptions options) =>
        Math.Max(1, (int)Math.Ceiling(options.RemediationTrafficBudgetFraction * options.ShipBatchSize));

    /// <summary>
    /// Re-ships the retained write-ahead-log entries covering the localised
    /// diverging leaf ranges to <paramref name="peer"/> through the ordinary
    /// causal-stable apply pipeline. When the repair stage is disabled the pass
    /// records a single skipped-with-reason-disabled signal and returns without
    /// reading the WAL; otherwise it bounds the re-send by the peer's
    /// high-water-mark cursor and the configured caps. Best-effort: a failure
    /// is logged and swallowed so the detect/localise cadence is never
    /// disturbed.
    /// </summary>
    /// <param name="options">The resolved per-tree replication options.</param>
    /// <param name="peer">The diverged peer cluster id.</param>
    /// <param name="ranges">The localised diverging leaf covering ranges.</param>
    /// <returns>
    /// The re-replay outcome. A <see cref="LeafReReplaySkipReason.WalTrimmed"/>
    /// or <see cref="LeafReReplaySkipReason.RangeEmpty"/> skip reason signals
    /// the caller to consider the bootstrap-snapshot fallback (the WAL was
    /// trimmed past the divergence, or the divergence sits at or below the
    /// peer cursor so no WAL entry was eligible); a logged-and-swallowed
    /// failure returns <see cref="LeafReReplayOutcome.NotAttempted"/>.
    /// </returns>
    private async Task<LeafReReplayOutcome> TryReReplayLeavesAsync(
        LatticeReplicationOptions options,
        string peer,
        IReadOnlyList<LeafReReplayRange> ranges)
    {
        if (!options.LeafReReplayEnabled)
        {
            LatticeReplicationMetrics.LeafReReplaySkipped.Add(
                1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, TreeName),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peer),
                new KeyValuePair<string, object?>(
                    LatticeReplicationMetrics.TagReason,
                    LatticeReplicationMetrics.LeafReReplaySkipReasonTag(LeafReReplaySkipReason.Disabled)));
            return new LeafReReplayOutcome { SkipReason = LeafReReplaySkipReason.Disabled };
        }

        try
        {
            var originClusterId = options.ClusterId;
            var peerCursor = await _probeTransport
                .GetPeerHighWaterMarkAsync(peer, TreeName, originClusterId, CancellationToken.None)
                .ConfigureAwait(true);

            var partitionCount = Math.Max(1, options.ReplogPartitions);
            var pageSize = Math.Max(1, options.ShipPartitionPageSize);
            var walSource = new WalGrainReReplaySource(_grainFactory, TreeName, partitionCount, pageSize);
            var sink = new TransportLeafReReplaySink(_replicationTransport, _batchEncoder, originClusterId);

            return await LeafReReplayer.ReplayAsync(
                TreeName,
                peer,
                originClusterId,
                ranges,
                peerCursor,
                walSource,
                sink,
                options.LeafReReplayMaxEntries,
                options.LeafReReplayMaxBytes,
                CancellationToken.None).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Targeted leaf re-replay failed for peer {Peer} on {Context}; the repair is best-effort and will retry on the next cadence",
                peer, LogContext);
            return LeafReReplayOutcome.NotAttempted;
        }
    }

    /// <summary>
    /// Falls back to a scoped bootstrap-snapshot repair of the localised
    /// divergent leaf ranges when the targeted leaf re-replay could not reach
    /// the divergence point - either because the local write-ahead-log was
    /// trimmed past it, or because the divergence sits at or below the peer's
    /// high-water-mark cursor so no WAL entry was eligible for re-replay. When
    /// the fallback stage is disabled the pass records a single
    /// skipped-with-reason-disabled signal and returns without reading the
    /// snapshot; otherwise it re-derives the committed projection of just the
    /// divergent ranges from the live tree (immune to WAL trimming and to the
    /// peer-cursor filter) and re-ships those committed entries to the diverged
    /// peer through the ordinary replication transport. Best-effort: a failure
    /// is logged and swallowed so the detect/localise cadence is never
    /// disturbed.
    /// </summary>
    /// <param name="options">The resolved per-tree replication options.</param>
    /// <param name="peer">The diverged peer cluster id.</param>
    /// <param name="ranges">The localised divergent leaf covering ranges.</param>
    /// <returns>
    /// The fallback outcome. A disabled fallback returns a
    /// <see cref="BootstrapFallbackSkipReason.Disabled"/> outcome; a
    /// logged-and-swallowed failure returns
    /// <see cref="BootstrapFallbackOutcome.NotAttempted"/>.
    /// </returns>
    private async Task<BootstrapFallbackOutcome> TryBootstrapFallbackAsync(
        LatticeReplicationOptions options,
        string peer,
        IReadOnlyList<LeafReReplayRange> ranges)
    {
        if (!options.BootstrapFallbackEnabled)
        {
            LatticeReplicationMetrics.BootstrapFallbackSkipped.Add(
                1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, TreeName),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peer),
                new KeyValuePair<string, object?>(
                    LatticeReplicationMetrics.TagReason,
                    LatticeReplicationMetrics.BootstrapFallbackSkipReasonTag(BootstrapFallbackSkipReason.Disabled)));
            return new BootstrapFallbackOutcome { SkipReason = BootstrapFallbackSkipReason.Disabled };
        }

        try
        {
            var originClusterId = options.ClusterId;
            var sink = new TransportLeafReReplaySink(_replicationTransport, _batchEncoder, originClusterId);

            return await BootstrapFallbackPlanner.PlanAsync(
                TreeName,
                peer,
                originClusterId,
                ranges,
                _snapshotProvider,
                sink,
                options.BootstrapFallbackMaxEntries,
                options.BootstrapFallbackMaxBytes,
                CancellationToken.None).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Bootstrap-snapshot fallback failed for peer {Peer} on {Context}; the repair is best-effort and will retry on the next cadence",
                peer, LogContext);
            return BootstrapFallbackOutcome.NotAttempted;
        }
    }

    /// <summary>
    /// Resolves the physical tree id for this tree (after registry alias
    /// resolution) so the localisation pass can address shard-root and
    /// internal-node grains directly. Resolved fresh on every localisation pass
    /// rather than cached for the activation: a registry alias swap (shadow-
    /// cutover restore, resize, reshard) can repoint the logical tree to a new
    /// physical tree underneath a live probe, and a cached physical id would
    /// leave the Merkle walk descending the retired tree's frozen structure.
    /// The read is read-only and cheap relative to the walk it precedes.
    /// </summary>
    private static async Task<string> EnsurePhysicalTreeIdAsync(ILattice lattice)
    {
        var routing = await lattice.GetRoutingAsync(CancellationToken.None).ConfigureAwait(true);
        return routing.PhysicalTreeId;
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
