using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ILatticeReplicationAdmin"/> implementation.
/// Tracks the last honoured re-seed request per
/// <c>(treeName, sourceClusterId)</c> pair in a process-local
/// dictionary, gates new requests against the configured
/// <see cref="LatticeReplicationOptions.OperatorReseedMinInterval"/>
/// (resolved per-tree via
/// <see cref="IOptionsMonitor{TOptions}"/>), and forwards honoured
/// requests to <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/>.
/// The dictionary timestamp is updated only after the coordinator
/// call returns successfully so a thrown coordinator exception does
/// not consume the rate-limit budget.
/// </summary>
internal sealed class LatticeReplicationAdmin(
    ILatticeBootstrapCoordinator bootstrapCoordinator,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    ILogger<LatticeReplicationAdmin> logger,
    TimeProvider? timeProvider = null,
    IGrainFactory? grainFactory = null) : ILatticeReplicationAdmin
{
    private readonly ILatticeBootstrapCoordinator _bootstrapCoordinator =
        bootstrapCoordinator ?? throw new ArgumentNullException(nameof(bootstrapCoordinator));
    private readonly IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ILogger<LatticeReplicationAdmin> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private readonly IGrainFactory? _grainFactory = grainFactory;

    /// <summary>
    /// Honoured-request timestamps keyed by <c>(treeName, sourceClusterId)</c>.
    /// Process-local; reset on silo restart. The dictionary is bounded
    /// by the operator's distinct re-seed surface and so does not
    /// require eviction.
    /// </summary>
    private readonly ConcurrentDictionary<(string Tree, string Source), DateTimeOffset> _lastHonoured = new();

    /// <inheritdoc />
    public async Task<OperatorReseedDecision> RequestSnapshotAsync(
        string treeName,
        string sourceClusterId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        var key = (treeName, sourceClusterId);
        var now = _time.GetUtcNow();
        var minInterval = _optionsMonitor.Get(treeName).OperatorReseedMinInterval;

        if (minInterval > TimeSpan.Zero
            && _lastHonoured.TryGetValue(key, out var lastAt))
        {
            var elapsed = now - lastAt;
            if (elapsed < minInterval)
            {
                var retryAfter = minInterval - elapsed;
                _logger.LogInformation(
                    "Operator re-seed request denied for tree {Tree} from {Source}: rate-limited (lastHonoured={LastAt}, retryAfter={RetryAfter})",
                    treeName, sourceClusterId, lastAt, retryAfter);
                return new OperatorReseedDecision(
                    Triggered: false,
                    LastRequestedAt: lastAt,
                    RetryAfter: retryAfter);
            }
        }

        _logger.LogInformation(
            "Operator re-seed request honoured for tree {Tree} from {Source}",
            treeName, sourceClusterId);

        await _bootstrapCoordinator
            .BootstrapAsync(treeName, sourceClusterId, cancellationToken)
            .ConfigureAwait(false);

        // Stamp the honoured time only after a successful coordinator
        // call so transport / coordinator faults do not consume the
        // rate-limit budget against the operator.
        _lastHonoured[key] = now;
        return new OperatorReseedDecision(
            Triggered: true,
            LastRequestedAt: now,
            RetryAfter: null);
    }

    /// <inheritdoc />
    public async Task<OperatorReseedDecision> ForceRequestSnapshotAsync(
        string treeName,
        string sourceClusterId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        var key = (treeName, sourceClusterId);
        var now = _time.GetUtcNow();

        // Audit every bypass call before dispatching so an operator
        // mistake (forcing a re-seed against the wrong tree, or
        // forcing repeatedly inside what the routine path would have
        // rate-limited) leaves a Information-level trail in the silo
        // log even when no other telemetry surface is wired up.
        _logger.LogInformation(
            "Operator re-seed FORCE bypass invoked for tree {Tree} from {Source} (rate limit skipped)",
            treeName, sourceClusterId);

        await _bootstrapCoordinator
            .BootstrapAsync(treeName, sourceClusterId, cancellationToken)
            .ConfigureAwait(false);

        // Update the rate-limit dictionary on a successful bypass so
        // a follow-up routine RequestSnapshotAsync inside the window
        // still observes the bypass as the "last honoured" request.
        // This preserves the operator's mental model that the limiter
        // knows about every actual re-seed, not just the rate-limited
        // ones. A coordinator exception leaves the dictionary
        // unchanged so a failed bypass does not silently consume the
        // budget for a follow-up routine call.
        _lastHonoured[key] = now;
        return new OperatorReseedDecision(
            Triggered: true,
            LastRequestedAt: now,
            RetryAfter: null);
    }

    /// <inheritdoc />
    public async Task<bool> ForceLiftBootstrapReadFenceAsync(
        string treeName,
        string reason,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(reason);
        cancellationToken.ThrowIfCancellationRequested();

        // Fail closed: without a grain factory there is no fence to reach, and
        // reporting "nothing armed" would be a lie about a tree that may be
        // fenced.
        var grainFactory = _grainFactory
            ?? throw new InvalidOperationException(
                $"{nameof(LatticeReplicationAdmin)} was constructed without an {nameof(IGrainFactory)}; it cannot force-lift a bootstrap read fence.");

        // Audit before dispatch, so an attempt is on record even when it fails.
        _logger.LogWarning(
            "Operator FORCE-LIFT of the snapshot bootstrap read fence requested for tree {Tree}: {Reason}. Reads may observe a partial snapshot import until the next successful bootstrap.",
            treeName, reason);

        var lifted = await grainFactory
            .GetGrain<Grains.ILatticeBootstrapCoordinatorGrain>(treeName)
            .ForceLiftReadFenceAsync(cancellationToken)
            .ConfigureAwait(false);

        if (lifted)
        {
            LatticeReplicationMetrics.BootstrapReadFenceForceLifted.Add(1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                LatticeTenantLabel.ForTree(treeName));
            _logger.LogWarning(
                "Snapshot bootstrap read fence FORCE-LIFTED for tree {Tree}; its automatic re-drive is stopped. Reads may observe a partial snapshot import until a bootstrap completes.",
                treeName);
        }
        else
        {
            _logger.LogInformation(
                "Operator force-lift of the snapshot bootstrap read fence for tree {Tree} found no fence armed; nothing changed.",
                treeName);
        }

        return lifted;
    }

    /// <inheritdoc />
    public async Task<bool> ResolveCrossClusterSagaParticipantAsync(
        string sagaId,
        bool commit,
        string reason,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);
        ArgumentException.ThrowIfNullOrEmpty(reason);
        cancellationToken.ThrowIfCancellationRequested();

        // Fail closed: without a grain factory there is no participant to reach.
        var grainFactory = _grainFactory
            ?? throw new InvalidOperationException(
                $"{nameof(LatticeReplicationAdmin)} was constructed without an {nameof(IGrainFactory)}; it cannot resolve a cross-cluster saga participant.");

        // Audit before dispatch, so an attempt is on record even when it fails.
        _logger.LogWarning(
            "Operator RESOLVE of cross-cluster saga participant {SagaId} to {Decision} requested: {Reason}. If the coordinator cannot be reached, nothing checks this against the other clusters of the saga.",
            sagaId, commit ? "commit" : "abort", reason);

        var resolved = await grainFactory
            .GetGrain<Grains.ICrossClusterSagaParticipantGrain>(sagaId)
            .OperatorResolveAsync(commit)
            .ConfigureAwait(false);

        _logger.LogWarning(
            resolved
                ? "Cross-cluster saga participant {SagaId} RESOLVED to {Decision} by an operator."
                : "Operator resolution of cross-cluster saga participant {SagaId} to {Decision} found it already resolved; nothing changed.",
            sagaId, commit ? "commit" : "abort");
        return resolved;
    }
}
