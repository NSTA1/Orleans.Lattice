using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Keeps this silo's compiled runtime replication-configuration snapshot current
/// with config writes committed on other silos (#5462). The core change feed
/// fires only on the silo hosting the leaf that committed a
/// <see cref="LatticeSystemTreeNames.ReplicationConfig"/> write, so before this
/// every other silo kept the enrollment it compiled at warm-up: a tree enabled at
/// runtime stayed "not enrolled" there, so that silo refused a peer's re-seed
/// export of the tree (leaving the peer's link wedged awaiting a re-seed) and
/// declined to coordinate a restore of it (replacing the tree locally while the
/// peers kept the discarded writes).
/// </summary>
/// <remarks>
/// <para>
/// <b>Publish.</b> On a config write this silo observed, it advances the
/// cluster-wide <see cref="IReplicationConfigEpochGrain"/>. The advance runs off
/// the writing grain's scheduler, is coalesced like the snapshot rebuild, and is
/// retried until it lands, so a transient failure never loses a write.
/// </para>
/// <para>
/// <b>Follow.</b> Every <see cref="PollInterval"/> it reads the epoch and the
/// cluster membership version, and rebuilds the snapshot when either has moved
/// since its last successful rebuild: an advance, a new epoch incarnation (the
/// grain restarted, so an advance may have been lost), or a membership change (a
/// silo that died between committing a write and publishing it). The values are
/// recorded only after the rebuild succeeds, so a failed rebuild is retried on the
/// next poll rather than leaving the old snapshot in force. The steady state is
/// one small grain call per interval and no rebuild. Mirrors the authorization
/// package's policy-epoch synchronizer.
/// </para>
/// </remarks>
internal sealed class ReplicationConfigEpochSynchronizer : IHostedService, IMutationObserver
{
    /// <summary>How often each silo reads the cluster-wide epoch by default.</summary>
    internal static readonly TimeSpan DefaultPollInterval = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan InitialPublishRetryDelay = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan MaxPublishRetryDelay = TimeSpan.FromSeconds(1);

    private readonly IGrainFactory _grainFactory;
    private readonly CompiledReplicationConfigSnapshotMaintainer _maintainer;
    private readonly IClusterMembershipService? _membership;
    private readonly TimeProvider _time;
    private readonly ILogger<ReplicationConfigEpochSynchronizer> _logger;
    private readonly CancellationTokenSource _stopping = new();

    // Coalescing state for publishing: 0 idle, 1 running, 2 running with a queued
    // follow-up advance.
    private int _publishState;
    private Task _publishLoop = Task.CompletedTask;
    private Task _pollLoop = Task.CompletedTask;

    private ReplicationConfigEpoch? _compiledEpoch;
    private MembershipVersion _compiledMembership;
    private IReplicationConfigEpochGrain? _epochGrain;
    private bool _pollFailing;

    /// <summary>Initializes the synchronizer.</summary>
    /// <param name="grainFactory">The silo's grain factory.</param>
    /// <param name="maintainer">This silo's compiled replication-configuration snapshot.</param>
    /// <param name="logger">The logger for publish and rebuild failures.</param>
    /// <param name="membership">Cluster membership, whose version change triggers a rebuild; <c>null</c> when unavailable.</param>
    /// <param name="timeProvider">The clock the poll and retry delays run on; defaults to <see cref="TimeProvider.System"/>.</param>
    public ReplicationConfigEpochSynchronizer(
        IGrainFactory grainFactory,
        CompiledReplicationConfigSnapshotMaintainer maintainer,
        ILogger<ReplicationConfigEpochSynchronizer> logger,
        IClusterMembershipService? membership = null,
        TimeProvider? timeProvider = null)
        : this(grainFactory, maintainer, logger, membership, timeProvider, DefaultPollInterval)
    {
    }

    /// <summary>Initializes the synchronizer with an explicit poll interval.</summary>
    internal ReplicationConfigEpochSynchronizer(
        IGrainFactory grainFactory,
        CompiledReplicationConfigSnapshotMaintainer maintainer,
        ILogger<ReplicationConfigEpochSynchronizer> logger,
        IClusterMembershipService? membership,
        TimeProvider? timeProvider,
        TimeSpan pollInterval)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(maintainer);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(pollInterval, TimeSpan.Zero);

        _grainFactory = grainFactory;
        _maintainer = maintainer;
        _logger = logger;
        _membership = membership;
        _time = timeProvider ?? TimeProvider.System;
        PollInterval = pollInterval;
    }

    /// <summary>How often this silo reads the cluster-wide epoch.</summary>
    internal TimeSpan PollInterval { get; }

    /// <summary>The most recently started publish loop, exposed so a test can await it.</summary>
    internal Task PublishLoop => Volatile.Read(ref _publishLoop);

    /// <summary>The poll loop, exposed so a test can await it after <see cref="StopAsync"/>.</summary>
    internal Task PollLoop => _pollLoop;

    private IReplicationConfigEpochGrain Epoch =>
        _epochGrain ??= _grainFactory.GetGrain<IReplicationConfigEpochGrain>(IReplicationConfigEpochGrain.GrainKey);

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _pollLoop = PollLoopAsync(_stopping.Token);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);
        foreach (var loop in (Task[])[_pollLoop, PublishLoop])
        {
            try
            {
                await loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Runs inline on the committing grain's write path, so it only schedules the
    /// advance: one ordinal comparison for every other tree.
    /// </remarks>
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        if (string.Equals(mutation.TreeId, LatticeSystemTreeNames.ReplicationConfig, StringComparison.Ordinal))
        {
            SchedulePublish();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Reads the epoch and the membership version once and rebuilds the snapshot
    /// when either has moved since the last successful rebuild. Throws when the
    /// read or the rebuild fails, leaving the recorded values unchanged so the
    /// next poll retries.
    /// </summary>
    /// <param name="cancellationToken">Cancels the poll.</param>
    /// <returns><c>true</c> when the snapshot was rebuilt.</returns>
    internal async Task<bool> PollOnceAsync(CancellationToken cancellationToken)
    {
        // Read both before the rebuild, so anything that moves them later is seen
        // by the next poll rather than credited to this rebuild.
        var membership = _membership?.CurrentSnapshot.Version ?? MembershipVersion.MinValue;
        var epoch = await Epoch.GetAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        if (_compiledEpoch == epoch && _compiledMembership == membership)
        {
            return false;
        }

        await _maintainer.RebuildNowAsync(cancellationToken).ConfigureAwait(false);
        _compiledEpoch = epoch;
        _compiledMembership = membership;
        return true;
    }

    private async Task PollLoopAsync(CancellationToken cancellationToken)
    {
        // One timer for the silo's lifetime: each tick costs no allocation.
        using var timer = new PeriodicTimer(PollInterval, _time);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                try
                {
                    await PollOnceAsync(cancellationToken).ConfigureAwait(false);
                    _pollFailing = false;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // Warn once per outage rather than once per poll.
                    var level = _pollFailing ? LogLevel.Debug : LogLevel.Warning;
                    _pollFailing = true;
                    _logger.Log(
                        level,
                        ex,
                        "Could not bring the compiled replication-configuration snapshot up to date with the cluster; retrying on the next poll.");
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Stopping.
        }
    }

    private void SchedulePublish()
    {
        while (true)
        {
            switch (Volatile.Read(ref _publishState))
            {
                case 0:
                    if (Interlocked.CompareExchange(ref _publishState, 1, 0) == 0)
                    {
                        // Run off the committing grain's scheduler.
                        Volatile.Write(ref _publishLoop, Task.Run(PublishLoopAsync));
                        return;
                    }

                    break;
                case 1:
                    if (Interlocked.CompareExchange(ref _publishState, 2, 1) == 1)
                    {
                        return;
                    }

                    break;
                default:
                    return;
            }
        }
    }

    private async Task PublishLoopAsync()
    {
        var cancellationToken = _stopping.Token;
        var retryDelay = InitialPublishRetryDelay;
        while (true)
        {
            try
            {
                await Epoch.AdvanceAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
                retryDelay = InitialPublishRetryDelay;

                // Go idle unless a write was observed while advancing; otherwise
                // advance once more so that write is covered too.
                if (Interlocked.CompareExchange(ref _publishState, 0, 1) == 1)
                {
                    return;
                }

                Volatile.Write(ref _publishState, 1);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                Volatile.Write(ref _publishState, 0);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Publishing a replication-configuration change to the cluster failed; retrying.");
                if (!await DelayAsync(retryDelay, cancellationToken).ConfigureAwait(false))
                {
                    Volatile.Write(ref _publishState, 0);
                    return;
                }

                retryDelay = retryDelay * 2 > MaxPublishRetryDelay ? MaxPublishRetryDelay : retryDelay * 2;
            }
        }
    }

    private async Task<bool> DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, _time, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
