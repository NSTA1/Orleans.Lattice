using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Keeps this silo's cached schema configuration for one configuration tree
/// (<c>sys-schema-policy</c> or <c>sys-schema-version</c>) current with changes
/// committed on other silos (#5238). The core change feed fires only on the silo
/// hosting the leaf that committed a configuration write, and the admin evicts only
/// its own silo, so before this every other silo kept its cached answer until
/// restart.
/// </summary>
/// <remarks>
/// <para>
/// <b>Publish.</b> On a write to the configuration tree that this silo observed, it
/// advances the cluster-wide <see cref="ISchemaConfigEpochGrain"/> for that tree.
/// The advance runs off the writing grain's scheduler, is coalesced, and is retried
/// until it lands, so a transient failure never loses a change.
/// </para>
/// <para>
/// <b>Follow.</b> Every <see cref="PollInterval"/> it reads the epoch and the cluster
/// membership version, and drops the whole cache when either has moved since the
/// last poll: an advance, a new epoch incarnation (the grain restarted, so an advance
/// may have been lost), or a membership change (a silo that died between committing
/// a write and publishing it). A poll that cannot read the epoch also drops the
/// cache, so the propagation bound holds while the epoch grain is unreachable. The
/// steady state is one small grain call per interval and no store read.
/// </para>
/// <para>
/// The result is the propagation contract: the silo that served an admin change
/// evicts before the admin call returns, and every other silo stops serving the
/// previous configuration within one <see cref="PollInterval"/> of the change being
/// published, which the committing silo does immediately after the commit.
/// </para>
/// </remarks>
/// <typeparam name="TCache">The provider cache this instance keeps current.</typeparam>
internal sealed class SchemaConfigEpochSynchronizer<TCache> : IHostedService, IMutationObserver
    where TCache : class, ISchemaConfigCache
{
    private static readonly TimeSpan InitialPublishRetryDelay = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan MaxPublishRetryDelay = TimeSpan.FromSeconds(1);

    private readonly IGrainFactory _grainFactory;
    private readonly TCache _cache;
    private readonly string _configTree;
    private readonly IClusterMembershipService? _membership;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _stopping = new();

    // Coalescing state for publishing: 0 idle, 1 running, 2 running with a queued
    // follow-up advance.
    private int _publishState;
    private Task _publishLoop = Task.CompletedTask;
    private Task _pollLoop = Task.CompletedTask;

    private SchemaConfigEpoch? _observedEpoch;
    private MembershipVersion _observedMembership;
    private ISchemaConfigEpochGrain? _epochGrain;
    private bool _pollFailing;

    /// <summary>Initializes the synchronizer.</summary>
    /// <param name="grainFactory">The silo's grain factory.</param>
    /// <param name="cache">This silo's provider cache for <paramref name="configTree"/>.</param>
    /// <param name="configTree">The reserved configuration tree whose writes the cache reflects.</param>
    /// <param name="pollInterval">How often to read the cluster-wide epoch; the propagation bound.</param>
    /// <param name="logger">The logger for publish and poll failures.</param>
    /// <param name="membership">Cluster membership, whose version change drops the cache; <c>null</c> when unavailable.</param>
    /// <param name="timeProvider">The clock the poll and retry delays run on; defaults to <see cref="TimeProvider.System"/>.</param>
    public SchemaConfigEpochSynchronizer(
        IGrainFactory grainFactory,
        TCache cache,
        string configTree,
        TimeSpan pollInterval,
        ILogger<SchemaConfigEpochSynchronizer<TCache>> logger,
        IClusterMembershipService? membership = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentException.ThrowIfNullOrEmpty(configTree);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(pollInterval, TimeSpan.Zero);

        _grainFactory = grainFactory;
        _cache = cache;
        _configTree = configTree;
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

    private ISchemaConfigEpochGrain Epoch =>
        _epochGrain ??= _grainFactory.GetGrain<ISchemaConfigEpochGrain>(_configTree);

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
        if (string.Equals(mutation.TreeId, _configTree, StringComparison.Ordinal))
        {
            SchedulePublish();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Reads the epoch and the membership version once and drops the cache when
    /// either has moved since the last poll. Throws when the read fails, leaving the
    /// recorded values unchanged.
    /// </summary>
    /// <param name="cancellationToken">Cancels the poll.</param>
    /// <returns><c>true</c> when the cache was dropped.</returns>
    internal async Task<bool> PollOnceAsync(CancellationToken cancellationToken)
    {
        // Read both before dropping, so anything that moves them later is seen by
        // the next poll rather than credited to this one.
        var membership = _membership?.CurrentSnapshot.Version ?? MembershipVersion.MinValue;
        var epoch = await Epoch.GetAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        if (_observedEpoch == epoch && _observedMembership == membership)
        {
            return false;
        }

        _cache.InvalidateAll();
        _observedEpoch = epoch;
        _observedMembership = membership;
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
                    // This silo cannot tell whether its cache is current, so it
                    // stops trusting it: the next lookups re-read the store.
                    _cache.InvalidateAll();

                    // Warn once per outage rather than once per poll.
                    var level = _pollFailing ? LogLevel.Debug : LogLevel.Warning;
                    _pollFailing = true;
                    _logger.Log(
                        level,
                        ex,
                        "Could not read the cluster-wide schema configuration epoch for {ConfigTree}; dropped the local cache and retrying on the next poll.",
                        _configTree);
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
                _logger.LogDebug(ex, "Publishing a schema configuration change for {ConfigTree} to the cluster failed; retrying.", _configTree);
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
