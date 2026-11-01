using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Membership;

/// <summary>
/// Keeps this silo's <see cref="MembershipResolutionCache"/> current with
/// <c>sys-membership-*</c> writes committed on other silos (#5518). The core change
/// feed fires only on the silo hosting the leaf that committed a membership write,
/// so before this every other silo kept serving its cached resolution of a subject
/// - with a group the subject had been removed from - until the entry expired,
/// which is <see cref="LatticeMembershipOptions.ResolutionCacheTtl"/> (five minutes
/// by default). A revocation is exactly when a stale group grant hurts.
/// </summary>
/// <remarks>
/// <para>
/// <b>Publish.</b> On a membership write this silo observed, it advances the
/// cluster-wide <see cref="IMembershipEpochGrain"/>. The advance runs off the
/// writing grain's scheduler, is coalesced, and is retried until it lands, so a
/// transient failure never loses a write. The committing silo's own cache is
/// flushed by the cache's own observer before the write returns.
/// </para>
/// <para>
/// <b>Follow.</b> Every <see cref="PollInterval"/> it reads the epoch and the
/// cluster membership version, and flushes the cache when either has moved since
/// the last poll: an advance, a new epoch incarnation (the grain restarted, so an
/// advance may have been lost), or a membership change (a silo that died between
/// committing a write and publishing it). The steady state is one small grain call
/// per interval and no flush.
/// </para>
/// <para>
/// <b>Fail closed.</b> Every successful poll confirms the cache current for
/// <see cref="LeaseDuration"/> from the moment the poll began, and the cache serves
/// a warm entry only while that confirmation holds. A silo that cannot read the
/// epoch - the epoch grain is unreachable, or the poll stalls - therefore stops
/// serving cached resolutions within one lease and resolves every caller afresh
/// from the directory, rather than serving pre-change groups for the rest of the
/// cache lifetime.
/// </para>
/// </remarks>
internal sealed class MembershipEpochSynchronizer : IHostedService, IMutationObserver
{
    /// <summary>How often each silo reads the cluster-wide epoch by default.</summary>
    internal static readonly TimeSpan DefaultPollInterval = TimeSpan.FromSeconds(1);

    /// <summary>How many poll intervals one successful poll keeps the cache current for.</summary>
    internal const int LeasePolls = 3;

    private static readonly TimeSpan InitialPublishRetryDelay = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan MaxPublishRetryDelay = TimeSpan.FromSeconds(1);

    private readonly IGrainFactory _grainFactory;
    private readonly MembershipResolutionCache _cache;
    private readonly IClusterMembershipService? _membership;
    private readonly TimeProvider _time;
    private readonly ILogger<MembershipEpochSynchronizer> _logger;
    private readonly CancellationTokenSource _stopping = new();

    // Coalescing state for publishing: 0 idle, 1 running, 2 running with a queued
    // follow-up advance.
    private int _publishState;
    private Task _publishLoop = Task.CompletedTask;
    private Task _pollLoop = Task.CompletedTask;

    private MembershipEpoch? _observedEpoch;
    private MembershipVersion _observedMembership;
    private IMembershipEpochGrain? _epochGrain;
    private bool _pollFailing;

    /// <summary>Initializes the synchronizer.</summary>
    /// <param name="grainFactory">The silo's grain factory.</param>
    /// <param name="cache">This silo's resolution cache.</param>
    /// <param name="logger">The logger for publish and poll failures.</param>
    /// <param name="membership">Cluster membership, whose version change flushes the cache; <c>null</c> when unavailable.</param>
    /// <param name="timeProvider">The clock the poll, the lease and the retry delays run on; defaults to <see cref="TimeProvider.System"/>.</param>
    public MembershipEpochSynchronizer(
        IGrainFactory grainFactory,
        MembershipResolutionCache cache,
        ILogger<MembershipEpochSynchronizer> logger,
        IClusterMembershipService? membership = null,
        TimeProvider? timeProvider = null)
        : this(grainFactory, cache, logger, membership, timeProvider, DefaultPollInterval)
    {
    }

    /// <summary>Initializes the synchronizer with an explicit poll interval.</summary>
    internal MembershipEpochSynchronizer(
        IGrainFactory grainFactory,
        MembershipResolutionCache cache,
        ILogger<MembershipEpochSynchronizer> logger,
        IClusterMembershipService? membership,
        TimeProvider? timeProvider,
        TimeSpan pollInterval)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(pollInterval, TimeSpan.Zero);

        _grainFactory = grainFactory;
        _cache = cache;
        _logger = logger;
        _membership = membership;
        _time = timeProvider ?? TimeProvider.System;
        PollInterval = pollInterval;
        LeaseDuration = pollInterval * LeasePolls;

        // From construction on, before any request can be served, the cache only
        // serves what a successful poll has confirmed current.
        cache.RequireConfirmedCurrency();
    }

    /// <summary>How often this silo reads the cluster-wide epoch.</summary>
    internal TimeSpan PollInterval { get; }

    /// <summary>How long one successful poll keeps the cache current for.</summary>
    internal TimeSpan LeaseDuration { get; }

    /// <summary>The most recently started publish loop, exposed so a test can await it.</summary>
    internal Task PublishLoop => Volatile.Read(ref _publishLoop);

    /// <summary>The poll loop, exposed so a test can await it after <see cref="StopAsync"/>.</summary>
    internal Task PollLoop => _pollLoop;

    private IMembershipEpochGrain Epoch =>
        _epochGrain ??= _grainFactory.GetGrain<IMembershipEpochGrain>(IMembershipEpochGrain.GrainKey);

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
    /// advance: one ordinal prefix test for every other tree.
    /// </remarks>
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        if (mutation.TreeId is { } treeId
            && treeId.StartsWith(MembershipConstants.TreePrefix, StringComparison.Ordinal))
        {
            SchedulePublish();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Reads the epoch and the membership version once, flushes the cache when
    /// either has moved since the last poll, and then confirms the cache current
    /// for <see cref="LeaseDuration"/> from the moment the poll began. Throws when
    /// the read fails, leaving the confirmation to lapse.
    /// </summary>
    /// <param name="cancellationToken">Cancels the poll.</param>
    /// <returns><c>true</c> when the cache was flushed.</returns>
    internal async Task<bool> PollOnceAsync(CancellationToken cancellationToken)
    {
        // The lease runs from before the read, so a slow read cannot extend it past
        // what the epoch it returns can vouch for. Read the membership version
        // before the epoch, so anything that moves it later is seen by the next poll.
        var started = _time.GetUtcNow();
        var membership = _membership?.CurrentSnapshot.Version ?? MembershipVersion.MinValue;
        var epoch = await Epoch.GetAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        var moved = _observedEpoch != epoch || _observedMembership != membership;
        if (moved)
        {
            // Flush before confirming, so no entry from before the change is ever
            // served under the new confirmation.
            _cache.Clear();
            _observedEpoch = epoch;
            _observedMembership = membership;
        }

        _cache.ConfirmCurrentUntil(started + LeaseDuration);
        return moved;
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
                        "Could not read the cluster-wide membership epoch; cached subject resolutions stop being served when the current confirmation lapses. Retrying on the next poll.");
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
                _logger.LogDebug(ex, "Publishing a membership change to the cluster failed; retrying.");
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
