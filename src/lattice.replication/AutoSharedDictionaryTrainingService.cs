using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication;

/// <summary>
/// <see cref="BackgroundService"/> that pumps the auto-training shared
/// compression-dictionary provider's
/// <see cref="AutoTrainingCompressionDictionaryProvider.TryTrain"/> on a
/// turn-safe cadence so the auto-distributing shared dictionary needs no host
/// code to train. The capture path
/// (<see cref="ReplicationMutationObserver"/>) feeds committed values into the
/// provider's reservoir; this driver periodically asks the provider to train a
/// dictionary off the hot path and publish it under a fresh id, which the
/// receiver then advertises and peers converge onto.
/// <para>
/// The pump polls at the provider's
/// <see cref="AutoTrainingCompressionDictionaryProvider.MinTrainingInterval"/>
/// (clamped to a sane floor and ceiling); the provider's
/// <see cref="AutoTrainingCompressionDictionaryProvider.TryTrain"/> re-checks
/// the cadence and the reservoir floor internally and is a cheap no-op when a
/// pass is not yet due, so an over-eager poll never trains early and never
/// blocks. Registered only by the single-switch
/// <c>AddLatticeAutoSharedDictionary</c> opt-in, so the default build never
/// runs it.
/// </para>
/// </summary>
internal sealed class AutoSharedDictionaryTrainingService : BackgroundService
{
    private static readonly TimeSpan MinPollInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxPollInterval = TimeSpan.FromMinutes(1);

    private readonly AutoTrainingCompressionDictionaryProvider _provider;
    private readonly ILogger<AutoSharedDictionaryTrainingService> _logger;
    private readonly TimeProvider _time;

    /// <summary>
    /// Initialises the service with the auto-training provider it pumps.
    /// </summary>
    /// <param name="provider">The auto-training shared-dictionary provider.</param>
    /// <param name="logger">Diagnostics logger.</param>
    /// <param name="time">
    /// Clock used for the poll delay; defaults to
    /// <see cref="TimeProvider.System"/> so tests can drive the cadence
    /// deterministically.
    /// </param>
    public AutoSharedDictionaryTrainingService(
        AutoTrainingCompressionDictionaryProvider provider,
        ILogger<AutoSharedDictionaryTrainingService> logger,
        TimeProvider? time = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(logger);
        _provider = provider;
        _logger = logger;
        _time = time ?? TimeProvider.System;
    }

    /// <summary>
    /// The poll cadence: the provider's minimum training interval clamped to
    /// <c>[1s, 1m]</c> so the pump stays responsive without busy-spinning.
    /// </summary>
    private TimeSpan PollInterval
    {
        get
        {
            var interval = _provider.MinTrainingInterval;
            if (interval < MinPollInterval)
            {
                return MinPollInterval;
            }

            return interval > MaxPollInterval ? MaxPollInterval : interval;
        }
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // A disabled provider never trains; skip the loop entirely so the
        // pump costs nothing when the host opted in to the wiring but left
        // training off.
        if (!_provider.Enabled)
        {
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(PollInterval, _time, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            try
            {
                _provider.TryTrain();
            }
            catch (ObjectDisposedException)
            {
                // The provider was disposed during host shutdown; stop pumping.
                return;
            }
            catch (Exception ex)
            {
                // Training is best-effort: a failed pass must never crash the
                // host. Log and retry on the next tick.
                _logger.LogWarning(ex, "Auto shared-dictionary training pass failed; will retry on the next tick.");
            }
        }
    }
}
