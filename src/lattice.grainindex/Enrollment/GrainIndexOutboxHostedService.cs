using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// Runs <see cref="GrainIndexOutboxDrainer"/> on a schedule for the lifetime of
/// the silo, so an index write that failed - or that a stopped silo never
/// issued - converges without anyone asking.
/// </summary>
/// <remarks>
/// <para>
/// Unlike the registry reconciler, this deliberately does <b>not</b> block
/// start-up. Its job is repair rather than validation: a silo whose outbox
/// cannot be drained yet is still a correct silo, and refusing to start would
/// turn a recoverable index delay into an outage.
/// </para>
/// <para>
/// The loop never lets an exception escape. A drain pass that throws - the
/// registry tree briefly unavailable, say - is logged and retried on the next
/// tick, because the whole point of the outbox is that it survives exactly this
/// kind of interruption.
/// </para>
/// </remarks>
internal sealed class GrainIndexOutboxHostedService : IHostedService, IDisposable
{
    private readonly GrainIndexOutboxDrainer _drainer;
    private readonly GrainIndexOutboxOptions _options;
    private readonly ILogger<GrainIndexOutboxHostedService> _logger;
    private readonly CancellationTokenSource _stopping = new();
    private Task? _loop;

    /// <summary>Initialises the service.</summary>
    /// <param name="drainer">The drain to run. Must not be <c>null</c>.</param>
    /// <param name="options">The outbox settings. Must not be <c>null</c>.</param>
    /// <param name="logger">Reports drain passes that threw. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexOutboxHostedService(
        GrainIndexOutboxDrainer drainer,
        IOptions<GrainIndexOutboxOptions> options,
        ILogger<GrainIndexOutboxHostedService> logger)
    {
        ArgumentNullException.ThrowIfNull(drainer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _drainer = drainer;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation(
                "The grain-index outbox drain is disabled; pending projections are still recorded but will only be applied by a host that drains them.");
            return Task.CompletedTask;
        }

        _loop = RunAsync(_stopping.Token);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);

        if (_loop is { } loop)
        {
            try
            {
                await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Shutdown ran out of patience; the outbox is durable, so the
                // next silo to start picks the work up unchanged.
            }
        }
    }

    /// <inheritdoc />
    public void Dispose() => _stopping.Dispose();

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        var interval = _options.RetryInterval > TimeSpan.Zero
            ? _options.RetryInterval
            : GrainIndexOutboxOptions.DefaultRetryInterval;

        using var timer = new PeriodicTimer(interval);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await _drainer
                    .DrainAsync(_options.MaxBatchSize, cancellationToken)
                    .ConfigureAwait(false);

                if (!result.IsEmpty)
                {
                    _logger.LogInformation(
                        "Grain-index outbox drain applied {Applied} of {Scanned} pending projections ({Failed} deferred, {Skipped} not declared here).",
                        result.Applied,
                        result.Scanned,
                        result.Failed,
                        result.Skipped);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "A grain-index outbox drain pass failed; it will be retried.");
            }

            try
            {
                if (!await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                    return;
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
