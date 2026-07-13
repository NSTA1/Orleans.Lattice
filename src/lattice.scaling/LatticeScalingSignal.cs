using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Live <see cref="ILatticeScalingSignal"/> implementation. Runs a silo-scoped
/// sampling timer that periodically collects compute pressure
/// (<see cref="IComputePressureCollector"/>), storage pressure
/// (<see cref="IStoragePressureCollector"/>), the cluster replica count
/// (<see cref="IReplicaCountProvider"/>), and split activity
/// (<see cref="ISplitActivityProbe"/>), folds them through the
/// <see cref="ScalingSignalComputer"/>, and caches the resulting
/// <see cref="ScalingSignal"/>.
/// <para>
/// <b>Per-scrape cost.</b> The heavy sampling happens on the timer, off the
/// scrape path. <see cref="GetScalingSignalAsync"/> returns a pre-built, cached
/// <see cref="System.Threading.Tasks.Task{TResult}"/> swapped atomically on each
/// tick, so a scrape allocates nothing and never fans out. Before the first
/// sample completes the facade returns a warming-up signal.
/// </para>
/// </summary>
internal sealed class LatticeScalingSignal : ILatticeScalingSignal, IHostedService
{
    /// <summary>
    /// The <see cref="ScalingSignal.Reason"/> reported before the first live
    /// sample has completed.
    /// </summary>
    internal const string WarmingUp = "warming up";

    private readonly IComputePressureCollector _computeCollector;
    private readonly IStoragePressureCollector _storageCollector;
    private readonly IReplicaCountProvider _replicaCountProvider;
    private readonly ISplitActivityProbe _splitProbe;
    private readonly ScalingSignalComputer _computer;
    private readonly IOptions<LatticeScalingSignalOptions> _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    private Task<ScalingSignal> _cachedTask;
    private CancellationTokenSource? _loopCts;
    private Task? _loopTask;

    /// <summary>
    /// Initialises the facade and seeds the cached signal with a warming-up
    /// snapshot so a scrape before the first sample still returns a well-formed
    /// value honouring the replica floor.
    /// </summary>
    public LatticeScalingSignal(
        IComputePressureCollector computeCollector,
        IStoragePressureCollector storageCollector,
        IReplicaCountProvider replicaCountProvider,
        ISplitActivityProbe splitProbe,
        ScalingSignalComputer computer,
        IOptions<LatticeScalingSignalOptions> options,
        TimeProvider timeProvider,
        ILogger<LatticeScalingSignal>? logger = null)
    {
        _computeCollector = computeCollector;
        _storageCollector = storageCollector;
        _replicaCountProvider = replicaCountProvider;
        _splitProbe = splitProbe;
        _computer = computer;
        _options = options;
        _timeProvider = timeProvider;
        _logger = logger ?? NullLogger<LatticeScalingSignal>.Instance;

        var floor = Math.Max(0, _options.Value.MinReplicas);
        _cachedTask = Task.FromResult(new ScalingSignal
        {
            ScaleValue = 0d,
            RawScaleValue = 0d,
            RecommendedReplicas = floor,
            Compute = default,
            Storage = default,
            Reason = WarmingUp,
            SampledAt = _timeProvider.GetUtcNow(),
        });
    }

    /// <inheritdoc />
    public Task<ScalingSignal> GetScalingSignalAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<ScalingSignal>(cancellationToken);
        }

        return Volatile.Read(ref _cachedTask);
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Create the orleans.lattice.scaling observable gauges (idempotent) and
        // publish the seeded warming-up snapshot so a scrape before the first
        // sample reports well-formed zeros honouring the replica floor.
        ScalingSignalGaugeRegistry.EnsureRegistered();
        ScalingSignalGaugeRegistry.Publish(
            ScalingGaugeSnapshot.FromSignal(Volatile.Read(ref _cachedTask).Result));

        _loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loopTask = RunSamplingLoopAsync(_loopCts.Token);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_loopCts is not null)
        {
            await _loopCts.CancelAsync().ConfigureAwait(false);
        }

        if (_loopTask is not null)
        {
            try
            {
                await _loopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }

        _loopCts?.Dispose();
    }

    private async Task RunSamplingLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await SampleOnceAsync(cancellationToken).ConfigureAwait(false);

            using var timer = new PeriodicTimer(SampleInterval(), _timeProvider);
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                await SampleOnceAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }

    /// <summary>
    /// Performs a single sample-and-cache cycle. Exposed to tests so the sampling
    /// behaviour can be exercised deterministically without driving the timer.
    /// Never throws: a failed sample logs and retains the previous cached signal.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the sample.</param>
    /// <returns>A task that completes when the cycle finishes.</returns>
    internal async Task SampleOnceAsync(CancellationToken cancellationToken)
    {
        try
        {
            var compute = await _computeCollector.CollectAsync(cancellationToken).ConfigureAwait(false);
            var storage = await _storageCollector.CollectAsync(cancellationToken).ConfigureAwait(false);
            var replicas = await _replicaCountProvider.GetActiveReplicaCountAsync(cancellationToken).ConfigureAwait(false);
            var splitInFlight = _splitProbe.AnySplitInFlight();
            var now = _timeProvider.GetUtcNow();

            var signal = _computer.Compute(compute, storage, replicas, splitInFlight, now);
            Volatile.Write(ref _cachedTask, Task.FromResult(signal));

            // Publish the flat scalar projection for the observable gauges. The
            // fold happens here, off the scrape path.
            ScalingSignalGaugeRegistry.Publish(ScalingGaugeSnapshot.FromSignal(signal));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Scaling signal sample failed; retaining the previous signal.");
        }
    }

    private TimeSpan SampleInterval()
    {
        var interval = _options.Value.SampleInterval;
        return interval > TimeSpan.Zero ? interval : LatticeScalingSignalOptions.DefaultSampleInterval;
    }
}
