using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A silo-hosted background service that warms the
/// <see cref="TenantResidencySnapshotMaintainer"/> once at start-up, so the
/// residency gate has an authoritative local-region view as early as possible and
/// the cold-start admit-all window (during which an unbuilt snapshot resolves every
/// tenant to online) is closed promptly rather than lazily on the first request.
/// </summary>
/// <remarks>
/// The warm-up is launched fire-and-forget: awaiting the first registry scan inside
/// <see cref="StartAsync"/> could stall silo start-up if the registry grain is not
/// yet reachable, so a transient failure is logged and retried on a short bounded
/// cadence until the snapshot is warm. This matches the repository's other
/// start-up-sensitive background services, which never block silo start on a cluster
/// round-trip. The window this closes is fail-open on residency grounds only (a
/// not-yet-built snapshot never wrongly denies), so a brief lazy warm is safe and
/// this service simply shortens it.
/// </remarks>
internal sealed class TenantResidencyWarmupHostedService : IHostedService
{
    private readonly TenantResidencySnapshotMaintainer _maintainer;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<TenantResidencyWarmupHostedService> _logger;

    private readonly CancellationTokenSource _stopping = new();
    private Task? _warmup;

    /// <summary>Initializes the warm-up hosted service.</summary>
    /// <param name="maintainer">The residency snapshot maintainer to warm. Must not be <c>null</c>.</param>
    /// <param name="timeProvider">The timestamp source backing the retry delay. Must not be <c>null</c>.</param>
    /// <param name="logger">The logger for warm-up failures. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantResidencyWarmupHostedService(
        TenantResidencySnapshotMaintainer maintainer,
        TimeProvider timeProvider,
        ILogger<TenantResidencyWarmupHostedService> logger)
    {
        ArgumentNullException.ThrowIfNull(maintainer);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        _maintainer = maintainer;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <summary>The warm-up task, exposed so a test can await its completion after <see cref="StopAsync"/>.</summary>
    internal Task? Warmup => _warmup;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _warmup = WarmLoopAsync(_stopping.Token);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);

        if (_warmup is { } warmup)
        {
            try
            {
                await warmup.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }
    }

    private async Task WarmLoopAsync(CancellationToken cancellationToken)
    {
        // A short, bounded retry cadence: the registry grain may not be reachable in
        // the very first moments of silo start. Once warm, EnsureWarmAsync is a
        // no-op, so this loop exits on the first success.
        var delay = TimeSpan.FromMilliseconds(250);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _maintainer.EnsureWarmAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    ex,
                    "Tenant-residency snapshot warm-up failed; retrying. Residency resolves to admit-all until the snapshot is warm.");
            }

            try
            {
                await Task.Delay(delay, _timeProvider, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
