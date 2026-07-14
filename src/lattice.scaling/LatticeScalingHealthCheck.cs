using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// ASP.NET Core / Kubernetes probe target for the autoscaling signal. Reads a
/// single cluster-aggregate <see cref="ScalingSignal"/> snapshot from
/// <see cref="ILatticeScalingSignal"/> and projects it onto a
/// <see cref="HealthCheckResult"/> using the tiered thresholds in
/// <see cref="LatticeScalingHealthCheckOptions"/>.
/// </summary>
/// <remarks>
/// The check is stateless: it holds no per-probe history because the facade it
/// reads is already a cluster-aggregate, so every verdict is a pure function of
/// the current snapshot and the bound options. It does not poll, schedule, or
/// fan out to grains beyond the single cached facade call, so it is cheap to
/// invoke on a readiness-probe cadence. Registered under the name
/// <see cref="LatticeScalingHealthCheckOptions.DefaultName"/> unless the caller
/// overrides it via
/// <see cref="LatticeScalingServiceCollectionExtensions.AddLatticeScalingHealthCheck(IHealthChecksBuilder, string, HealthStatus?, IEnumerable{string})"/>.
/// </remarks>
internal sealed class LatticeScalingHealthCheck(
    ILatticeScalingSignal signal,
    IOptionsMonitor<LatticeScalingHealthCheckOptions> optionsMonitor,
    ILogger<LatticeScalingHealthCheck> logger,
    TimeProvider? timeProvider = null) : IHealthCheck
{
    private readonly ILatticeScalingSignal _signal =
        signal ?? throw new ArgumentNullException(nameof(signal));
    private readonly IOptionsMonitor<LatticeScalingHealthCheckOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ILogger<LatticeScalingHealthCheck> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        cancellationToken.ThrowIfCancellationRequested();

        var name = string.IsNullOrEmpty(context.Registration?.Name)
            ? LatticeScalingHealthCheckOptions.DefaultName
            : context.Registration.Name;
        var options = _optionsMonitor.Get(name);

        var snapshot = await _signal.GetScalingSignalAsync(cancellationToken).ConfigureAwait(false);
        var compute = snapshot.Compute;
        var storage = snapshot.Storage;

        var worstCompute = Math.Max(compute.Activation, Math.Max(compute.Resource, compute.WalDispatch));

        var status = HealthStatus.Healthy;

        // Tiered normalised compute-pressure signal.
        if (options.ComputePressure is { } tier)
        {
            if (worstCompute >= tier.Unhealthy)
            {
                status = Worsen(status, HealthStatus.Unhealthy);
            }
            else if (worstCompute >= tier.Degraded)
            {
                status = Worsen(status, HealthStatus.Degraded);
            }
        }

        // Discrete WAL-saturation signal (hard ceiling indicator).
        if (options.UnhealthyOnWalSaturated && compute.WalSaturation == WalSaturationState.Saturated)
        {
            status = Worsen(status, HealthStatus.Unhealthy);
        }
        else if (options.DegradeOnWalThrottled && compute.WalSaturation == WalSaturationState.Throttled)
        {
            status = Worsen(status, HealthStatus.Degraded);
        }

        // Advisory storage-axis signal - never escalates past Degraded because
        // the storage axis is not wired to the replica recommendation.
        if (options.DegradeOnStorageOverThreshold && storage.OverThreshold)
        {
            status = Worsen(status, HealthStatus.Degraded);
        }

        var data = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["scaleValue"] = snapshot.ScaleValue,
            ["recommendedReplicas"] = snapshot.RecommendedReplicas,
            ["reason"] = snapshot.Reason ?? string.Empty,
            ["computeActivation"] = compute.Activation,
            ["computeResource"] = compute.Resource,
            ["computeWalDispatch"] = compute.WalDispatch,
            ["computeWorst"] = worstCompute,
            ["walSaturation"] = compute.WalSaturation.ToString(),
            ["storageOverThreshold"] = storage.OverThreshold,
            ["storageWalRetainedBytes"] = storage.WalRetainedBytes,
            ["sampledAt"] = snapshot.SampledAt,
            ["checkedAt"] = _time.GetUtcNow(),
        };

        var description = status switch
        {
            HealthStatus.Healthy => "Scaling signal is within configured pressure thresholds.",
            HealthStatus.Degraded => "Scaling signal indicates elevated pressure on at least one axis.",
            HealthStatus.Unhealthy => "Scaling signal indicates a saturated compute axis; the cluster is under-provisioned.",
            _ => null,
        };

        if (status != HealthStatus.Healthy)
        {
            _logger.LogDebug(
                "Scaling health check returning {Status}: worstCompute={WorstCompute}, walSaturation={WalSaturation}, storageOverThreshold={StorageOverThreshold}",
                status,
                worstCompute,
                compute.WalSaturation,
                storage.OverThreshold);
        }

        return new HealthCheckResult(status, description, exception: null, data: data);
    }

    /// <summary>
    /// Combines the running aggregate with a candidate verdict, keeping the
    /// worse of the two. <see cref="HealthStatus"/> orders
    /// <c>Unhealthy</c> (0) below <c>Degraded</c> (1) below <c>Healthy</c> (2),
    /// so "worse" is the numerically smaller value.
    /// </summary>
    private static HealthStatus Worsen(HealthStatus current, HealthStatus candidate) =>
        candidate < current ? candidate : current;
}
