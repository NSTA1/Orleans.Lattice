using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Scaffold <see cref="ILatticeScalingSignal"/> implementation. Performs no live
/// pressure collection: it returns a well-formed zero signal with an all-healthy
/// compute axis, a not-over-threshold storage axis, and
/// <see cref="ScalingSignal.Reason"/> set to <see cref="NotYetCollecting"/>.
/// Replaced by the real collector-backed implementation in later issues (#1186
/// compute, #1187 storage, #1188 endpoint); registered via
/// <see cref="Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddSingleton(Microsoft.Extensions.DependencyInjection.IServiceCollection, System.Type, System.Type)"/>
/// so those issues can substitute a richer implementation.
/// </summary>
internal sealed class StubLatticeScalingSignal(
    IOptions<LatticeScalingSignalOptions> options,
    TimeProvider timeProvider) : ILatticeScalingSignal
{
    /// <summary>
    /// The <see cref="ScalingSignal.Reason"/> value the scaffold reports while
    /// no live collection is wired up.
    /// </summary>
    internal const string NotYetCollecting = "not yet collecting";

    private readonly IOptions<LatticeScalingSignalOptions> _options = options;
    private readonly TimeProvider _timeProvider = timeProvider;

    /// <inheritdoc />
    public Task<ScalingSignal> GetScalingSignalAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var floor = Math.Max(0, _options.Value.MinReplicas);

        var signal = new ScalingSignal
        {
            ScaleValue = 0d,
            RecommendedReplicas = floor,
            Compute = default,
            Storage = default,
            Reason = NotYetCollecting,
            SampledAt = _timeProvider.GetUtcNow(),
        };

        return Task.FromResult(signal);
    }
}
