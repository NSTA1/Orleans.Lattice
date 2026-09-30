namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The production <see cref="ITenantPolicyEpochPublisher"/>: advances the epoch on
/// the single cluster-wide <see cref="ITenantPolicyEpochGrain"/>.
/// </summary>
/// <param name="grainFactory">The silo's grain factory.</param>
internal sealed class GrainTenantPolicyEpochPublisher(IGrainFactory grainFactory) : ITenantPolicyEpochPublisher
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public Task AdvanceAsync(CancellationToken cancellationToken) =>
        _grainFactory.GetGrain<ITenantPolicyEpochGrain>(ITenantPolicyEpochGrain.Key)
            .AdvanceAsync()
            .WaitAsync(cancellationToken);
}
