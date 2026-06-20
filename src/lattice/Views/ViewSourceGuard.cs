namespace Orleans.Lattice.Views;

/// <summary>
/// Default <see cref="IViewSourceGuard"/>. Unions three view-registration sources
/// so the dependent-view lookup is authoritative regardless of where a view was
/// declared or whether its maintainer has activated on the calling silo:
/// <list type="bullet">
/// <item><description>the startup-declared registrations captured by <c>AddLatticeViews</c>;</description></item>
/// <item><description>the in-memory <see cref="IViewCatalog"/> (covers a runtime view registered on this silo whose durable record may not have landed yet);</description></item>
/// <item><description>the cluster-wide durable <see cref="IViewRegistryGrain"/> (covers a runtime view created on another silo).</description></item>
/// </list>
/// </summary>
internal sealed class ViewSourceGuard(
    IGrainFactory grainFactory,
    IViewCatalog catalog,
    IReadOnlyList<StartupViewRegistration> startupRegistrations) : IViewSourceGuard
{
    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> FindDependentViewsAsync(string sourceTreeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceTreeId);

        var dependents = new SortedSet<string>(StringComparer.Ordinal);

        for (var i = 0; i < startupRegistrations.Count; i++)
        {
            if (string.Equals(startupRegistrations[i].SourceTreeId, sourceTreeId, StringComparison.Ordinal))
            {
                dependents.Add(startupRegistrations[i].ViewName);
            }
        }

        foreach (var registration in catalog.All())
        {
            if (string.Equals(registration.SourceTreeId, sourceTreeId, StringComparison.Ordinal))
            {
                dependents.Add(registration.ViewName);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        var durable = await grainFactory
            .GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey)
            .ListAsync();
        for (var i = 0; i < durable.Count; i++)
        {
            if (string.Equals(durable[i].SourceTreeId, sourceTreeId, StringComparison.Ordinal))
            {
                dependents.Add(durable[i].ViewName);
            }
        }

        return dependents.Count == 0 ? [] : [.. dependents];
    }
}
