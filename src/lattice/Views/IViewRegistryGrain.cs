namespace Orleans.Lattice.Views;

/// <summary>
/// Cluster-wide durable registry of materialised views created at runtime through
/// <see cref="ILatticeViewFactory.Create"/>. A single activation (keyed by the
/// well-known <see cref="SingletonKey"/>) persists every runtime view's
/// <see cref="RuntimeViewRegistration"/> so the hosted view-activation service can
/// re-register them into the in-memory <see cref="IViewCatalog"/> and re-activate
/// their maintainers on silo start, making runtime views survive a restart
/// identically to startup-declared views (until they are explicitly deleted).
/// <para>
/// Startup-declared views are <b>not</b> recorded here: they are declarative and
/// authoritative through <c>AddLatticeViews</c>, and on a name conflict the
/// startup declaration wins.
/// </para>
/// </summary>
[Alias(TypeAliases.IViewRegistryGrain)]
internal interface IViewRegistryGrain : IGrainWithStringKey
{
    /// <summary>
    /// The well-known singleton grain key for the cluster-wide runtime-view
    /// registry. A single fixed key collapses every silo's registry reference onto
    /// one cluster-wide activation; the leading underscore marks it as a reserved
    /// internal key rather than a caller-supplied view name.
    /// </summary>
    const string SingletonKey = "_lattice_view_registry";

    /// <summary>
    /// Records (or replaces) the durable registration for
    /// <paramref name="registration"/>'s view name. Idempotent for an identical
    /// registration.
    /// </summary>
    Task RegisterAsync(RuntimeViewRegistration registration);

    /// <summary>
    /// Removes the durable registration for <paramref name="viewName"/>. A no-op
    /// when no runtime registration by that name exists.
    /// </summary>
    Task UnregisterAsync(string viewName);

    /// <summary>Returns a snapshot of every durable runtime-view registration.</summary>
    Task<IReadOnlyList<RuntimeViewRegistration>> ListAsync();
}
