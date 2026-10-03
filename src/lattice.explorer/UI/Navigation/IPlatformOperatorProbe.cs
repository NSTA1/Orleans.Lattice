namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// An area whose visibility is wider than platform-operator standing, and which
/// therefore answers the operator question separately. The Access area is visible
/// to a tenant administrator under delegated tenant access administration, and
/// that must never read as standing over every tenant.
/// </summary>
internal interface IPlatformOperatorProbe
{
    /// <summary>
    /// Whether the caller administers access for the whole cluster: the question the
    /// platform-operator gate asks, without the tenant-scoped admission that widens
    /// the area's own visibility.
    /// </summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><see langword="true"/> only when the cluster proved the standing.</returns>
    ValueTask<bool> IsClusterAccessAdministratorAsync(CancellationToken cancellationToken);
}
