namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The fail-closed <see cref="IExplorerTenantOperatorGate"/> used when no head
/// supplied a real one: nobody validates as a platform operator, so a
/// cross-tenant request always degrades to the caller's active tenant.
/// </summary>
/// <remarks>
/// A platform-operator signal is a real, probed decision owned by the plugin
/// that performs the probe, so the pure navigation core has none of its own.
/// Registering this default keeps the tenant-view graph resolvable on a head
/// that opts into tenant scoping without registering an administrative surface,
/// and it fails closed rather than admitting an unvalidated caller.
/// </remarks>
internal sealed class DeniedExplorerTenantOperatorGate : IExplorerTenantOperatorGate
{
    /// <summary>The shared fail-closed instance.</summary>
    public static DeniedExplorerTenantOperatorGate Instance { get; } = new();

    private DeniedExplorerTenantOperatorGate()
    {
    }

    /// <inheritdoc />
    public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(false);
}
