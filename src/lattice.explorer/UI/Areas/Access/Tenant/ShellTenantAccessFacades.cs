using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

/// <summary>
/// The delegated tenant-access facades the Shell's own transport serves, resolved
/// lazily and optionally from the circuit's services. A head whose transport does
/// not serve them yields <see langword="null"/>, so the tenant pages keep their
/// cluster-wide behaviour.
/// </summary>
/// <param name="services">The circuit's services.</param>
internal sealed class ShellTenantAccessFacades(IServiceProvider services) : ITenantAccessFacades
{
    private readonly Lazy<ILatticeTenantDirectoryAdmin?> _directory = new(() => Resolve<ILatticeTenantDirectoryAdmin>(services));
    private readonly Lazy<ILatticeTenantPolicyAdmin?> _policy = new(() => Resolve<ILatticeTenantPolicyAdmin>(services));

    /// <inheritdoc />
    public ILatticeTenantDirectoryAdmin? Directory => _directory.Value;

    /// <inheritdoc />
    public ILatticeTenantPolicyAdmin? Policy => _policy.Value;

    private static TFacade? Resolve<TFacade>(IServiceProvider services)
        where TFacade : class
    {
        try
        {
            return services.GetShellFacade<TFacade>();
        }
        catch (InvalidOperationException)
        {
            // A registration the head cannot construct is a facade it does not serve.
            return null;
        }
    }
}
