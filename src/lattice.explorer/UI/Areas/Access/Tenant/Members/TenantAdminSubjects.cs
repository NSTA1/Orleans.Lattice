using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Members;

/// <summary>
/// Reads a tenant's administrator entries for the Members page, which lists them
/// read-only as implicit members (D5). It reads through the tenant access facade
/// the Tenancy area's admin editor writes through, and remembers nothing, so an
/// entry just added there is seen on the next read.
/// </summary>
/// <remarks>
/// Absent or failing, the answer is <see langword="null"/> - unknown - never an
/// empty list: a page must not say a tenant has no administrators when it could
/// not read them.
/// </remarks>
/// <param name="services">The circuit's services, through which the facade is resolved lazily and optionally.</param>
internal sealed class TenantAdminSubjects(IServiceProvider services)
{
    private readonly IServiceProvider _services = services ?? throw new ArgumentNullException(nameof(services));

    /// <summary>Reads <paramref name="tenant"/>'s administrator entries, in ascending ordinal order.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The entries, or <see langword="null"/> when they could not be read.</returns>
    /// <exception cref="OperationCanceledException">The read was cancelled.</exception>
    public async Task<IReadOnlyList<string>?> ReadAsync(string tenant, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        ILatticeTenantAccessAdmin? access;
        try
        {
            access = _services.GetShellFacade<ILatticeTenantAccessAdmin>();
        }
        catch (InvalidOperationException)
        {
            // A registered facade whose own dependencies are missing cannot serve this circuit.
            return null;
        }

        if (access is null)
        {
            return null;
        }

        try
        {
            var report = await access.ListAdminSubjectsAsync(tenant, cancellationToken).ConfigureAwait(true);
            if (report?.Subjects is not { } subjects)
            {
                return null;
            }

            var ordered = new string[subjects.Count];
            for (var i = 0; i < ordered.Length; i++)
            {
                ordered[i] = subjects[i];
            }

            Array.Sort(ordered, StringComparer.Ordinal);
            return ordered;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return null;
        }
    }
}
