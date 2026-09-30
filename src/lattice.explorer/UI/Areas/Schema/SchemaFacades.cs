using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The services the Schema area reads, resolved optionally from the circuit. A
/// head that registers no schema facade gets a hidden area rather than a failed
/// one: every missing service is a fail-closed "no".
/// </summary>
/// <param name="services">The circuit's service provider.</param>
internal sealed class SchemaFacades(IServiceProvider services)
{
    private readonly Lazy<ILatticeSchemaControl?> _schema = new(services.GetShellFacade<ILatticeSchemaControl>);
    private readonly Lazy<ILatticeAppsControl?> _apps = new(services.GetShellFacade<ILatticeAppsControl>);
    private readonly Lazy<IExplorerSession?> _session = new(services.GetService<IExplorerSession>);
    private readonly Lazy<IExplorerAuthSession?> _auth = new(services.GetService<IExplorerAuthSession>);
    private readonly Lazy<ILatticeActiveTenantProvider?> _tenant = new(services.GetService<ILatticeActiveTenantProvider>);

    /// <summary>
    /// The tenant the circuit's calls assert right now, or <see langword="null"/>
    /// when they assert none. Everything the area remembers is keyed on it, so an
    /// answer read under one tenant is never served under another.
    /// </summary>
    public string? AssertedTenant => _tenant.Value?.AssertedTenant;

    /// <summary>
    /// The tenant the area's listings are for: <see langword="null"/> with tenancy
    /// off, otherwise the asserted tenant or the reserved default one.
    /// </summary>
    public string? ListingTenant => ShellAssertedTenant.ListingTenantOf(_tenant.Value);

    /// <summary>The schema control facade (T1's adapter), or <see langword="null"/> when the head serves none.</summary>
    public ILatticeSchemaControl? Schema => _schema.Value;

    /// <summary>The apps control facade, read only for manifest schema declarations; <see langword="null"/> when absent.</summary>
    public ILatticeAppsControl? Apps => _apps.Value;

    /// <summary>Core's session, whose connection lists the trees.</summary>
    public IExplorerSession? Session => _session.Value;

    /// <summary>Core's sign-in session, read to tell an anonymous refusal from a signed-in one.</summary>
    public IExplorerAuthSession? Auth => _auth.Value;

    /// <summary>The schema control facade, or an exception naming why there is none.</summary>
    /// <returns>The facade.</returns>
    /// <exception cref="NotSupportedException">The head serves no schema administration.</exception>
    public ILatticeSchemaControl RequireSchema() =>
        Schema ?? throw new NotSupportedException("This Explorer does not serve schema administration.");
}
