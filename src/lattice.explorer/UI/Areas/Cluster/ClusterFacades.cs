using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The transport-neutral facades the Cluster area reads, resolved optionally from
/// the circuit's services. A head that registers none of them gets a hidden area
/// rather than a failed one: every missing facade is a fail-closed "no".
/// </summary>
/// <param name="services">The circuit's service provider.</param>
internal sealed class ClusterFacades(IServiceProvider services)
{
    private readonly Lazy<ILatticeTreeAdmin?> _treeAdmin = new(services.GetShellFacade<ILatticeTreeAdmin>);
    private readonly Lazy<ILatticeReplicationStatus?> _replicationStatus = new(services.GetShellFacade<ILatticeReplicationStatus>);
    private readonly Lazy<IExplorerSession?> _session = new(services.GetService<IExplorerSession>);
    private readonly Lazy<ShellAssertedTenant> _tenant = new(() => services.GetService<ShellAssertedTenant>() ?? ShellAssertedTenant.None);
    private readonly Lazy<ShellCaller> _caller = new(() => ShellCaller.Of(services));

    /// <summary>
    /// The tenant the circuit's calls assert right now, or <see langword="null"/>
    /// when they assert none. Everything the area remembers is keyed on it, so an
    /// answer read under one tenant is never served under another.
    /// </summary>
    public string? AssertedTenant => _tenant.Value.AssertedTenant;

    /// <summary>The tenant a tenant-scoped listing read now is for: the asserted tenant, the reserved default when none is asserted, or <see langword="null"/> with tenancy off.</summary>
    public string? ListingTenant => _tenant.Value.ListingTenant;

    /// <summary>
    /// The caller now - sign-in, endpoint and asserted tenant - which everything the
    /// area remembers is filed under, so an answer read for one caller is never
    /// served to the next.
    /// </summary>
    public ShellCallerKey Caller => _caller.Value.Current;

    /// <summary>Tree administration (T1's adapter), or <see langword="null"/> when the head serves none.</summary>
    public ILatticeTreeAdmin? TreeAdmin => _treeAdmin.Value;

    /// <summary>The replication peer report (R1), or <see langword="null"/> when the head serves none.</summary>
    public ILatticeReplicationStatus? ReplicationStatus => _replicationStatus.Value;

    /// <summary>Core's session, whose connection answers cluster info and the tree catalogue.</summary>
    public IExplorerSession? Session => _session.Value;

    /// <summary>Whether a cluster connection is configured for this circuit.</summary>
    public bool IsConnected => Session is { IsConfigured: true };

    /// <summary>The tree administration facade, or an exception naming why there is none.</summary>
    /// <returns>The facade.</returns>
    /// <exception cref="NotSupportedException">The head serves no tree administration.</exception>
    public ILatticeTreeAdmin RequireTreeAdmin() =>
        TreeAdmin ?? throw new NotSupportedException("This Explorer does not serve tree administration.");
}
