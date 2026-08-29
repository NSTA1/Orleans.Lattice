using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The remote-host <see cref="ILatticeApiMcpGroupEndpointSource"/>: projects each
/// facade group's served endpoint from the configured
/// <see cref="LatticeApiMcpRemoteOptions"/> so the capabilities report advertises
/// the correct per-group endpoint. A group with no configured endpoint resolves
/// to <see langword="null"/>.
/// </summary>
internal sealed class LatticeApiMcpRemoteGroupEndpointSource : ILatticeApiMcpGroupEndpointSource
{
    private readonly string? _state;
    private readonly string? _data;
    private readonly string? _auth;
    private readonly string? _backup;
    private readonly string? _replication;
    private readonly string? _treeAdmin;
    private readonly string? _tenantAdmin;
    private readonly string? _telemetry;

    /// <summary>Captures the per-group endpoints from the resolved remote options.</summary>
    public LatticeApiMcpRemoteGroupEndpointSource(IOptions<LatticeApiMcpRemoteOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var value = options.Value;
        _state = value.State?.Endpoint;
        _data = value.Data?.Endpoint;
        _auth = value.Auth?.Endpoint;
        _backup = value.Backup?.Endpoint;
        _replication = value.Replication?.Endpoint;
        _treeAdmin = value.TreeAdmin?.Endpoint;
        _tenantAdmin = value.TenantAdmin?.Endpoint;
        _telemetry = value.Telemetry?.Endpoint;
    }

    /// <inheritdoc />
    public string? EndpointFor(LatticeApiMcpGroup group) => group switch
    {
        LatticeApiMcpGroup.State => _state,
        LatticeApiMcpGroup.Data => _data,
        LatticeApiMcpGroup.Auth => _auth,
        LatticeApiMcpGroup.Backup => _backup,
        LatticeApiMcpGroup.Replication => _replication,
        LatticeApiMcpGroup.TreeAdmin => _treeAdmin,
        LatticeApiMcpGroup.TenantAdmin => _tenantAdmin,
        // Telemetry is a routable per-region facade like every sibling group. It
        // resolves to null only when this head serves telemetry from the co-located
        // tool module instead, which is the same "co-hosted, no separate endpoint"
        // shape the in-silo topology reports for every group.
        LatticeApiMcpGroup.Telemetry => _telemetry,
        // The repository-context group is served from the head's own Lattice trees
        // rather than a dedicated facade endpoint, so it advertises none.
        _ => null,
    };
}
