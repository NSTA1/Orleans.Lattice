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
        // Telemetry has no per-region gRPC endpoint by design: it is served by a
        // head-local PromQL proxy co-located with the MCP server rather than a
        // routable per-region facade, so its endpoint is intentionally null.
        _ => null,
    };
}
