namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Per-region remote endpoint configuration for one additional (peer) region
/// under the <c>Orleans.Lattice.Api.Mcp</c> remote-host topology. Added to
/// <see cref="LatticeApiMcpRemoteOptions.Regions"/> so a caller may optionally
/// target this region on any tool call; the top-level per-group endpoints on
/// <see cref="LatticeApiMcpRemoteOptions"/> define the default (current) region.
/// </summary>
/// <remarks>
/// A region serves a facade group only when that group's per-region endpoint is
/// supplied; an unset group is not routable in the region and is reported
/// unavailable for it in <c>lattice_list_regions</c> (fail-closed discovery). The
/// caller credential the MCP credential bridge resolves flows to this region
/// exactly as it does to the default region, so the peer authorizes the call
/// independently, fail-closed.
/// </remarks>
public sealed class LatticeApiMcpRemoteRegionOptions
{
    /// <summary>
    /// The stable region id a caller targets this region by (the value passed as
    /// the optional per-call <c>region</c> selector). Required and must be unique
    /// across the configured regions.
    /// </summary>
    public required string RegionId { get; set; }

    /// <summary>
    /// The Orleans cluster id this region belongs to, surfaced in
    /// <c>lattice_list_regions</c>; optional advertisement metadata.
    /// </summary>
    public string? ClusterId { get; set; }

    /// <summary>
    /// The region's endpoint for the read-only state facade
    /// (<c>ILatticeStateQuery</c>), or <see langword="null"/> to not serve the
    /// state group in this region.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? State { get; set; }

    /// <summary>
    /// The region's endpoint for the read/write data facade
    /// (<c>ILatticeDataApi</c>), or <see langword="null"/> to not serve the data
    /// group in this region.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? Data { get; set; }

    /// <summary>
    /// The region's endpoint for the auth-admin control plane
    /// (<c>ILatticeAuthAdmin</c>), or <see langword="null"/> to not serve the auth
    /// group in this region.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? Auth { get; set; }

    /// <summary>
    /// The region's endpoint for the backup control facade
    /// (<c>ILatticeBackupControl</c>), or <see langword="null"/> to not serve the
    /// backup group in this region.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? Backup { get; set; }

    /// <summary>
    /// The region's endpoint for the replication control facade
    /// (<c>ILatticeReplicationControl</c>), or <see langword="null"/> to not serve
    /// the replication group in this region.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? Replication { get; set; }

    /// <summary>
    /// The region's endpoint for the tree-administration control facade
    /// (<c>ILatticeTreeAdmin</c>), or <see langword="null"/> to not serve the
    /// tree-administration group in this region.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? TreeAdmin { get; set; }

    /// <summary>
    /// The region's endpoint for the tenant-administration control facade
    /// (<c>ILatticeTenantAdmin</c>) and its read-only self-awareness facade
    /// (<c>ILatticeTenantSelfService</c>), or <see langword="null"/> to not serve
    /// the tenant group in this region.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? TenantAdmin { get; set; }
}
