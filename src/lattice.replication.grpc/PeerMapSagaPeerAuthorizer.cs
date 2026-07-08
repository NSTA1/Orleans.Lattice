using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Default <see cref="ISagaPeerAuthorizer"/> for the gRPC binding. It
/// authorizes exactly the configured replication peer set: an origin
/// cluster is authorized when it is a non-empty key in the unified
/// <see cref="LatticeReplicationGrpcOptions.Peers"/> map (the clusters
/// this silo is set up to replicate with). Any unknown, empty, or
/// whitespace origin is rejected, so an unauthorized peer's saga control
/// call is denied before it reaches
/// <see cref="ILatticeSagaControlHandler"/>.
/// </summary>
internal sealed class PeerMapSagaPeerAuthorizer : ISagaPeerAuthorizer
{
    private readonly IOptionsMonitor<LatticeReplicationGrpcOptions> _options;

    /// <summary>Initialises the authorizer.</summary>
    public PeerMapSagaPeerAuthorizer(IOptionsMonitor<LatticeReplicationGrpcOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(string? originClusterId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(originClusterId))
        {
            return Task.FromResult(false);
        }

        var authorized = _options.CurrentValue.Peers.ContainsKey(originClusterId);
        return Task.FromResult(authorized);
    }
}
