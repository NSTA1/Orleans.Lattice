using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Warns at startup when <see cref="LatticeReplicationSecurityOptions.BindCredentialToOriginCluster"/>
/// is on but this cluster resolves one cluster-wide secret for every origin. In that mode
/// the binding proves only that the caller holds the mesh secret, never which cluster it
/// is: any origin a caller claims resolves the same secret, so the binding passes. The
/// estate keeps working - nothing is refused - but it has no per-origin authentication,
/// and an operator who believes otherwise is told so once, at startup.
/// </summary>
/// <remarks>
/// The check probes the secret source with an origin no configured peer can have. A
/// source that resolves a secret for it falls back to a cluster-wide secret; when no
/// configured peer resolves a different one, no origin is authenticated individually.
/// It runs once and never fails startup.
/// </remarks>
internal sealed class ReplicationOriginBindingStartupCheck(
    IReplicationSecretProvider secrets,
    IOptionsMonitor<LatticeReplicationSecurityOptions> options,
    ILogger<ReplicationOriginBindingStartupCheck> logger,
    IReplicationTopology? topology = null) : IHostedService
{
    /// <summary>The origin id the probe resolves; no cluster is configured under it.</summary>
    internal const string ProbeOrigin = "lattice-origin-binding-probe-unconfigured";

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (await IsClusterWideOnlyAsync(cancellationToken).ConfigureAwait(false))
            {
                logger.LogWarning(
                    "Replication: BindCredentialToOriginCluster is on, but this cluster resolves one cluster-wide "
                    + "secret for every origin, so the binding authenticates possession of the mesh secret, not a "
                    + "cluster identity: a holder of that secret can claim any origin. Configure a secret per peer "
                    + "for per-origin authentication.");
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(ex, "Replication: the origin-binding startup check could not resolve the secret source.");
        }
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Whether binding is on and every origin - an unconfigured probe and each configured
    /// peer alike - resolves the same cluster-wide secret.
    /// </summary>
    internal async Task<bool> IsClusterWideOnlyAsync(CancellationToken cancellationToken)
    {
        var current = options.CurrentValue;
        if (!current.RequireAuthentication || !current.BindCredentialToOriginCluster)
        {
            return false;
        }

        var shared = await secrets.GetOutboundSecretAsync(ProbeOrigin, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrEmpty(shared))
        {
            return false;
        }

        foreach (var peer in topology?.CurrentPeers ?? [])
        {
            var secret = await secrets.GetOutboundSecretAsync(peer, cancellationToken).ConfigureAwait(false);
            if (!string.Equals(secret, shared, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }
}
