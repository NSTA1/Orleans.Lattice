using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Samples.CrossClusterReplication;

/// <summary>
/// Builds one in-process Orleans cluster wired for active-active cross-cluster
/// replication over the canonical gRPC push transport. Two of these, given
/// mirror-image <see cref="SiteConfig"/>s, form a two-site topology that
/// converges a write made on either side onto the other.
/// </summary>
internal static class SiteFactory
{
    /// <summary>The single tree both sites replicate, merged last-writer-wins.</summary>
    public const string TreeName = "orders";

    public static WebApplication Build(SiteConfig site)
    {
        // A WebApplication (not a bare generic host) because the replication
        // gRPC receiver is served over an ASP.NET Core / Kestrel pipeline.
        var builder = WebApplication.CreateBuilder();

        // Deterministic, quiet console: silence the framework + Orleans chatter
        // so the only output is this sample's own before/after narration.
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.None);

        // Serve the inbound replication gRPC endpoint as plaintext HTTP/2
        // (h2c) on this site's local port. Loopback-only, no TLS ceremony.
        // Clear the ASP.NET default URLs first (http://localhost:5000 +
        // https://localhost:5001) so the two sites do not collide on them;
        // the only endpoint each site exposes is its explicit gRPC port.
        builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, string.Empty);
        builder.WebHost.ConfigureKestrel(k =>
            k.ListenLocalhost(site.GrpcPort, o => o.Protocols = HttpProtocols.Http2));

        builder.Host.UseOrleans(silo =>
        {
            // Each site is its own Orleans cluster: distinct ClusterId and
            // distinct silo/gateway ports so both can run in one process.
            silo.UseLocalhostClustering(
                siloPort: site.SiloPort,
                gatewayPort: site.GatewayPort,
                serviceId: "xcluster-sample",
                clusterId: site.ClusterId);
            silo.AddMemoryGrainStorageAsDefault();
            silo.UseInMemoryReminderService();
            silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

            // Opt the "orders" tree into replication and name this site's peer.
            // ReplicatedTrees declares the per-tree merge mode; ReplicationPeers
            // lists the cluster ids this site ships to.
            silo.AddLatticeReplication(opts =>
            {
                opts.ClusterId = site.ClusterId;
                // A replicated tree is single-shape: every value must be
                // authored under the one merge mode declared here. This tree is
                // LwwRegister, so plain SetAsync/DeleteAsync writes are correct.
                // Had it been declared as a CRDT mode (OrSet, PnCounter, ...),
                // the origin cluster would reject any write that did not match -
                // a plain LWW write, or a different CRDT type - with
                // LatticeReplicationModeMismatchException, because the receiver
                // could not decode the bytes under the declared shape. See
                // docs/lattice.replication/replication-modes.md#single-shape-per-tree.
                opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
                {
                    [TreeName] = LatticeMergeMode.LwwRegister,
                };
                opts.ReplicationPeers = new[] { site.PeerClusterId };
            });
        });

        // Cross-cluster gRPC binding. One Peers entry wires both the live-push
        // transport and the bootstrap snapshot transport to the peer's h2c
        // endpoint. AllowPlaintextEndpoints permits the http:// loopback URL;
        // LocalClusterId stamps this site's origin on every outbound batch.
        builder.Services.AddLatticeReplicationGrpc(opts =>
        {
            opts.Peers[site.PeerClusterId] = new Uri($"http://localhost:{site.PeerGrpcPort}");
            opts.AllowPlaintextEndpoints = true;
            opts.LocalClusterId = site.ClusterId;
        });

        // This is a loopback dev sample with no shared secret, so turn off
        // the receiver-side shared-secret authenticator (it is on by default;
        // production deployments must supply a secret and leave it on).
        builder.Services.Configure<LatticeReplicationSecurityOptions>(o =>
            o.RequireAuthentication = false);

        var app = builder.Build();

        // Map the inbound replication routes (live-push + snapshot) onto this
        // site's Kestrel pipeline so the peer can ship batches to it.
        app.MapLatticeReplicationGrpc();

        return app;
    }
}
