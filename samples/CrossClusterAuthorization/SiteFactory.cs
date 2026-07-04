using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Samples.CrossClusterAuthorization;

/// <summary>
/// Builds one in-process Orleans cluster wired with the full authorization
/// stack (Membership + Auth + the State/Auth admin APIs) and cross-cluster
/// replication of the reserved membership/auth system trees. Two of these, given
/// mirror-image <see cref="SiteConfig"/>s, form a two-site topology whose policy
/// and membership surface converges across sites, so a revoke authored on one
/// site becomes enforced on the other.
/// </summary>
internal static class SiteFactory
{
    /// <summary>The single data tree both sites replicate, merged last-writer-wins.</summary>
    public const string TreeName = "production-line";

    public static WebApplication Build(SiteConfig site)
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.None);

        // Serve the inbound replication gRPC endpoint as plaintext HTTP/2 (h2c)
        // on this site's local port; clear the ASP.NET default URLs so the two
        // sites do not collide on them.
        builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, string.Empty);
        builder.WebHost.ConfigureKestrel(k =>
            k.ListenLocalhost(site.GrpcPort, o => o.Protocols = HttpProtocols.Http2));

        builder.Host.UseOrleans(silo =>
        {
            // Each site is its own Orleans cluster: distinct ClusterId and ports
            // so both run in one process.
            silo.UseLocalhostClustering(
                siloPort: site.SiloPort,
                gatewayPort: site.GatewayPort,
                serviceId: "cross-cluster-authorization-sample",
                clusterId: site.ClusterId);
            silo.AddMemoryGrainStorageAsDefault();
            silo.UseInMemoryReminderService();
            silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

            // Membership resolves the ambient caller credential into a subject
            // whose groups are expanded from the directory's user/group edges.
            silo.AddLatticeMembership();

            // Auth installs the enforcement gate. Default-deny: only explicit
            // allow rules grant access. "root-admin" is a bootstrap administrator
            // so the sample can seed users/groups/rules before any rule exists.
            silo.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add("root-admin");
            });

            // Replicate the data tree last-writer-wins so a write on one site
            // converges on the other.
            silo.AddLatticeReplication(opts =>
            {
                opts.ClusterId = site.ClusterId;
                opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
                {
                    [TreeName] = LatticeMergeMode.LwwRegister,
                };
                opts.ReplicationPeers = new[] { site.PeerClusterId };
            });

            // The system-tree replication special case: enrol the reserved
            // membership + authorization-policy trees into replication so the
            // identity and policy surface converges across sites. This is what
            // makes a revoke authored on one site become enforced on the other.
            silo.ReplicateLatticeSystemTrees();

            // The trusted-token authenticator that maps the ambient credential's
            // token to the caller subject id (a real deployment uses JWT/Entra).
            silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DemoAuthenticator>();
        });

        // Cross-cluster gRPC binding to the peer's h2c endpoint.
        builder.Services.AddLatticeReplicationGrpc(opts =>
        {
            opts.Peers[site.PeerClusterId] = new Uri($"http://localhost:{site.PeerGrpcPort}");
            opts.AllowPlaintextEndpoints = true;
            opts.LocalClusterId = site.ClusterId;
        });

        // Loopback dev sample with no shared secret: turn off the receiver-side
        // shared-secret authenticator (production must supply a secret).
        builder.Services.Configure<LatticeReplicationSecurityOptions>(o =>
            o.RequireAuthentication = false);

        var app = builder.Build();

        // Map the inbound replication routes so the peer can ship batches here.
        app.MapLatticeReplicationGrpc();

        return app;
    }
}
