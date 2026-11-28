using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Warns once at start-up when the authorization layer is registered on a silo
/// whose Orleans client gateway accepts connections with no inbound connection
/// middleware at all - that is, without TLS and therefore without client-certificate
/// authentication (issue #5346).
/// </summary>
/// <remarks>
/// <para>
/// Orleans does not authenticate the client id a gateway connection announces, so a
/// client that can reach the gateway port can claim another silo's in-silo hosted
/// client id (<c>hosted-{silo}</c>) and keep reserved capability keys that
/// <c>LatticeCapabilityStrippingCallFilter</c> would otherwise strip. The gateway
/// port is therefore a trusted-infrastructure boundary; untrusted callers belong on
/// the gRPC and HTTP facades.
/// </para>
/// <para>
/// Orleans exposes no reliable check for "the gateway requires a client certificate":
/// <c>UseTls</c> captures its <c>TlsOptions</c> in a closure registered on
/// <see cref="SiloConnectionOptions"/> rather than in the options system. The only
/// observable condition is whether any gateway inbound middleware is configured, so
/// this service warns when there is none, and when there is some it logs at
/// information level that the client-certificate mode cannot be verified.
/// </para>
/// </remarks>
internal sealed class GatewayTrustBoundaryWarner(
    ILogger<GatewayTrustBoundaryWarner> logger,
    IOptions<EndpointOptions> endpointOptions,
    IOptions<SiloConnectionOptions> connectionOptions,
    IServiceProvider services) : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var gatewayPort = endpointOptions.Value.GatewayPort;
        if (gatewayPort <= 0)
        {
            return Task.CompletedTask;
        }

        var probe = new CountingConnectionBuilder(services);
        try
        {
            ((SiloConnectionOptions.ISiloConnectionBuilderOptions)connectionOptions.Value)
                .ConfigureGatewayInboundBuilder(probe);
        }
        catch (Exception ex)
        {
            // A diagnostic must never fail silo start-up.
            logger.LogDebug(ex, "Could not inspect the Orleans gateway inbound connection pipeline.");
            return Task.CompletedTask;
        }

        if (probe.MiddlewareCount == 0)
        {
            logger.LogWarning(
                "Lattice authorization is registered but the Orleans client gateway on port {GatewayPort} accepts "
                    + "connections without TLS or client-certificate authentication. Orleans does not authenticate the "
                    + "client id a gateway connection announces, so any client that can reach this port can claim a "
                    + "silo's hosted client id and keep reserved Lattice capabilities. Expose the gateway only to "
                    + "trusted infrastructure (or require client certificates with UseTls and "
                    + "RemoteCertificateMode.RequireCertificate) and route untrusted callers through the gRPC or HTTP "
                    + "facades. See issue #5346.",
                gatewayPort);
        }
        else
        {
            logger.LogInformation(
                "Lattice authorization is registered and the Orleans client gateway on port {GatewayPort} has "
                    + "{MiddlewareCount} inbound connection middleware component(s) configured. Lattice cannot verify "
                    + "that they require a client certificate; the gateway must admit trusted infrastructure only "
                    + "(see issue #5346).",
                gatewayPort,
                probe.MiddlewareCount);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Records how many middleware components a connection-builder delegate adds
    /// without building or invoking any of them.
    /// </summary>
    private sealed class CountingConnectionBuilder(IServiceProvider services) : IConnectionBuilder
    {
        public int MiddlewareCount { get; private set; }

        public IServiceProvider ApplicationServices { get; } = services;

        public IConnectionBuilder Use(Func<ConnectionDelegate, ConnectionDelegate> middleware)
        {
            MiddlewareCount++;
            return this;
        }

        public ConnectionDelegate Build() => static _ => Task.CompletedTask;
    }
}
