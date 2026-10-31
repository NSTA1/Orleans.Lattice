using System.Text;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>API-level access to the estate as a user, for arranging and verifying state a journey is not itself exercising.</summary>
public sealed partial class JourneyEstate
{
    private static readonly Lazy<IServiceProvider> SerializerProvider = new(() =>
        new ServiceCollection().AddSerializer().BuildServiceProvider());

    private readonly Dictionary<string, GrpcChannel> _channels = new(StringComparer.Ordinal);

    /// <summary>
    /// The Orleans serialization services a facade's gRPC client is created over
    /// (for example <c>LatticeDataApiGrpcClient.Create(estate.Invoker(), JourneyEstate.Serialization)</c>).
    /// </summary>
    public static IServiceProvider Serialization => SerializerProvider.Value;

    /// <summary>
    /// A gRPC call invoker that reaches <paramref name="region"/> through its gateway, over TLS
    /// verified against the run's CA, signed in as <paramref name="user"/> with a Basic
    /// credential - exactly what the Explorer sends on that user's behalf.
    /// </summary>
    /// <param name="user">The subject id; <see cref="JourneyEstateOptions.Administrator"/> by default.</param>
    /// <param name="password">The password; the directory's by default. Pass another to be refused.</param>
    /// <param name="region">The region.</param>
    /// <param name="tenant">The tenant to assert, or <see langword="null"/> for none.</param>
    public CallInvoker Invoker(string user = JourneyEstateOptions.Administrator, string? password = null, string region = West, string? tenant = null)
    {
        GrpcChannel channel;
        lock (_channels)
        {
            if (!_channels.TryGetValue(region, out channel!))
            {
                var handler = new SocketsHttpHandler { EnableMultipleHttp2Connections = true };
                handler.SslOptions.RemoteCertificateValidationCallback = JourneyHostEnvironment.TrustOnly(_certificates.Authority);
                channel = GrpcChannel.ForAddress(RegionOf(region).GatewayAddress, new GrpcChannelOptions { HttpHandler = handler, DisposeHttpClient = true });
                _channels[region] = channel;
            }
        }

        var authorization = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + (password ?? PasswordOf(user))));
        return channel.Intercept(metadata =>
        {
            metadata.Add("authorization", authorization);
            if (!string.IsNullOrEmpty(tenant))
            {
                metadata.Add(LatticeActiveTenantAssertion.DefaultHeaderName, tenant);
            }

            return metadata;
        });
    }
}
