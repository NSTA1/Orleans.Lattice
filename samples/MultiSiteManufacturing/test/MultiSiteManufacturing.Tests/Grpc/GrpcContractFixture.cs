using System.Net.Http;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

namespace MultiSiteManufacturing.Tests.Grpc;

/// <summary>
/// Hosts the real <c>MultiSiteManufacturing.Host</c> pipeline in-process
/// via <see cref="WebApplicationFactory{TEntryPoint}"/> and hands out
/// gRPC channels wired to the in-memory test server. Overriding the
/// environment to <c>Testing</c> flips <c>Program.cs</c> onto in-memory
/// Orleans storage so the tests don't depend on Azurite.
/// </summary>
public sealed class GrpcContractFixture : IDisposable
{
    private readonly WebApplicationFactory<Program> _factory;
    private readonly List<GrpcChannel> _channels = [];

    public GrpcContractFixture()
    {
        _factory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder => builder.UseEnvironment("Testing"));
    }

    /// <summary>Creates a fresh gRPC channel backed by the in-proc TestServer handler.</summary>
    public GrpcChannel CreateChannel()
    {
        var handler = _factory.Server.CreateHandler();
        var channel = GrpcChannel.ForAddress(
            _factory.Server.BaseAddress,
            new GrpcChannelOptions { HttpHandler = handler });
        _channels.Add(channel);
        return channel;
    }

    /// <summary>Root service provider of the host — useful for resolving FederationRouter directly.</summary>
    public IServiceProvider Services => _factory.Services;

    public void Dispose()
    {
        foreach (var channel in _channels)
        {
            channel.Dispose();
        }
        _factory.Dispose();
    }
}
