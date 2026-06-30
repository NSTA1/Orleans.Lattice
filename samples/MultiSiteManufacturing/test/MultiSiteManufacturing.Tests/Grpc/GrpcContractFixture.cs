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
public sealed class GrpcContractFixture : IAsyncDisposable
{
    /// <summary>
    /// Upper bound on how long we wait for a single host (and its
    /// co-hosted Orleans silo) to tear down. Each contract test boots a
    /// fresh full host; during silo shutdown an Orleans stream-consumer
    /// <c>UnsubscribeAsync</c> can hang indefinitely when the streaming
    /// pub-sub runtime is already stopping. A hung teardown must never
    /// wedge the test run (it would leak silos and eventually trip the
    /// blame-hang dump), so disposal is bounded - we abandon a stuck silo
    /// and let the process reclaim it at exit rather than block forever.
    /// </summary>
    private static readonly TimeSpan DisposeTimeout = TimeSpan.FromSeconds(20);

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

    /// <summary>Root service provider of the host - useful for resolving FederationRouter directly.</summary>
    public IServiceProvider Services => _factory.Services;

    public async ValueTask DisposeAsync()
    {
        foreach (var channel in _channels)
        {
            channel.Dispose();
        }

        try
        {
            await _factory.DisposeAsync().AsTask().WaitAsync(DisposeTimeout);
        }
        catch (TimeoutException)
        {
            // Silo teardown wedged on a hung stream-unsubscribe; abandon it
            // so the remaining tests (and final process exit) aren't blocked.
        }
    }
}
