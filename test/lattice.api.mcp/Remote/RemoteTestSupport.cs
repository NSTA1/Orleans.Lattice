using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.Replication.Grpc;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Shared support for the remote-host adapter tests: a single Orleans serializer
/// provider (required by every gRPC client's <c>Create</c>) and factory helpers
/// that build each client over a <see cref="FakeCallInvoker"/>.
/// </summary>
internal static class RemoteTestSupport
{
    /// <summary>A service provider with Orleans serialization registered, shared across the tests.</summary>
    public static IServiceProvider Serializer { get; } =
        new ServiceCollection().AddSerializer().BuildServiceProvider();

    /// <summary>Builds a real <see cref="IOptionsMonitor{T}"/> over the configured remote options.</summary>
    public static IOptionsMonitor<LatticeApiMcpRemoteOptions> OptionsMonitor(Action<LatticeApiMcpRemoteOptions> configure)
    {
        var services = new ServiceCollection();
        services.Configure(configure);
        return services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeApiMcpRemoteOptions>>();
    }

    /// <summary>Builds a real <see cref="IOptions{T}"/> over the configured remote options.</summary>
    public static IOptions<LatticeApiMcpRemoteOptions> Options(Action<LatticeApiMcpRemoteOptions> configure)
    {
        var options = new LatticeApiMcpRemoteOptions();
        configure(options);
        return Microsoft.Extensions.Options.Options.Create(options);
    }

    /// <summary>Builds a state-API client over <paramref name="invoker"/>.</summary>
    public static LatticeStateApiGrpcClient StateClient(CallInvoker invoker)
        => LatticeStateApiGrpcClient.Create(invoker, Serializer);

    /// <summary>Builds a data-API client over <paramref name="invoker"/>.</summary>
    public static LatticeDataApiGrpcClient DataClient(CallInvoker invoker)
        => LatticeDataApiGrpcClient.Create(invoker, Serializer);

    /// <summary>Builds an auth-API client over <paramref name="invoker"/>.</summary>
    public static LatticeAuthApiGrpcClient AuthClient(CallInvoker invoker)
        => LatticeAuthApiGrpcClient.Create(invoker, Serializer);

    /// <summary>Builds a backup-API client over <paramref name="invoker"/>.</summary>
    public static LatticeBackupApiGrpcClient BackupClient(CallInvoker invoker)
        => LatticeBackupApiGrpcClient.Create(invoker, Serializer);

    /// <summary>Builds a replication-API client over <paramref name="invoker"/>.</summary>
    public static LatticeReplicationApiGrpcClient ReplicationClient(CallInvoker invoker)
        => LatticeReplicationApiGrpcClient.Create(invoker, Serializer);
}
