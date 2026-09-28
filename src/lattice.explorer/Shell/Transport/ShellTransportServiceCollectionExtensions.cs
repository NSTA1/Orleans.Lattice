using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.Shell.Transport;

/// <summary>
/// Registers the Shell's transport: the per-circuit channel and one scoped
/// adapter per transport-neutral facade the Shell consumes, so every area binds to
/// the facade interface and resolves a real, credential-aware transport.
/// </summary>
/// <remarks>
/// <para>
/// <b>Lifetimes.</b> The channel and every adapter are scoped - one per Blazor
/// circuit - because they carry that circuit's endpoint and sign-in. The only
/// singletons are the stateless <see cref="IShellGrpcChannelFactory"/> and the
/// codec-only <see cref="ShellTransportSerializer"/>, neither of which depends on
/// anything scoped. No singleton may ever take a facade from this registration.
/// </para>
/// <para>
/// <b>What the host must register.</b> The channel reads the circuit's
/// <c>IExplorerSession</c> and <c>IExplorerAuthSession</c> from the Explorer Core
/// services, which the web head registers.
/// </para>
/// <para>
/// Every registration uses <c>TryAdd</c>, so a host (or a test) that registered a
/// facade first keeps its own.
/// </para>
/// </remarks>
internal static class ShellTransportServiceCollectionExtensions
{
    /// <summary>
    /// The facade interfaces this registration binds to a transport adapter, in
    /// registration order.
    /// </summary>
    internal static readonly IReadOnlyList<Type> FacadeTypes =
    [
        typeof(ILatticeAuthAdmin),
        typeof(ILatticeBackupControl),
        typeof(ILatticeSchemaControl),
        typeof(ILatticeTenantAdmin),
        typeof(ILatticeTenantAccessAdmin),
        typeof(ILatticeTenantGrantAdmin),
        typeof(ILatticeTenantRegionAdmin),
        typeof(ILatticeTenantSelfService),
        typeof(ILatticeTenantQuotaUsage),
        typeof(ILatticeTelemetry),
        typeof(ILatticeTreeAdmin),
        typeof(ILatticeReplicationControl),
        typeof(ILatticeReplicationStatus),
        typeof(ILatticeAppsControl),
    ];

    /// <summary>Registers the channel and every transport adapter.</summary>
    /// <param name="services">The service collection to register into.</param>
    /// <returns>The same service collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    internal static IServiceCollection AddShellTransport(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<IShellGrpcChannelFactory, ShellGrpcChannelFactory>();
        services.TryAddSingleton<ShellTransportSerializer>();
        services.TryAddScoped<ShellTransportChannel>();

        services.TryAddScoped<ILatticeAuthAdmin, ShellAuthAdminTransport>();
        services.TryAddScoped<ILatticeBackupControl, ShellBackupControlTransport>();
        services.TryAddScoped<ILatticeSchemaControl, ShellSchemaControlTransport>();
        services.TryAddScoped<ILatticeTenantAdmin, ShellTenantAdminTransport>();
        services.TryAddScoped<ILatticeTenantAccessAdmin, ShellTenantAccessAdminTransport>();
        services.TryAddScoped<ILatticeTenantGrantAdmin, ShellTenantGrantAdminTransport>();
        services.TryAddScoped<ILatticeTenantRegionAdmin, ShellTenantRegionAdminTransport>();
        services.TryAddScoped<ILatticeTenantSelfService, ShellTenantSelfServiceTransport>();
        services.TryAddScoped<ILatticeTenantQuotaUsage, ShellTenantQuotaUsageTransport>();
        services.TryAddScoped<ILatticeTelemetry, ShellTelemetryTransport>();
        services.TryAddScoped<ILatticeTreeAdmin, ShellTreeAdminTransport>();
        services.TryAddScoped<ILatticeReplicationControl, ShellReplicationControlTransport>();
        services.TryAddScoped<ILatticeReplicationStatus, ShellReplicationStatusTransport>();
        services.TryAddScoped<ILatticeAppsControl, ShellAppsControlTransport>();

        return services;
    }

    /// <summary>
    /// Registers a gRPC client that implements its facade directly - for example
    /// the app catalogue, workspace and bridge clients - as a scoped
    /// <typeparamref name="TFacade"/> built over the circuit's channel.
    /// </summary>
    /// <remarks>
    /// The client is built once per circuit over
    /// <see cref="ShellTransportChannel.Invoker"/>, which resolves the current
    /// endpoint and sign-in on every call, so it follows reconfiguration and
    /// sign-in without being rebuilt. Its faults surface as the client itself
    /// documents them; a client that leaves raw transport faults should instead be
    /// wrapped in a <see cref="ShellTransportAdapter{TClient}"/>, as the apps
    /// control and replication status clients are. Like every registration here it
    /// uses <c>TryAdd</c>, and it registers the client before the transport's own
    /// adapters, so it takes a facade those adapters would otherwise serve.
    /// </remarks>
    /// <typeparam name="TFacade">The facade interface the client implements.</typeparam>
    /// <param name="services">The service collection to register into.</param>
    /// <param name="create">The client's factory, for example <c>SomeGrpcClient.Create</c>.</param>
    /// <returns>The same service collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    internal static IServiceCollection AddShellTransportClient<TFacade>(
        this IServiceCollection services,
        Func<CallInvoker, IServiceProvider, TFacade> create)
        where TFacade : class
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(create);

        services.TryAddScoped(provider =>
        {
            var channel = provider.GetRequiredService<ShellTransportChannel>();
            return create(channel.Invoker, channel.SerializerServices);
        });
        services.AddShellTransport();

        return services;
    }
}
