using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Shared construction for the delegated tenant access binding tests: a service over
/// substitute <see cref="ILatticeTenantDirectoryAdmin"/> and
/// <see cref="ILatticeTenantPolicyAdmin"/> facades (either of which may be absent),
/// and a client wired to it through the in-memory <see cref="LoopbackCallInvoker"/>,
/// so every call round-trips through the real Orleans marshallers.
/// </summary>
internal sealed class TenantAccessGrpcHarness : IDisposable
{
    private readonly ServiceProvider _serializers;

    public TenantAccessGrpcHarness(bool withDirectory = true, bool withPolicy = true)
    {
        _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();
        Directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        Policy = Substitute.For<ILatticeTenantPolicyAdmin>();
        Methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        Service = new LatticeTenantAdminGrpcService(
            Methods,
            new FakeTenantAdmin(),
            new FakeTenantSelfService(),
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Options.Create(new LatticeTenantAdminApiGrpcOptions()),
            NullLogger<LatticeTenantAdminGrpcService>.Instance,
            new FakeTenantRegionAdmin(),
            new FakeTenantQuotaUsage(),
            new FakeTenantAccessAdmin(),
            new FakeTenantGrantAdmin(),
            withDirectory ? Directory : null,
            withPolicy ? Policy : null);
        Client = new LatticeTenantAdminApiGrpcClient(new LoopbackCallInvoker(Service, _serializers), Methods);
    }

    /// <summary>The substitute directory facade (registered only when the harness was built with it).</summary>
    public ILatticeTenantDirectoryAdmin Directory { get; }

    /// <summary>The substitute policy facade (registered only when the harness was built with it).</summary>
    public ILatticeTenantPolicyAdmin Policy { get; }

    public LatticeTenantAdminGrpcMethods Methods { get; }

    public LatticeTenantAdminGrpcService Service { get; }

    public LatticeTenantAdminApiGrpcClient Client { get; }

    public void Dispose() => _serializers.Dispose();

    /// <summary>A server call context for a call to <paramref name="methodName"/> on the binding.</summary>
    public static FakeServerCallContext Context(string methodName) =>
        new("/" + LatticeTenantAdminGrpcMethods.ServiceName + "/" + methodName);
}
