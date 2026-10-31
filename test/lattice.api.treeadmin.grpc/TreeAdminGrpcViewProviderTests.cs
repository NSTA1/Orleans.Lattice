using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// The read-only <c>ListViewProviders</c> RPC (#5369) of the tree-admin gRPC
/// binding, without a cluster: it adapts onto
/// <see cref="ILatticeTreeAdmin.ListViewProvidersAsync"/>, a refusal maps to
/// <see cref="StatusCode.PermissionDenied"/>, the interceptor names it with its own
/// appended operation and the source tree, and the client sends the source to the
/// right RPC.
/// </summary>
[TestFixture]
public sealed class TreeAdminGrpcViewProviderTests
{
    private ServiceProvider _serializerProvider = null!;
    private LatticeTreeAdminGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeTreeAdminGrpcMethods.FromServiceProvider(_serializerProvider);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _serializerProvider.Dispose();

    private LatticeTreeAdminGrpcService CreateService(ILatticeTreeAdmin control)
    {
        var bridge = Substitute.For<ILatticeTreeAdminApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        return new LatticeTreeAdminGrpcService(
            _methods,
            control,
            bridge,
            Substitute.For<ILatticeTreeAdminApiAuthSchemeSource>(),
            Options.Create(new LatticeTreeAdminApiGrpcOptions()),
            NullLogger<LatticeTreeAdminGrpcService>.Instance);
    }

    private static FakeServerCallContext Context() =>
        new("/" + LatticeTreeAdminGrpcMethods.ServiceName + "/" + LatticeTreeAdminGrpcMethods.ListViewProvidersMethodName);

    private static TreeViewProviderCatalog Catalog() => new()
    {
        Providers = [new TreeViewProviderInfo { ProviderKey = "by-status.v1", Description = "Orders by status" }],
    };

    [Test]
    public async Task The_rpc_adapts_onto_the_facade()
    {
        var control = Substitute.For<ILatticeTreeAdmin>();
        var catalog = Catalog();
        control.ListViewProvidersAsync("orders", Arg.Any<CancellationToken>()).Returns(catalog);

        var answered = await CreateService(control).ListViewProviders(new TreeAdminTreeRequest { TreeId = "orders" }, Context());

        Assert.That(answered, Is.SameAs(catalog));
    }

    [Test]
    public void A_caller_without_admin_authority_is_refused_with_permission_denied()
    {
        var control = Substitute.For<ILatticeTreeAdmin>();
        control.ListViewProvidersAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("no admin"));

        var ex = Assert.ThrowsAsync<RpcException>(() =>
            CreateService(control).ListViewProviders(new TreeAdminTreeRequest { TreeId = "orders" }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void The_interceptor_names_the_rpc_and_the_source_tree()
    {
        var (operation, target) = LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
            "/" + LatticeTreeAdminGrpcMethods.ServiceName + "/" + LatticeTreeAdminGrpcMethods.ListViewProvidersMethodName,
            new TreeAdminTreeRequest { TreeId = "orders" });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeTreeAdminApiOperation.ListViewProviders));
            Assert.That(target, Is.EqualTo("orders"));
            Assert.That((int)operation, Is.GreaterThan((int)LatticeTreeAdminApiOperation.GetWalReclamation), "Appended after the shipped values.");
        });
    }

    [Test]
    public async Task The_client_sends_the_source_to_its_rpc()
    {
        var invoker = new UnaryResponseCallInvoker(Catalog());

        var catalog = await LatticeTreeAdminApiGrpcClient.Create(invoker, _serializerProvider).ListViewProvidersAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.ListViewProvidersMethodName));
            Assert.That(invoker.LastRequest, Is.EqualTo(new TreeAdminTreeRequest { TreeId = "orders" }));
            Assert.That(catalog.Providers.Single().ProviderKey, Is.EqualTo("by-status.v1"));
        });
    }

    [Test]
    public void The_client_refuses_a_missing_source()
    {
        var client = LatticeTreeAdminApiGrpcClient.Create(new UnaryResponseCallInvoker(Catalog()), _serializerProvider);

        Assert.That(async () => await client.ListViewProvidersAsync(""), Throws.ArgumentException);
    }

    [Test]
    public void BindService_binds_the_rpc_for_both_the_metadata_pass_and_a_concrete_service()
    {
        LatticeTreeAdminGrpcMethodsHolder.Current = _methods;
        var metadata = new RecordingServiceBinder();
        var bound = new RecordingServiceBinder();

        LatticeTreeAdminGrpcServiceBase.BindService(metadata, null);
        LatticeTreeAdminGrpcServiceBase.BindService(bound, CreateService(Substitute.For<ILatticeTreeAdmin>()));

        Assert.Multiple(() =>
        {
            Assert.That(metadata.MethodNames, Does.Contain(LatticeTreeAdminGrpcMethods.ListViewProvidersMethodName));
            Assert.That(bound.MethodNames, Does.Contain(LatticeTreeAdminGrpcMethods.ListViewProvidersMethodName));
        });
    }

    private sealed class RecordingServiceBinder : ServiceBinderBase
    {
        public List<string> MethodNames { get; } = [];

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler) =>
            MethodNames.Add(method.Name);

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            DuplexStreamingServerMethod<TRequest, TResponse>? handler) =>
            MethodNames.Add(method.Name);
    }
}
