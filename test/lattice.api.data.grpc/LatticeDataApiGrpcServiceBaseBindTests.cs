using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Data;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Unit coverage for the gRPC binding hook on
/// <see cref="LatticeDataApiGrpcServiceBase"/>. Proves the metadata-recording pass
/// (null service impl) and the per-request-binding pass (concrete impl) both add
/// all ten unary methods, that a null binder is rejected, and that the hook fails
/// loudly when the method-definition holder was not initialised first.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class LatticeDataApiGrpcServiceBaseBindTests
{
    private const int ExpectedMethodCount = 10;

    private LatticeDataApiGrpcMethods? _savedHolder;

    private static LatticeDataApiGrpcMethods Methods()
    {
        var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        return LatticeDataApiGrpcMethods.FromServiceProvider(provider);
    }

    private static LatticeDataApiGrpcService NewServiceImpl()
    {
        var api = Substitute.For<ILatticeDataApi>();
        var bridge = Substitute.For<ILatticeDataApiCredentialBridge>();
        var tenantBridge = Substitute.For<ILatticeDataApiActiveTenantBridge>();
        return new LatticeDataApiGrpcService(
            Methods(),
            api,
            bridge,
            tenantBridge,
            NullLogger<LatticeDataApiGrpcService>.Instance);
    }

    [SetUp]
    public void SetUp() => _savedHolder = LatticeDataApiGrpcMethodsHolder.Current;

    [TearDown]
    public void TearDown() => LatticeDataApiGrpcMethodsHolder.Current = _savedHolder;

    [Test]
    public void BindService_records_all_methods_for_a_null_service_impl()
    {
        LatticeDataApiGrpcMethodsHolder.Current = Methods();
        var binder = new CountingServiceBinder();

        LatticeDataApiGrpcServiceBase.BindService(binder, null);

        Assert.That(binder.AddedMethods, Is.EqualTo(ExpectedMethodCount));
    }

    [Test]
    public void BindService_binds_all_methods_for_a_concrete_service_impl()
    {
        LatticeDataApiGrpcMethodsHolder.Current = Methods();
        var binder = new CountingServiceBinder();

        LatticeDataApiGrpcServiceBase.BindService(binder, NewServiceImpl());

        Assert.That(binder.AddedMethods, Is.EqualTo(ExpectedMethodCount));
    }

    [Test]
    public void BindService_throws_on_null_binder()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeDataApiGrpcServiceBase.BindService(null!, null));
    }

    [Test]
    public void BindService_throws_when_the_methods_holder_is_uninitialised()
    {
        LatticeDataApiGrpcMethodsHolder.Current = null;
        var binder = new CountingServiceBinder();

        Assert.Throws<InvalidOperationException>(
            () => LatticeDataApiGrpcServiceBase.BindService(binder, null));
    }

    /// <summary>
    /// A <see cref="ServiceBinderBase"/> that counts the unary methods bound onto
    /// it, so a test can assert the full RPC surface was registered without a live
    /// gRPC server.
    /// </summary>
    private sealed class CountingServiceBinder : ServiceBinderBase
    {
        public int AddedMethods { get; private set; }

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler)
            => AddedMethods++;
    }
}
