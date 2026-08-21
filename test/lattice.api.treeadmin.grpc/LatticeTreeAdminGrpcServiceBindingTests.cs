using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

[TestFixture]
public sealed class LatticeTreeAdminGrpcServiceBindingTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void BindService_withConcreteImplementation_registersCreateViewHandler()
    {
        var methods = LatticeTreeAdminGrpcMethods.FromServiceProvider(_services);
        LatticeTreeAdminGrpcMethodsHolder.Current = methods;
        var service = new LatticeTreeAdminGrpcService(
            methods,
            Substitute.For<ILatticeTreeAdmin>(),
            Substitute.For<ILatticeTreeAdminApiCredentialBridge>(),
            Substitute.For<ILatticeTreeAdminApiAuthSchemeSource>(),
            NullLogger<LatticeTreeAdminGrpcService>.Instance);
        var binder = new RecordingServiceBinder();

        LatticeTreeAdminGrpcServiceBase.BindService(binder, service);

        Assert.That(
            binder.MethodNames,
            Does.Contain(LatticeTreeAdminGrpcMethods.CreateViewMethodName));
    }

    private sealed class RecordingServiceBinder : ServiceBinderBase
    {
        public List<string> MethodNames { get; } = [];

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler)
        {
            Assert.That(handler, Is.Not.Null);
            MethodNames.Add(method.Name);
        }
    }
}
