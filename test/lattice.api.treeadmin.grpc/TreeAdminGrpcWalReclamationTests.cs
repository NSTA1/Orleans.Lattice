using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// The read-only <c>GetWalReclamation</c> RPC (#4195) of the tree-admin gRPC binding,
/// without a cluster: it adapts onto <see cref="ILatticeWalReclamation"/>, a host
/// without that facade answers <see cref="StatusCode.Unimplemented"/>, the
/// interceptor names it with its own appended operation and the tree it reads, the
/// report round-trips with its derived wedge verdict intact, and the client sends
/// the tree to the right RPC.
/// </summary>
[TestFixture]
public sealed class TreeAdminGrpcWalReclamationTests
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

    private LatticeTreeAdminGrpcService CreateService(ILatticeWalReclamation? reclamation)
    {
        var bridge = Substitute.For<ILatticeTreeAdminApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        return new LatticeTreeAdminGrpcService(
            _methods,
            Substitute.For<ILatticeTreeAdmin>(),
            bridge,
            Substitute.For<ILatticeTreeAdminApiAuthSchemeSource>(),
            Options.Create(new LatticeTreeAdminApiGrpcOptions()),
            NullLogger<LatticeTreeAdminGrpcService>.Instance,
            walReclamation: reclamation);
    }

    private static FakeServerCallContext Context() =>
        new("/" + LatticeTreeAdminGrpcMethods.ServiceName + "/" + LatticeTreeAdminGrpcMethods.GetWalReclamationMethodName);

    private static TreeWalReclamationReport Wedged() => new()
    {
        TreeId = "orders",
        PinStoreReadable = true,
        PinCount = 3,
        PinsWithoutOffset = 1,
        FloorHolder = new TreeWalFloorHolder
        {
            ConsumerId = "consumer",
            LeafId = "bplusleaf/abc",
            Partition = 1,
            PinOffset = 42,
            PersistedCheckpoint = -1,
            State = TreeWalFloorHolderState.NeverCheckpointed,
        },
    };

    [Test]
    public async Task The_rpc_adapts_onto_the_wal_reclamation_facade()
    {
        var reclamation = Substitute.For<ILatticeWalReclamation>();
        var report = Wedged();
        reclamation.GetWalReclamationAsync("orders", Arg.Any<CancellationToken>()).Returns(report);

        var answered = await CreateService(reclamation).GetWalReclamation(new TreeAdminTreeRequest { TreeId = "orders" }, Context());

        Assert.That(answered, Is.SameAs(report));
    }

    [Test]
    public void A_host_without_the_facade_answers_unimplemented()
    {
        var ex = Assert.ThrowsAsync<RpcException>(() =>
            CreateService(null).GetWalReclamation(new TreeAdminTreeRequest { TreeId = "orders" }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void A_caller_without_read_authority_is_refused_with_permission_denied()
    {
        var reclamation = Substitute.For<ILatticeWalReclamation>();
        reclamation.GetWalReclamationAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("no read"));

        var ex = Assert.ThrowsAsync<RpcException>(() =>
            CreateService(reclamation).GetWalReclamation(new TreeAdminTreeRequest { TreeId = "orders" }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void The_interceptor_names_the_rpc_and_the_tree_it_reads()
    {
        var (operation, target) = LatticeTreeAdminApiGrpcAuthInterceptor.DescribeCall(
            "/" + LatticeTreeAdminGrpcMethods.ServiceName + "/" + LatticeTreeAdminGrpcMethods.GetWalReclamationMethodName,
            new TreeAdminTreeRequest { TreeId = "orders" });

        Assert.Multiple(() =>
        {
            Assert.That(operation, Is.EqualTo(LatticeTreeAdminApiOperation.GetWalReclamation));
            Assert.That(target, Is.EqualTo("orders"));
            Assert.That((int)operation, Is.GreaterThan((int)LatticeTreeAdminApiOperation.CancelStorageUsageRefresh), "Appended after the shipped values.");
        });
    }

    [Test]
    public void The_report_round_trips_with_its_wedge_verdict()
    {
        var serializer = _serializerProvider.GetRequiredService<Serializer>();
        var report = Wedged();

        var copy = serializer.Deserialize<TreeWalReclamationReport>(serializer.SerializeToArray(report));

        Assert.Multiple(() =>
        {
            Assert.That(copy, Is.EqualTo(report));
            Assert.That(copy.IsWedged, Is.True);
            Assert.That(copy.FloorHolder!.HoldsOffsetFloor, Is.True);
        });
    }

    [Test]
    public async Task The_client_sends_the_tree_to_its_rpc()
    {
        var invoker = new UnaryResponseCallInvoker(Wedged());

        var report = await LatticeTreeAdminApiGrpcClient.Create(invoker, _serializerProvider).GetWalReclamationAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeTreeAdminGrpcMethods.GetWalReclamationMethodName));
            Assert.That(invoker.LastRequest, Is.EqualTo(new TreeAdminTreeRequest { TreeId = "orders" }));
            Assert.That(report.IsWedged, Is.True);
        });
    }

    [Test]
    public void The_client_refuses_a_missing_tree()
    {
        var client = LatticeTreeAdminApiGrpcClient.Create(new UnaryResponseCallInvoker(Wedged()), _serializerProvider);

        Assert.That(async () => await client.GetWalReclamationAsync(""), Throws.ArgumentException);
    }

    [Test]
    public void BindService_binds_the_rpc_for_both_the_metadata_pass_and_a_concrete_service()
    {
        LatticeTreeAdminGrpcMethodsHolder.Current = _methods;
        var metadata = new RecordingServiceBinder();
        var bound = new RecordingServiceBinder();

        LatticeTreeAdminGrpcServiceBase.BindService(metadata, null);
        LatticeTreeAdminGrpcServiceBase.BindService(bound, CreateService(Substitute.For<ILatticeWalReclamation>()));

        Assert.Multiple(() =>
        {
            Assert.That(metadata.MethodNames, Does.Contain(LatticeTreeAdminGrpcMethods.GetWalReclamationMethodName));
            Assert.That(bound.MethodNames, Does.Contain(LatticeTreeAdminGrpcMethods.GetWalReclamationMethodName));
            Assert.That(bound.MethodNames, Is.EquivalentTo(metadata.MethodNames), "both passes bind the same surface");
        });
    }

    private sealed class RecordingServiceBinder : ServiceBinderBase
    {
        public List<string> MethodNames { get; } = [];

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler) =>
            MethodNames.Add(method.Name);
    }
}
