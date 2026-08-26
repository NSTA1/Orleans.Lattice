using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Data;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Server-side unit coverage for <see cref="LatticeDataApiGrpcService"/> driven
/// directly over a substituted <see cref="ILatticeDataApi"/> facade and a stub
/// <see cref="ServerCallContext"/> - no gRPC server, no cluster. Exercises the
/// full typed-CRDT write and read dispatch (every op and kind, and the unknown
/// fall-through), and the exception-to-status mapping arms that turn a facade
/// failure into the right gRPC <see cref="StatusCode"/>.
/// </summary>
[TestFixture]
public sealed class LatticeDataApiGrpcServiceUnitTests
{
    private ILatticeDataApi _api = null!;
    private ILatticeDataApiCredentialBridge _bridge = null!;
    private ILatticeDataApiActiveTenantBridge _tenantBridge = null!;
    private LatticeDataApiGrpcService _service = null!;

    private static LatticeDataApiGrpcMethods Methods()
    {
        var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        return LatticeDataApiGrpcMethods.FromServiceProvider(provider);
    }

    private static StubServerCallContext Context(string method = "/orleans.lattice.api.data/Test") =>
        new(method);

    [SetUp]
    public void SetUp()
    {
        _api = Substitute.For<ILatticeDataApi>();
        _bridge = Substitute.For<ILatticeDataApiCredentialBridge>();
        _bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        _tenantBridge = Substitute.For<ILatticeDataApiActiveTenantBridge>();
        _tenantBridge.Resolve(Arg.Any<ServerCallContext>()).Returns((TenantId?)null);
        _service = new LatticeDataApiGrpcService(
            Methods(),
            _api,
            _bridge,
            _tenantBridge,
            NullLogger<LatticeDataApiGrpcService>.Instance);
    }

    private static CrdtWriteRequest Write(CrdtWriteOp op) =>
        new()
        {
            TreeId = "t",
            Key = "k",
            Op = op,
            ReplicaId = "r1",
            Amount = 1,
            Element = [1],
            Field = "f1",
            Index = 0,
        };

    [Test]
    public async Task CrdtWrite_dispatches_every_known_op_without_faulting()
    {
        foreach (var op in Enum.GetValues<CrdtWriteOp>())
        {
            var response = await _service.CrdtWrite(Write(op), Context());
            Assert.That(response, Is.Not.Null, $"op {op} should dispatch and return a response");
        }
    }

    [Test]
    public void CrdtWrite_unknown_op_maps_to_invalid_argument()
    {
        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.CrdtWrite(Write((CrdtWriteOp)999), Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public async Task CrdtRead_decodes_every_known_kind()
    {
        _api.CounterGetAsync("t", "k").Returns(5L);
        _api.GCounterGetAsync("t", "k").Returns(7L);
        _api.SetGetAsync("t", "k").Returns((IReadOnlyList<byte[]>)new List<byte[]> { new byte[] { 1 } });
        _api.GSetGetAsync("t", "k").Returns((IReadOnlyList<byte[]>)new List<byte[]> { new byte[] { 2 } });
        _api.RwSetGetAsync("t", "k").Returns((IReadOnlyList<byte[]>)new List<byte[]> { new byte[] { 3 } });
        _api.RegisterGetAsync("t", "k").Returns((IReadOnlyList<byte[]>)new List<byte[]> { new byte[] { 4 } });
        _api.SequenceGetAsync("t", "k").Returns((IReadOnlyList<byte[]>)new List<byte[]> { new byte[] { 5 } });
        _api.OrFlagGetAsync("t", "k").Returns(true);
        _api.RwFlagGetAsync("t", "k").Returns(false);
        _api.MaxRegisterGetAsync("t", "k").Returns((byte[]?)[9]);
        _api.MinRegisterGetAsync("t", "k").Returns((byte[]?)null);
        _api.VersionVectorGetAsync("t", "k")
            .Returns((IReadOnlyDictionary<string, string>)new Dictionary<string, string> { ["r1"] = "3" });
        _api.MapGetAsync("t", "k").Returns((IReadOnlyDictionary<string, IReadOnlyList<byte[]>>)
            new Dictionary<string, IReadOnlyList<byte[]>> { ["f1"] = new List<byte[]> { new byte[] { 6 } } });

        foreach (var kind in Enum.GetValues<CrdtKind>())
        {
            var response = await _service.CrdtRead(
                new CrdtReadRequest { TreeId = "t", Key = "k", Kind = kind },
                Context());
            Assert.That(response, Is.Not.Null, $"kind {kind} should decode a response");
        }
    }

    [Test]
    public async Task CrdtRead_max_register_yields_singleton_and_min_register_yields_empty()
    {
        _api.MaxRegisterGetAsync("t", "k").Returns((byte[]?)[9]);
        _api.MinRegisterGetAsync("t", "k").Returns((byte[]?)null);

        var max = await _service.CrdtRead(
            new CrdtReadRequest { TreeId = "t", Key = "k", Kind = CrdtKind.MaxRegister }, Context());
        var min = await _service.CrdtRead(
            new CrdtReadRequest { TreeId = "t", Key = "k", Kind = CrdtKind.MinRegister }, Context());

        Assert.Multiple(() =>
        {
            Assert.That(max.Elements, Has.Count.EqualTo(1));
            Assert.That(min.Elements, Is.Empty);
        });
    }

    [Test]
    public void CrdtRead_unknown_kind_maps_to_invalid_argument()
    {
        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.CrdtRead(
                new CrdtReadRequest { TreeId = "t", Key = "k", Kind = (CrdtKind)999 },
                Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void Set_rethrows_an_rpc_exception_unchanged()
    {
        var original = new RpcException(new Status(StatusCode.NotFound, "already an rpc fault"));
        _api.SetAsync("t", "k", Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(original));

        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.Set(new DataSetRequest { TreeId = "t", Key = "k", Value = [1] }, Context()));

        Assert.That(ex, Is.SameAs(original));
    }

    [Test]
    public void Set_maps_operation_cancelled_to_cancelled()
    {
        _api.SetAsync("t", "k", Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new OperationCanceledException()));

        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.Set(new DataSetRequest { TreeId = "t", Key = "k", Value = [1] }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
    }

    [Test]
    public void Set_maps_argument_exception_to_invalid_argument()
    {
        _api.SetAsync("t", "k", Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new ArgumentException("bad arg")));

        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.Set(new DataSetRequest { TreeId = "t", Key = "k", Value = [1] }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void Set_maps_saturation_to_resource_exhausted()
    {
        _api.SetAsync("t", "k", Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new LatticeSaturatedException()));

        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.Set(new DataSetRequest { TreeId = "t", Key = "k", Value = [1] }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
    }

    [Test]
    public void Set_maps_an_unexpected_fault_to_internal()
    {
        _api.SetAsync("t", "k", Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("boom")));

        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.Set(new DataSetRequest { TreeId = "t", Key = "k", Value = [1] }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Internal));
    }

    [Test]
    public void Set_throws_on_null_request()
    {
        Assert.ThrowsAsync<ArgumentNullException>(() => _service.Set(null!, Context()));
    }

    [Test]
    public void Set_maps_tenant_access_denied_to_permission_denied()
    {
        _api.SetAsync("t", "k", Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new LatticeTenantAccessDeniedException(
                "Tenant 'acme' is not admitted to write to tree 't'.")));

        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.Set(new DataSetRequest { TreeId = "t", Key = "k", Value = [1] }, Context()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex.Status.Detail, Does.Contain("acme"));
        });
    }

    [Test]
    public async Task Set_stamps_the_bridged_active_tenant_around_the_facade_call()
    {
        _tenantBridge.Resolve(Arg.Any<ServerCallContext>()).Returns(TenantId.Parse("acme"));

        TenantId? observed = null;
        _api.SetAsync("t", "k", Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                observed = LatticeActiveTenantContext.Current;
                return Task.CompletedTask;
            });

        await _service.Set(new DataSetRequest { TreeId = "t", Key = "k", Value = [1] }, Context());

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.EqualTo(TenantId.Parse("acme")),
                "the facade must observe the caller's asserted active tenant on the ambient scope");
            Assert.That(LatticeActiveTenantContext.Current, Is.Null,
                "the active-tenant scope must be restored once the call completes");
        });
    }

    [Test]
    public async Task Set_leaves_no_active_tenant_when_the_bridge_asserts_none()
    {
        // Cold path: the bridge default returns null, so the facade sees no active
        // tenant (the resolver then applies its own fail-closed membership rules).
        TenantId? observed = TenantId.Parse("stale");
        _api.SetAsync("t", "k", Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                observed = LatticeActiveTenantContext.Current;
                return Task.CompletedTask;
            });

        await _service.Set(new DataSetRequest { TreeId = "t", Key = "k", Value = [1] }, Context());

        Assert.That(observed, Is.Null);
    }

    [Test]
    public void Constructor_validates_its_dependencies()
    {
        var methods = Methods();
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => new LatticeDataApiGrpcService(null!, _api, _bridge, _tenantBridge, NullLogger<LatticeDataApiGrpcService>.Instance));
            Assert.Throws<ArgumentNullException>(
                () => new LatticeDataApiGrpcService(methods, null!, _bridge, _tenantBridge, NullLogger<LatticeDataApiGrpcService>.Instance));
            Assert.Throws<ArgumentNullException>(
                () => new LatticeDataApiGrpcService(methods, _api, null!, _tenantBridge, NullLogger<LatticeDataApiGrpcService>.Instance));
            Assert.Throws<ArgumentNullException>(
                () => new LatticeDataApiGrpcService(methods, _api, _bridge, null!, NullLogger<LatticeDataApiGrpcService>.Instance));
            Assert.Throws<ArgumentNullException>(
                () => new LatticeDataApiGrpcService(methods, _api, _bridge, _tenantBridge, null!));
        });
    }
}
