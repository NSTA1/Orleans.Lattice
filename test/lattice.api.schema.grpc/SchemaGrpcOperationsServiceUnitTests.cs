using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// Unit tests for the accept-then-poll RPCs of <see cref="LatticeSchemaGrpcService"/>
/// (#4123): each forwards to the hosted facade's <see cref="ILatticeSchemaOperations"/>
/// surface and wraps the result, and a hosted control without that surface
/// answers <see cref="StatusCode.Unimplemented"/>. No cluster or server.
/// </summary>
[TestFixture]
public sealed class SchemaGrpcOperationsServiceUnitTests
{
    private const string Tree = "orders";

    private ServiceProvider _serializerProvider = null!;
    private LatticeSchemaGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeSchemaGrpcMethods.FromServiceProvider(_serializerProvider);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _serializerProvider.Dispose();

    private LatticeSchemaGrpcService CreateService(ILatticeSchemaControl control)
    {
        var bridge = Substitute.For<ILatticeSchemaApiCredentialBridge>();
        bridge.Resolve(Arg.Any<ServerCallContext>()).Returns((LatticeCredential?)null);
        return new LatticeSchemaGrpcService(
            _methods,
            control,
            bridge,
            Substitute.For<ILatticeSchemaApiAuthSchemeSource>(),
            Options.Create(new LatticeSchemaApiGrpcOptions()),
            NullLogger<LatticeSchemaGrpcService>.Instance);
    }

    private static FakeServerCallContext Context(string methodName) =>
        new(SchemaGrpcTestDoubles.FullMethod(methodName));

    private static LatticeOperationHandle Handle(string kind) => new()
    {
        OperationId = "op-1",
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = [Tree] },
        Created = true,
    };

    private static LatticeOperationStatus Status() => new()
    {
        OperationId = "op-1",
        Kind = SchemaOperationKinds.Remediation,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = [Tree] },
        State = LatticeOperationState.Running,
        Phase = SchemaOperationPhases.Build,
        CompletedUnits = 2,
        TotalUnits = 5,
        UnitName = SchemaOperationPhases.ValuesUnit,
    };

    private static ILatticeSchemaControl OperationsControl(out ILatticeSchemaOperations operations)
    {
        var control = Substitute.For<ILatticeSchemaControl, ILatticeSchemaOperations>();
        operations = (ILatticeSchemaOperations)control;
        return control;
    }

    [Test]
    public async Task StartRemediation_forwards_the_tree_transform_policy_and_operation_id()
    {
        var control = OperationsControl(out var operations);
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });
        var handle = Handle(SchemaOperationKinds.Remediation);
        operations.StartRemediationAsync(Tree, Arg.Any<LatticeValueTransform>(), policy, "op-1", Arg.Any<CancellationToken>())
            .Returns(handle);

        var result = await CreateService(control).StartRemediation(
            new RemediateRequest { TreeId = Tree, Transform = LatticeValueTransform.Passthrough(), TargetPolicy = policy, OperationId = "op-1" },
            Context(LatticeSchemaGrpcMethods.StartRemediationMethodName));

        Assert.That(result, Is.SameAs(handle));
    }

    [Test]
    public async Task StartMigration_and_StartAdvanceAndMigrate_forward_to_the_operations_surface()
    {
        var control = OperationsControl(out var operations);
        var migration = Handle(SchemaOperationKinds.Migration);
        var advance = Handle(SchemaOperationKinds.AdvanceAndMigrate);
        operations.StartMigrationAsync(Tree, "m-1", Arg.Any<CancellationToken>()).Returns(migration);
        operations.StartAdvanceAndMigrateAsync(Tree, 4, "a-1", Arg.Any<CancellationToken>()).Returns(advance);
        var service = CreateService(control);

        var started = await service.StartMigration(
            new SchemaMigrationStartRequest { TreeId = Tree, OperationId = "m-1" },
            Context(LatticeSchemaGrpcMethods.StartMigrationMethodName));
        var advanced = await service.StartAdvanceAndMigrate(
            new AdvanceVersionRequest { TreeId = Tree, NewTargetVersion = 4, OperationId = "a-1" },
            Context(LatticeSchemaGrpcMethods.StartAdvanceAndMigrateMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(started, Is.SameAs(migration));
            Assert.That(advanced, Is.SameAs(advance));
        });
    }

    [Test]
    public async Task Status_list_and_cancel_wrap_the_facade_result()
    {
        var control = OperationsControl(out var operations);
        var status = Status();
        var page = new LatticeOperationPage { Operations = [status], NextPageToken = null };
        operations.GetOperationStatusAsync("op-1", Arg.Any<CancellationToken>()).Returns(status);
        operations.CancelOperationAsync("op-1", Arg.Any<CancellationToken>()).Returns(status with { CancelRequested = true });
        operations.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), Arg.Any<CancellationToken>()).Returns(page);
        var service = CreateService(control);

        var read = await service.GetSchemaOperationStatus(
            new SchemaOperationRequest { OperationId = "op-1" },
            Context(LatticeSchemaGrpcMethods.GetSchemaOperationStatusMethodName));
        var cancelled = await service.CancelSchemaOperation(
            new SchemaOperationRequest { OperationId = "op-1" },
            Context(LatticeSchemaGrpcMethods.CancelSchemaOperationMethodName));
        var listed = await service.ListSchemaOperations(
            new LatticeOperationListRequest { PageSize = 10 },
            Context(LatticeSchemaGrpcMethods.ListSchemaOperationsMethodName));

        Assert.Multiple(() =>
        {
            Assert.That(read.Status, Is.EqualTo(status));
            Assert.That(cancelled.Status!.CancelRequested, Is.True);
            Assert.That(listed, Is.SameAs(page));
        });
    }

    [Test]
    public async Task A_status_read_of_an_invisible_operation_answers_with_no_status()
    {
        var control = OperationsControl(out var operations);
        operations.GetOperationStatusAsync("nope", Arg.Any<CancellationToken>()).Returns((LatticeOperationStatus?)null);

        var read = await CreateService(control).GetSchemaOperationStatus(
            new SchemaOperationRequest { OperationId = "nope" },
            Context(LatticeSchemaGrpcMethods.GetSchemaOperationStatusMethodName));

        Assert.That(read.Status, Is.Null);
    }

    [Test]
    public void A_hosted_control_without_the_operations_surface_answers_unimplemented()
    {
        var service = CreateService(Substitute.For<ILatticeSchemaControl>());

        var ex = Assert.ThrowsAsync<RpcException>(() => service.GetSchemaOperationStatus(
            new SchemaOperationRequest { OperationId = "op-1" },
            Context(LatticeSchemaGrpcMethods.GetSchemaOperationStatusMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void A_start_the_facade_refuses_maps_onto_the_matching_status_code()
    {
        var control = OperationsControl(out var operations);
        operations.StartMigrationAsync(Tree, null, Arg.Any<CancellationToken>())
            .Returns<LatticeOperationHandle>(_ => throw new LatticeAuthorizationDeniedException("denied"));

        var ex = Assert.ThrowsAsync<RpcException>(() => CreateService(control).StartMigration(
            new SchemaMigrationStartRequest { TreeId = Tree },
            Context(LatticeSchemaGrpcMethods.StartMigrationMethodName)));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }
}
