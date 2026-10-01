using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Testing;
using LatticeOperationState = Orleans.Lattice.Api.Operations.LatticeOperationState;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaComplianceOperations"/> (#4126), the
/// accept-then-poll compliance scan, driven on a real
/// <see cref="LatticeOperationRunner"/> over in-memory operation grains: a start
/// authorizes read and returns at once, the scan polls to a report rebuilt from the
/// result map, a retried start is idempotent, and every status read, listing and
/// cancel is scoped fail-closed to the compliance kind and the trees the caller may
/// read.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaComplianceOperationsTests
{
    private const string Tree = "orders";
    private const string Tenant = "default";

    private static LatticeSchemaComplianceReport Report(string treeId = Tree) => new()
    {
        TreeId = treeId,
        HasPolicy = true,
        CompliantCount = 4,
        NonCompliantCount = 1,
        ScannedCount = 5,
        RuleBreakdown = [new LatticeSchemaComplianceRuleCount { Reason = "must be json", Count = 1 }],
    };

    private sealed class Harness
    {
        public required InMemoryOperationRunner Operations { get; init; }

        public required ILatticeSchemaComplianceAdmin Compliance { get; init; }

        public required LatticeSchemaComplianceOperations Facade { get; init; }

        public LatticeSchemaComplianceOperations With(ILatticeAccessGate gate) =>
            new(Operations.Runner, Compliance, new SchemaAccessAuthorizer(gate), new DefaultTenantContextResolver());
    }

    private static Harness Create(ILatticeAccessGate? gate = null)
    {
        var operations = new InMemoryOperationRunner();
        var compliance = Substitute.For<ILatticeSchemaComplianceAdmin>();
        compliance.ScanComplianceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Report(call.ArgAt<string>(0)));
        return new Harness
        {
            Operations = operations,
            Compliance = compliance,
            Facade = new LatticeSchemaComplianceOperations(
                operations.Runner,
                compliance,
                new SchemaAccessAuthorizer(gate ?? RecordingAccessGate.Allow()),
                new DefaultTenantContextResolver()),
        };
    }

    private static async Task<LatticeOperationStatus> UntilTerminalAsync(ILatticeOperations operations, string operationId)
    {
        LatticeOperationStatus? status = null;
        await TestPoll.UntilAsync(
            async () => (status = await operations.GetOperationStatusAsync(operationId)) is { IsTerminal: true },
            $"operation {operationId} to finish");
        return status!;
    }

    [Test]
    public async Task A_started_scan_returns_a_handle_and_polls_to_the_report()
    {
        var h = Create();

        var handle = await h.Facade.StartComplianceScanAsync(Tree, "scan-1");
        var status = await UntilTerminalAsync(h.Facade, handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("scan-1"));
            Assert.That(handle.Kind, Is.EqualTo(SchemaComplianceScanOperation.Kind));
            Assert.That(handle.Created, Is.True);
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { Tree }));
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(status.PhaseCount, Is.EqualTo(2));
            Assert.That(status.ResultReference, Is.EqualTo(Tree));
            Assert.That(SchemaComplianceScanResults.TryReadReport(status.Result, out var report), Is.True);
            Assert.That(report.NonCompliantCount, Is.EqualTo(1));
            Assert.That(report.RuleBreakdown[0].Reason, Is.EqualTo("must be json"));
        });
    }

    [Test]
    public async Task A_start_authorizes_read_over_the_tree_and_a_denied_caller_starts_nothing()
    {
        var gate = RecordingAccessGate.Allow();
        var h = Create(gate);

        await h.Facade.StartComplianceScanAsync(Tree, "scan-read");
        var denied = h.With(RecordingAccessGate.Deny());

        Assert.Multiple(() =>
        {
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Read));
            Assert.That(gate.Last.TreeId, Is.EqualTo(Tree));
            Assert.That(
                async () => await denied.StartComplianceScanAsync(Tree, "scan-denied"),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(h.Operations.Record(Tenant, "scan-denied"), Is.Null, "A denied start records nothing.");
        });
    }

    [Test]
    public async Task A_retried_start_with_the_same_id_starts_nothing()
    {
        var h = Create();

        var first = await h.Facade.StartComplianceScanAsync(Tree, "scan-twice");
        await UntilTerminalAsync(h.Facade, first.OperationId);
        var second = await h.Facade.StartComplianceScanAsync(Tree, "scan-twice");

        Assert.Multiple(() =>
        {
            Assert.That(first.Created, Is.True);
            Assert.That(second.Created, Is.False);
        });
        await h.Compliance.Received(1).ScanComplianceAsync(Tree, Arg.Any<CancellationToken>());
    }

    [TestCase("has space")]
    [TestCase("slash/id")]
    public void A_malformed_operation_id_is_refused_before_anything_starts(string operationId)
    {
        var h = Create();

        Assert.That(async () => await h.Facade.StartComplianceScanAsync(Tree, operationId), Throws.ArgumentException);
    }

    [Test]
    public void Null_or_empty_arguments_are_refused()
    {
        var h = Create();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await h.Facade.StartComplianceScanAsync(""), Throws.ArgumentException);
            Assert.That(async () => await h.Facade.GetOperationStatusAsync(""), Throws.ArgumentException);
            Assert.That(async () => await h.Facade.CancelOperationAsync(""), Throws.ArgumentException);
            Assert.That(async () => await h.Facade.ListOperationsAsync(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task A_failed_scan_reads_as_failed_with_the_engine_reason()
    {
        var h = Create();
        h.Compliance.ScanComplianceAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromException<LatticeSchemaComplianceReport>(new InvalidOperationException("scan broke")));

        var handle = await h.Facade.StartComplianceScanAsync(Tree);
        var status = await UntilTerminalAsync(h.Facade, handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(status.FailureReason, Does.Contain("scan broke"));
        });
    }

    [Test]
    public async Task An_operation_over_a_tree_the_caller_cannot_read_is_not_found_rather_than_forbidden()
    {
        var h = Create();
        var handle = await h.Facade.StartComplianceScanAsync(Tree, "scan-hidden");
        await UntilTerminalAsync(h.Facade, handle.OperationId);
        var stranger = h.With(RecordingAccessGate.Deny());

        Assert.Multiple(async () =>
        {
            Assert.That(await stranger.GetOperationStatusAsync("scan-hidden"), Is.Null);
            Assert.That(await stranger.CancelOperationAsync("scan-hidden"), Is.Null);
            Assert.That((await stranger.ListOperationsAsync(new LatticeOperationListRequest())).Operations, Is.Empty);
        });
    }

    [Test]
    public async Task Another_kind_of_operation_is_neither_read_nor_listed()
    {
        var h = Create();
        await h.Facade.StartComplianceScanAsync(Tree, "scan-mine");
        var other = await h.Operations.Runner.StartAsync(
            new LatticeOperationStart { TenantId = Tenant, OperationId = "remediate-1", Kind = "schema.remediate", TreeIds = [Tree] },
            static (_, _) => Task.FromResult(0),
            static _ => LatticeOperationCompletion.Succeeded());
        await other.Completion!;

        var page = await h.Facade.ListOperationsAsync(new LatticeOperationListRequest());

        Assert.Multiple(async () =>
        {
            Assert.That(await h.Facade.GetOperationStatusAsync("remediate-1"), Is.Null);
            Assert.That(await h.Facade.CancelOperationAsync("remediate-1"), Is.Null);
            Assert.That(page.Operations.Select(o => o.OperationId), Is.EqualTo(new[] { "scan-mine" }));
        });
    }

    [Test]
    public async Task Cancelling_a_running_scan_stops_it_and_reads_as_cancelled()
    {
        var h = Create();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Compliance.ScanComplianceAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, call.ArgAt<CancellationToken>(1));
                return Report();
            });

        var handle = await h.Facade.StartComplianceScanAsync(Tree, "scan-cancel");
        await started.Task;
        var afterCancel = await h.Facade.CancelOperationAsync(handle.OperationId);
        var status = await UntilTerminalAsync(h.Facade, handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(afterCancel!.CancelRequested, Is.True);
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Cancelled));
        });
    }

    [Test]
    public void Constructor_null_dependencies_throw()
    {
        var h = Create();
        var authorizer = new SchemaAccessAuthorizer(RecordingAccessGate.Allow());
        var tenants = new DefaultTenantContextResolver();

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeSchemaComplianceOperations(null!, h.Compliance, authorizer, tenants), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaComplianceOperations(h.Operations.Runner, null!, authorizer, tenants), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaComplianceOperations(h.Operations.Runner, h.Compliance, null!, tenants), Throws.ArgumentNullException);
            Assert.That(() => new LatticeSchemaComplianceOperations(h.Operations.Runner, h.Compliance, authorizer, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void AddLatticeSchemaApi_registers_the_compliance_operations_once()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        services.AddSingleton(Substitute.For<ILatticeSchemaAdmin>());

        builder.AddLatticeSchemaApi();
        builder.AddLatticeSchemaApi();

        var registrations = services.Where(d => d.ServiceType == typeof(ILatticeSchemaComplianceOperations)).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(registrations, Has.Count.EqualTo(1));
            Assert.That(registrations[0].ImplementationType, Is.EqualTo(typeof(LatticeSchemaComplianceOperations)));
            Assert.That(registrations[0].Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
        });
    }
}
