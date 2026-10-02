using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the <see cref="ILatticeSchemaOperations"/> half of
/// <see cref="GrpcLatticeSchemaControl"/> (#4209): in remote mode the schema
/// remediation and migration MCP tools reach the cluster through this adapter, so each
/// start verb must forward its tree, arguments and idempotency id to the matching
/// accept-then-poll RPC, and the status, list and cancel verbs must reach the schema
/// operation RPCs rather than the compliance-scan ones. Deterministic over a
/// <see cref="FakeCallInvoker"/> - no channel, no cluster.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeSchemaControlOperationsTests
{
    private static ILatticeSchemaOperations Adapter(FakeCallInvoker invoker)
        => new GrpcLatticeSchemaControl(RemoteTestSupport.SchemaClient(invoker));

    private static LatticeOperationHandle Handle(string kind) => new()
    {
        OperationId = "op-1",
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        Created = true,
    };

    private static LatticeOperationStatus Status() => new()
    {
        OperationId = "op-1",
        Kind = SchemaOperationKinds.Migration,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        State = LatticeOperationState.Running,
        Phase = SchemaOperationPhases.DryRun,
    };

    [Test]
    public async Task StartRemediationAsync_forwards_the_tree_transform_policy_and_id()
    {
        var transform = LatticeValueTransform.Passthrough();
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });
        var invoker = new FakeCallInvoker(_ => Handle(SchemaOperationKinds.Remediation));

        var handle = await Adapter(invoker).StartRemediationAsync("orders", transform, policy, "rem-1");

        var sent = (RemediateRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastMethod, Does.EndWith("/StartRemediation"));
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Transform, Is.EqualTo(transform));
            Assert.That(sent.TargetPolicy, Is.SameAs(policy));
            Assert.That(sent.OperationId, Is.EqualTo("rem-1"));
            Assert.That(handle.Kind, Is.EqualTo(SchemaOperationKinds.Remediation));
        });
    }

    [Test]
    public async Task StartMigrationAsync_forwards_the_tree_and_id()
    {
        var invoker = new FakeCallInvoker(_ => Handle(SchemaOperationKinds.Migration));

        var handle = await Adapter(invoker).StartMigrationAsync("orders", "mig-1");

        var sent = (SchemaMigrationStartRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastMethod, Does.EndWith("/StartMigration"));
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.OperationId, Is.EqualTo("mig-1"));
            Assert.That(handle.OperationId, Is.EqualTo("op-1"));
        });
    }

    [Test]
    public async Task StartAdvanceAndMigrateAsync_forwards_the_new_target_and_a_null_id()
    {
        var invoker = new FakeCallInvoker(_ => Handle(SchemaOperationKinds.AdvanceAndMigrate));

        await Adapter(invoker).StartAdvanceAndMigrateAsync("orders", 5);

        var sent = (AdvanceVersionRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastMethod, Does.EndWith("/StartAdvanceAndMigrate"));
            Assert.That(sent.NewTargetVersion, Is.EqualTo(5u));
            Assert.That(sent.OperationId, Is.Null);
        });
    }

    [Test]
    public async Task GetOperationStatusAsync_reaches_the_schema_operation_rpc_and_unwraps_the_status()
    {
        var invoker = new FakeCallInvoker(_ => new SchemaOperationStatusResponse { Status = Status() });

        var status = await Adapter(invoker).GetOperationStatusAsync("op-1");

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastMethod, Does.EndWith("/GetSchemaOperationStatus"));
            Assert.That(((SchemaOperationRequest)invoker.LastRequest!).OperationId, Is.EqualTo("op-1"));
            Assert.That(status!.Kind, Is.EqualTo(SchemaOperationKinds.Migration));
        });
    }

    [Test]
    public async Task GetOperationStatusAsync_maps_an_invisible_operation_to_null()
    {
        var status = await Adapter(new FakeCallInvoker(_ => new SchemaOperationStatusResponse())).GetOperationStatusAsync("op-1");

        Assert.That(status, Is.Null);
    }

    [Test]
    public async Task ListOperationsAsync_forwards_the_page_request()
    {
        var request = new LatticeOperationListRequest { PageSize = 5, PageToken = "next" };
        var invoker = new FakeCallInvoker(_ => new LatticeOperationPage { Operations = [Status()], NextPageToken = "after" });

        var page = await Adapter(invoker).ListOperationsAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastMethod, Does.EndWith("/ListSchemaOperations"));
            Assert.That(invoker.LastRequest, Is.SameAs(request));
            Assert.That(page.NextPageToken, Is.EqualTo("after"));
        });
    }

    [Test]
    public async Task CancelOperationAsync_reaches_the_schema_cancel_rpc()
    {
        var invoker = new FakeCallInvoker(_ => new SchemaOperationStatusResponse { Status = Status() with { CancelRequested = true } });

        var status = await Adapter(invoker).CancelOperationAsync("op-1");

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastMethod, Does.EndWith("/CancelSchemaOperation"));
            Assert.That(status!.CancelRequested, Is.True);
        });
    }
}
