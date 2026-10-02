using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the accept-then-poll schema RPCs (#4123) over a live,
/// co-hosted gRPC server bound to a real cluster's facade: a remediation started
/// over the wire returns its handle and polls to its outcome, appears in the
/// listing, and an unknown operation reads and cancels as not found.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeSchemaGrpcOperationsE2ETests
{
    private GrpcSchemaClusterFixture _fixture = null!;
    private GrpcSchemaHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcSchemaClusterFixture();
        await _fixture.InitializeAsync();
        _host = await _fixture.CreateGrpcHostAsync(new AllowAllSchemaApiAuthorizer());
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    private async Task<LatticeOperationStatus> UntilTerminalAsync(string operationId)
    {
        LatticeOperationStatus? status = null;
        await TestPoll.UntilAsync(
            async () => (status = await _host.Client.GetSchemaOperationStatusAsync(operationId)) is { IsTerminal: true },
            $"operation {operationId} to finish");
        return status!;
    }

    [Test]
    public async Task A_remediation_started_over_the_wire_polls_to_its_outcome_and_is_listed()
    {
        const string treeId = "grpc-ops-remediate";
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("k1", "{\"v\":1}"u8.ToArray());
        await tree.SetAsync("k2", "{\"v\":2}"u8.ToArray());

        var handle = await _host.Client.StartRemediationAsync(
            treeId,
            LatticeValueTransform.Passthrough(),
            new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }),
            "grpc-remediate-1");
        var status = await UntilTerminalAsync(handle.OperationId);
        var page = await _host.Client.ListSchemaOperationsAsync(new LatticeOperationListRequest { PageSize = 500 });

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("grpc-remediate-1"));
            Assert.That(handle.Kind, Is.EqualTo(SchemaOperationKinds.Remediation));
            Assert.That(handle.Created, Is.True);
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(status.Result[SchemaOperationResultKeys.ValuesProcessed], Is.EqualTo("2"));
            Assert.That(page.Operations.Select(o => o.OperationId), Does.Contain("grpc-remediate-1"));
        });
    }

    [Test]
    public async Task A_migration_of_an_unversioned_tree_is_accepted_and_fails_in_band()
    {
        var handle = await _host.Client.StartMigrationAsync("grpc-ops-unversioned");

        var status = await UntilTerminalAsync(handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(SchemaOperationKinds.Migration));
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(status.FailureReason, Does.Contain("not versioned"));
        });
    }

    [Test]
    public async Task An_advance_and_migrate_started_over_the_wire_carries_its_operation_id()
    {
        const string treeId = "grpc-ops-advance";
        await _host.Client.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(9, 1));

        var handle = await _host.Client.StartAdvanceAndMigrateAsync(treeId, 2, "grpc-advance-1");
        var status = await UntilTerminalAsync(handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("grpc-advance-1"));
            Assert.That(handle.Kind, Is.EqualTo(SchemaOperationKinds.AdvanceAndMigrate));
            Assert.That(status.IsTerminal, Is.True);
        });
    }

    [Test]
    public async Task An_unknown_operation_reads_and_cancels_as_not_found()
    {
        Assert.Multiple(async () =>
        {
            Assert.That(await _host.Client.GetSchemaOperationStatusAsync("never-started"), Is.Null);
            Assert.That(await _host.Client.CancelSchemaOperationAsync("never-started"), Is.Null);
        });
    }

    [Test]
    public void The_client_refuses_empty_arguments_before_calling()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => _host.Client.StartRemediationAsync("", LatticeValueTransform.Passthrough(),
                new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>())), Throws.ArgumentException);
            Assert.That(() => _host.Client.StartRemediationAsync("t", LatticeValueTransform.Passthrough(), null!),
                Throws.ArgumentNullException);
            Assert.That(() => _host.Client.StartMigrationAsync(""), Throws.ArgumentException);
            Assert.That(() => _host.Client.StartAdvanceAndMigrateAsync("", 2), Throws.ArgumentException);
            Assert.That(async () => await _host.Client.GetSchemaOperationStatusAsync(""), Throws.ArgumentException);
            Assert.That(async () => await _host.Client.CancelSchemaOperationAsync(""), Throws.ArgumentException);
            Assert.That(() => _host.Client.ListSchemaOperationsAsync(null!), Throws.ArgumentNullException);
        });
    }
}
