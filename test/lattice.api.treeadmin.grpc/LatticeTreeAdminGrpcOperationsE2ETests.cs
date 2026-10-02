using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the accept-then-poll tree-administration RPCs (#4124) over
/// a live, co-hosted gRPC server bound to a real cluster: a started orphaned-leaf
/// audit and a started WAL move run in the background on the silo, poll over the
/// wire to a succeeded status with their result map, and are listed.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTreeAdminGrpcOperationsE2ETests
{
    private const string Tree = "ops-customers";

    private GrpcTreeAdminClusterFixture _fixture = null!;
    private GrpcTreeAdminHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcTreeAdminClusterFixture();
        await _fixture.InitializeAsync();
        await _fixture.GrainFactory.GetGrain<ILattice>(Tree).SetAsync("k", "{}"u8.ToArray());
        _host = await _fixture.CreateGrpcHostAsync(new AllowAllTreeAdminApiAuthorizer());
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
            async () => (status = await _host.Client.GetTreeAdminOperationStatusAsync(operationId)) is { IsTerminal: true },
            $"operation {operationId} to finish");
        return status!;
    }

    [Test]
    public async Task A_started_orphaned_leaf_audit_polls_to_its_totals_over_the_wire()
    {
        var handle = await _host.Client.StartOrphanedLeavesAuditAsync(Tree, "audit-e2e");

        var status = await UntilTerminalAsync(handle.OperationId);
        var page = await _host.Client.ListTreeAdminOperationsAsync(new LatticeOperationListRequest());

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(TreeAdminOperationKinds.OrphanedLeavesAudit));
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(status.UnitName, Is.EqualTo(TreeAdminOperationUnits.Shards));
            Assert.That(status.CompletedUnits, Is.EqualTo(status.TotalUnits));
            Assert.That(status.Result[TreeAdminOperationResultKeys.TreeId], Is.EqualTo(Tree));
            Assert.That(status.Result[TreeAdminOperationResultKeys.OrphanedLeaves], Is.EqualTo("0"));
            Assert.That(page.Operations.Select(o => o.OperationId), Does.Contain("audit-e2e"));
        });
    }

    [Test]
    public async Task A_started_wal_move_to_the_current_provider_succeeds_as_already_at_target()
    {
        var placement = await _host.Client.GetWalPlacementAsync(Tree);
        var currentKey = placement.Partitions[0].ProviderKey;

        var handle = await _host.Client.StartWalMoveAsync(Tree, 0, currentKey, operationId: "move-e2e");
        var status = await UntilTerminalAsync(handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(status.Result[TreeAdminOperationResultKeys.Outcome], Is.EqualTo(nameof(TreeWalMoveOutcome.AlreadyAtTarget)));
            Assert.That(status.Result[TreeAdminOperationResultKeys.ToProviderKey], Is.EqualTo(currentKey));
        });
    }

    [Test]
    public async Task An_unknown_operation_reads_as_not_found_over_the_wire()
    {
        Assert.Multiple(async () =>
        {
            Assert.That(await _host.Client.GetTreeAdminOperationStatusAsync("no-such-op"), Is.Null);
            Assert.That(await _host.Client.CancelTreeAdminOperationAsync("no-such-op"), Is.Null);
        });
    }
}
