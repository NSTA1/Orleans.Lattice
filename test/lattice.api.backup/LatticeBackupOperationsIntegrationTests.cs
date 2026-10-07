using System.Text;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// End-to-end coverage of <see cref="ILatticeBackupOperations"/> (#4122) against a
/// live single-silo cluster: a start returns at once and polls to the outcome, a
/// retried start is idempotent, listing pages newest-first, and every status read,
/// listing and cancel is scoped fail-closed to the caller's tenant and grants (an
/// operation the caller may not see is not found, never forbidden).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeBackupOperationsIntegrationTests
{
    private const string Tree = "ops-orders";

    private ApiBackupClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ApiBackupClusterFixture();
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        for (var i = 0; i < 5; i++)
        {
            await tree.SetAsync($"k{i}", Encoding.UTF8.GetBytes($"v{i}"));
        }
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeBackupOperations Operations => (ILatticeBackupOperations)_fixture.Control;

    private ILatticeBackupOperations OperationsWith(ILatticeAccessGate gate, ITenantContextResolver? tenant = null) =>
        (ILatticeBackupOperations)_fixture.CreateControlWith(new BackupAccessAuthorizer(gate), tenant);

    private static LatticeBackupCaptureRequest Capture(string name = "ops") =>
        new(name, BackupScopeSelector.WholeTree(Tree));

    private static async Task<LatticeOperationStatus> UntilTerminalAsync(ILatticeBackupOperations operations, string operationId)
    {
        LatticeOperationStatus? status = null;
        await TestPoll.UntilAsync(
            async () => (status = await operations.GetOperationStatusAsync(operationId)) is { IsTerminal: true },
            $"operation {operationId} to finish");
        return status!;
    }

    [Test]
    public async Task A_started_capture_returns_at_once_and_polls_to_the_catalogued_backup()
    {
        var handle = await Operations.StartBackupAsync(Capture(), "capture-1");

        var status = await UntilTerminalAsync(Operations, handle.OperationId);
        var manifest = await _fixture.Catalog.GetAsync(status.ResultReference!);

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("capture-1"));
            Assert.That(handle.Kind, Is.EqualTo(BackupOperationKinds.Capture));
            Assert.That(handle.Created, Is.True);
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { Tree }));
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(status.PhaseCount, Is.EqualTo(2));
            Assert.That(status.FinishedAtUtc, Is.Not.Null);
            Assert.That(status.Result[BackupOperationResultKeys.BackupId], Is.EqualTo(status.ResultReference));
            Assert.That(manifest, Is.Not.Null, "The result reference names the catalogued backup.");
        });
    }

    [Test]
    public async Task A_retried_start_with_the_same_id_starts_nothing()
    {
        var first = await Operations.StartBackupAsync(Capture(), "capture-idempotent");
        await UntilTerminalAsync(Operations, first.OperationId);

        var second = await Operations.StartBackupAsync(Capture(), "capture-idempotent");

        Assert.Multiple(() =>
        {
            Assert.That(first.Created, Is.True);
            Assert.That(second.Created, Is.False);
            Assert.That(second.OperationId, Is.EqualTo(first.OperationId));
        });
    }

    [Test]
    public async Task Reusing_an_id_for_a_different_kind_is_refused()
    {
        var first = await Operations.StartBackupAsync(Capture(), "kind-clash");
        var status = await UntilTerminalAsync(Operations, first.OperationId);

        Assert.That(
            async () => await Operations.StartRestoreAsync(new LatticeRestoreRequest(status.ResultReference!), "kind-clash"),
            Throws.InvalidOperationException);
    }

    [TestCase("has space")]
    [TestCase("slash/id")]
    public void A_malformed_operation_id_is_refused_before_anything_starts(string operationId)
    {
        Assert.That(async () => await Operations.StartBackupAsync(Capture(), operationId), Throws.ArgumentException);
    }

    [Test]
    public async Task A_started_restore_records_a_revertible_restore_result()
    {
        var capture = await UntilTerminalAsync(Operations, (await Operations.StartBackupAsync(Capture())).OperationId);

        var handle = await Operations.StartRestoreAsync(new LatticeRestoreRequest(
            capture.ResultReference!, "ops-restored", mode: LatticeRestoreMode.ShadowCutover));
        var status = await UntilTerminalAsync(Operations, handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(status.Kind, Is.EqualTo(BackupOperationKinds.Restore));
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(BackupOperationResults.TryReadRestoreResult(status.Result, out var restore), Is.True);
            Assert.That(restore!.EntriesApplied, Is.EqualTo(5));
            Assert.That(restore.PreviousPhysicalTreeId, Is.Not.Null, "A shadow cutover carries what revert needs.");
        });
    }

    [Test]
    public async Task A_failed_restore_reads_as_failed_with_the_engine_reason()
    {
        var handle = await Operations.StartRestoreAsync(new LatticeRestoreRequest("no-such-backup", "ops-missing"));

        var status = await UntilTerminalAsync(Operations, handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(status.FailureReason, Does.Contain("LatticeRestoreValidationException"));
        });
    }

    [Test]
    public async Task Listing_pages_the_callers_operations_newest_first()
    {
        var ids = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            var handle = await Operations.StartBackupAsync(Capture($"page-{i}"), $"page-{i}");
            await UntilTerminalAsync(Operations, handle.OperationId);
            ids.Add(handle.OperationId);
        }

        var seen = new List<string>();
        string? token = null;
        do
        {
            var page = await Operations.ListOperationsAsync(new LatticeOperationListRequest { PageSize = 2, PageToken = token });
            Assert.That(page.Operations, Has.Count.LessThanOrEqualTo(2));
            seen.AddRange(page.Operations.Select(o => o.OperationId));
            token = page.NextPageToken;
        }
        while (token is not null);

        var mine = seen.Where(ids.Contains).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(mine, Is.EqualTo(new[] { "page-2", "page-1", "page-0" }), "Newest first, each once.");
            Assert.That(seen, Is.Unique);
        });
    }

    [Test]
    public void A_malformed_page_token_is_refused()
    {
        Assert.That(
            async () => await Operations.ListOperationsAsync(new LatticeOperationListRequest { PageToken = "garbage" }),
            Throws.ArgumentException);
    }

    [Test]
    public async Task An_operation_in_another_tenant_is_not_found_not_forbidden()
    {
        var handle = await Operations.StartBackupAsync(Capture(), "tenant-scoped");
        await UntilTerminalAsync(Operations, handle.OperationId);
        var acme = OperationsWith(new AllowGate(), new FixedTenantResolver(TenantId.Parse("acme")));

        var status = await acme.GetOperationStatusAsync(handle.OperationId);
        var cancelled = await acme.CancelOperationAsync(handle.OperationId);
        var page = await acme.ListOperationsAsync(new LatticeOperationListRequest { PageSize = 500 });

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.Null);
            Assert.That(cancelled, Is.Null);
            Assert.That(page.Operations.Select(o => o.OperationId), Does.Not.Contain(handle.OperationId));
        });
    }

    [Test]
    public async Task An_operation_over_trees_the_caller_may_not_read_is_not_found()
    {
        var handle = await Operations.StartBackupAsync(Capture(), "grant-scoped");
        await UntilTerminalAsync(Operations, handle.OperationId);
        var denied = OperationsWith(new DenyGate());

        Assert.Multiple(async () =>
        {
            Assert.That(await denied.GetOperationStatusAsync(handle.OperationId), Is.Null);
            Assert.That(await denied.CancelOperationAsync(handle.OperationId), Is.Null);
            Assert.That((await denied.ListOperationsAsync(new LatticeOperationListRequest())).Operations, Is.Empty);
        });
    }

    [Test]
    public void A_start_is_authorized_exactly_as_its_blocking_twin()
    {
        var denied = OperationsWith(new DenyGate());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await denied.StartBackupAsync(Capture()), Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await denied.StartRestoreAsync(new LatticeRestoreRequest("any", "ops-target")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }

    [Test]
    public async Task A_status_read_of_an_unknown_or_malformed_id_is_not_found()
    {
        Assert.Multiple(async () =>
        {
            Assert.That(await Operations.GetOperationStatusAsync("never-started"), Is.Null);
            Assert.That(await Operations.GetOperationStatusAsync("bad/id"), Is.Null);
            Assert.That(await Operations.CancelOperationAsync("never-started"), Is.Null);
        });
    }

    [Test]
    public async Task Cancelling_a_finished_operation_leaves_it_unchanged()
    {
        var handle = await Operations.StartBackupAsync(Capture(), "finished-cancel");
        await UntilTerminalAsync(Operations, handle.OperationId);

        var status = await Operations.CancelOperationAsync(handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(status!.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(status.CancelRequested, Is.False);
        });
    }

    private sealed class AllowGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Allow());
    }

    private sealed class DenyGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny("denied by test"));
    }

    private sealed class FixedTenantResolver(TenantId tenant) : ITenantContextResolver
    {
        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) => new(tenant);

        public bool TryResolveCurrent(out TenantId resolved)
        {
            resolved = tenant;
            return true;
        }
    }
}
