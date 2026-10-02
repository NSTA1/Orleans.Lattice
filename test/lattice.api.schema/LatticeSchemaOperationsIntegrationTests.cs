using System.Text;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// End-to-end coverage of <see cref="ILatticeSchemaOperations"/> (#4123) against a
/// live single-silo cluster: a start returns as soon as the remediation is
/// accepted, while the run is still held inside its build, and the status reports
/// the phase and the values processed out of the build's total; the run then
/// polls to its outcome. Covers the abort and cancel outcomes, idempotent start,
/// the advance-and-migrate phases, and the fail-closed scoping of every status
/// read, listing and cancel to the caller's tenant and grants. Holds are released
/// by the test, never by a clock.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeSchemaOperationsIntegrationTests
{
    private ApiSchemaClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ApiSchemaClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeSchemaOperations Operations => _fixture.Control;

    private static LatticeSchemaPolicy JsonPolicy() => new(new[] { LatticeSchemaRule.Json() });

    private static LatticeValueTransform AddStatus() => LatticeValueTransform.Passthrough(
        LatticeValueTransform.SetMember("status", LatticeValueTransform.Const(LatticeConstant.Text("ok"))));

    private async Task<string> SeedAsync(string prefix, params string[] values)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
        for (var i = 0; i < values.Length; i++)
        {
            await tree.SetAsync($"k{i}", Encoding.UTF8.GetBytes(values[i]));
        }

        return treeId;
    }

    private Task<string> SeedDocumentsAsync(string prefix, int count) =>
        SeedAsync(prefix, Enumerable.Range(0, count).Select(i => $"{{\"v\":{i}}}").ToArray());

    private static async Task<LatticeOperationStatus> UntilTerminalAsync(ILatticeSchemaOperations operations, string operationId)
    {
        LatticeOperationStatus? status = null;
        await TestPoll.UntilAsync(
            async () => (status = await operations.GetOperationStatusAsync(operationId)) is { IsTerminal: true },
            $"operation {operationId} to finish");
        return status!;
    }

    [Test]
    public async Task A_started_remediation_returns_while_its_build_is_held_and_reports_build_progress()
    {
        var treeId = await SeedDocumentsAsync("ops-held", 3);
        var hold = BuildWriteGate.Arm(treeId);
        LatticeOperationHandle handle;
        LatticeOperationStatus during;
        try
        {
            handle = await InterleaveProbe.AnswersWhileHeldAsync(
                Operations.StartRemediationAsync(treeId, AddStatus(), JsonPolicy(), "held-1"),
                hold.Release.Task,
                "the start");
            await hold.Entered.Task.WaitAsync(InterleaveProbe.HangBound);
            during = await InterleaveProbe.AnswersWhileHeldAsync(
                Operations.GetOperationStatusAsync(handle.OperationId),
                hold.Release.Task,
                "the operation status read")
                ?? throw new AssertionException("the running operation was not visible");
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        var finished = await UntilTerminalAsync(Operations, handle.OperationId);
        var report = await _fixture.Control.GetRemediationStatusAsync(treeId);
        var k0 = Encoding.UTF8.GetString(await _fixture.GrainFactory.GetGrain<ILattice>(treeId).GetAsync("k0") ?? []);

        Assert.Multiple(() =>
        {
            Assert.That(handle.Created, Is.True);
            Assert.That(handle.Kind, Is.EqualTo(SchemaOperationKinds.Remediation));
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { treeId }));
            Assert.That(during.State, Is.EqualTo(LatticeOperationState.Running));
            Assert.That(during.Phase, Is.EqualTo(SchemaOperationPhases.Build));
            Assert.That(during.PhaseIndex, Is.EqualTo(1));
            Assert.That(during.PhaseCount, Is.EqualTo(3));
            Assert.That(during.CompletedUnits, Is.Zero);
            Assert.That(during.TotalUnits, Is.EqualTo(3), "the build's total is the dry run's count");
            Assert.That(during.UnitName, Is.EqualTo(SchemaOperationPhases.ValuesUnit));
            Assert.That(finished.State, Is.EqualTo(LatticeOperationState.Succeeded), finished.FailureReason);
            Assert.That(finished.Result[SchemaOperationResultKeys.Outcome], Is.EqualTo(SchemaOperationResultKeys.Completed));
            Assert.That(finished.Result[SchemaOperationResultKeys.ValuesProcessed], Is.EqualTo("3"));
            Assert.That(report.Succeeded, Is.True);
            Assert.That(report.OperationId, Is.EqualTo("held-1"), "the tree's report names the tracked operation");
            Assert.That(k0, Does.Contain("status"));
        });
    }

    [Test]
    public async Task A_remediation_that_meets_an_unremediable_value_fails_naming_it_with_nothing_cut_over()
    {
        var treeId = await SeedAsync("ops-abort", "{\"v\":1}", "not-json");

        var handle = await Operations.StartRemediationAsync(treeId, LatticeValueTransform.Passthrough(), JsonPolicy());
        var status = await UntilTerminalAsync(Operations, handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(status.Result[SchemaOperationResultKeys.Outcome], Is.EqualTo(SchemaOperationResultKeys.Aborted));
            Assert.That(status.Result[SchemaOperationResultKeys.OffendingKey], Is.EqualTo("k1"));
            Assert.That(status.FailureReason, Does.Contain("'k1'").And.Contain("Nothing was cut over"));
        });
        Assert.That(await _fixture.Control.GetPolicyAsync(treeId), Is.Null, "nothing was installed");
    }

    [Test]
    public async Task Cancelling_a_remediation_held_in_its_build_cancels_it_and_leaves_the_tree_untouched()
    {
        // More values than one slice (512) holds, so the held slice is not the
        // build's last and the cancel lands before cutover, where it would be declined.
        var treeId = await SeedDocumentsAsync("ops-cancel", 600);
        var hold = BuildWriteGate.Arm(treeId);
        LatticeOperationHandle handle;
        LatticeOperationStatus? requested;
        try
        {
            handle = await Operations.StartRemediationAsync(treeId, AddStatus(), JsonPolicy(), "cancel-1");
            await hold.Entered.Task.WaitAsync(InterleaveProbe.HangBound);
            requested = await Operations.CancelOperationAsync(handle.OperationId);
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        var status = await UntilTerminalAsync(Operations, handle.OperationId);
        var report = await _fixture.Control.GetRemediationStatusAsync(treeId);
        var k0 = Encoding.UTF8.GetString(await _fixture.GrainFactory.GetGrain<ILattice>(treeId).GetAsync("k0") ?? []);

        Assert.Multiple(() =>
        {
            Assert.That(requested!.CancelRequested, Is.True);
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Cancelled));
            Assert.That(report.WasCancelled, Is.True);
            Assert.That(k0, Is.EqualTo("{\"v\":0}"), "the tree still serves its original values");
        });
        Assert.That(await _fixture.Control.GetPolicyAsync(treeId), Is.Null);
    }

    [Test]
    public async Task A_retried_start_with_the_same_id_starts_nothing()
    {
        var treeId = await SeedDocumentsAsync("ops-idem", 1);
        var first = await Operations.StartRemediationAsync(treeId, AddStatus(), JsonPolicy(), "idem-1");
        await UntilTerminalAsync(Operations, first.OperationId);

        var second = await Operations.StartRemediationAsync(treeId, AddStatus(), JsonPolicy(), "idem-1");

        Assert.Multiple(() =>
        {
            Assert.That(first.Created, Is.True);
            Assert.That(second.Created, Is.False);
            Assert.That(second.OperationId, Is.EqualTo("idem-1"));
        });
    }

    [Test]
    public async Task An_advance_and_migrate_reports_its_four_phases_and_restamps_the_tree()
    {
        var treeId = $"ops-migrate-{Guid.NewGuid():N}";
        await _fixture.Control.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(ApiSchemaClusterFixture.SchemaId, 1));
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("k0", Encoding.UTF8.GetBytes("{\"v\":0}"));
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("{\"v\":1}"));

        var handle = await Operations.StartAdvanceAndMigrateAsync(treeId, 2);
        var status = await UntilTerminalAsync(Operations, handle.OperationId);
        var config = await _fixture.Control.GetVersionConfigAsync(treeId);

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(SchemaOperationKinds.AdvanceAndMigrate));
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(status.PhaseCount, Is.EqualTo(4));
            Assert.That(status.Result[SchemaOperationResultKeys.ValuesProcessed], Is.EqualTo("2"));
            Assert.That(config!.Value.TargetVersion, Is.EqualTo(2u));
        });

        var again = await UntilTerminalAsync(Operations, (await Operations.StartMigrationAsync(treeId)).OperationId);
        Assert.Multiple(() =>
        {
            Assert.That(again.Kind, Is.EqualTo(SchemaOperationKinds.Migration));
            Assert.That(again.State, Is.EqualTo(LatticeOperationState.Succeeded), "an already-migrated tree is a no-op success");
        });
    }

    [Test]
    public async Task An_advance_that_does_not_advance_fails_the_operation()
    {
        var treeId = $"ops-no-advance-{Guid.NewGuid():N}";
        await _fixture.Control.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(ApiSchemaClusterFixture.SchemaId, 2));

        var status = await UntilTerminalAsync(Operations, (await Operations.StartAdvanceAndMigrateAsync(treeId, 2)).OperationId);

        Assert.That(status.State, Is.EqualTo(LatticeOperationState.Failed));
    }

    [Test]
    public async Task Listing_pages_only_schema_operations_newest_first()
    {
        var treeId = await SeedDocumentsAsync("ops-list", 1);
        var ids = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            var handle = await Operations.StartRemediationAsync(treeId, AddStatus(), JsonPolicy(), $"list-{treeId[^6..]}-{i}");
            await UntilTerminalAsync(Operations, handle.OperationId);
            ids.Add(handle.OperationId);
        }

        var seen = new List<LatticeOperationStatus>();
        string? token = null;
        do
        {
            var page = await Operations.ListOperationsAsync(new LatticeOperationListRequest { PageSize = 2, PageToken = token });
            seen.AddRange(page.Operations);
            token = page.NextPageToken;
        }
        while (token is not null);

        Assert.Multiple(() =>
        {
            Assert.That(seen.Select(s => s.OperationId).Where(ids.Contains), Is.EqualTo(Enumerable.Reverse(ids)));
            Assert.That(seen.Select(s => s.Kind), Has.All.StartWith(SchemaOperationKinds.Prefix));
        });
    }

    [Test]
    public async Task An_operation_in_another_tenant_or_over_unreadable_trees_is_not_found()
    {
        var treeId = await SeedDocumentsAsync("ops-scope", 1);
        var handle = await Operations.StartRemediationAsync(treeId, AddStatus(), JsonPolicy());
        await UntilTerminalAsync(Operations, handle.OperationId);
        var acme = _fixture.CreateControlWith(RecordingAccessGate.Allow(), new FixedTenantResolver(TenantId.Parse("acme")));
        var denied = _fixture.CreateControlWith(RecordingAccessGate.Deny());

        Assert.Multiple(async () =>
        {
            Assert.That(await acme.GetOperationStatusAsync(handle.OperationId), Is.Null);
            Assert.That(await acme.CancelOperationAsync(handle.OperationId), Is.Null);
            Assert.That((await acme.ListOperationsAsync(new LatticeOperationListRequest { PageSize = 500 })).Operations
                .Select(o => o.OperationId), Does.Not.Contain(handle.OperationId));
            Assert.That(await denied.GetOperationStatusAsync(handle.OperationId), Is.Null);
            Assert.That(await denied.CancelOperationAsync(handle.OperationId), Is.Null);
            Assert.That((await denied.ListOperationsAsync(new LatticeOperationListRequest())).Operations, Is.Empty);
        });
    }

    [Test]
    public async Task Starting_and_cancelling_need_schema_management_while_reading_needs_read()
    {
        var treeId = await SeedDocumentsAsync("ops-grants", 1);
        var hold = BuildWriteGate.Arm(treeId);
        var reader = _fixture.CreateControlWith(new OperationScopedGate(LatticeOperation.Read));
        LatticeOperationHandle handle;
        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(async () => await reader.StartRemediationAsync(treeId, AddStatus(), JsonPolicy()),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>());
                Assert.That(async () => await reader.StartMigrationAsync(treeId),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>());
                Assert.That(async () => await reader.StartAdvanceAndMigrateAsync(treeId, 2),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>());
            });

            handle = await Operations.StartRemediationAsync(treeId, AddStatus(), JsonPolicy());
            await hold.Entered.Task.WaitAsync(InterleaveProbe.HangBound);
            Assert.That(await reader.GetOperationStatusAsync(handle.OperationId), Is.Not.Null);
            Assert.That(async () => await reader.CancelOperationAsync(handle.OperationId),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        Assert.That((await UntilTerminalAsync(Operations, handle.OperationId)).State, Is.EqualTo(LatticeOperationState.Succeeded));
    }

    [TestCase("has space")]
    [TestCase("slash/id")]
    public void A_malformed_operation_id_is_refused_before_anything_starts(string operationId) =>
        Assert.That(
            async () => await Operations.StartRemediationAsync("ops-any", AddStatus(), JsonPolicy(), operationId),
            Throws.ArgumentException);

    [Test]
    public async Task A_status_read_or_cancel_of_an_unknown_or_malformed_id_is_not_found()
    {
        Assert.Multiple(async () =>
        {
            Assert.That(await Operations.GetOperationStatusAsync("never-started"), Is.Null);
            Assert.That(await Operations.GetOperationStatusAsync("bad/id"), Is.Null);
            Assert.That(await Operations.CancelOperationAsync("never-started"), Is.Null);
        });
    }

    [Test]
    public void Argument_guards_refuse_before_anything_starts()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await Operations.StartRemediationAsync("", AddStatus(), JsonPolicy()), Throws.ArgumentException);
            Assert.That(async () => await Operations.StartRemediationAsync("t", AddStatus(), null!), Throws.ArgumentNullException);
            Assert.That(async () => await Operations.StartMigrationAsync(""), Throws.ArgumentException);
            Assert.That(async () => await Operations.StartAdvanceAndMigrateAsync("", 2), Throws.ArgumentException);
            Assert.That(async () => await Operations.GetOperationStatusAsync(""), Throws.ArgumentException);
            Assert.That(async () => await Operations.CancelOperationAsync(""), Throws.ArgumentException);
            Assert.That(async () => await Operations.ListOperationsAsync(null!), Throws.ArgumentNullException);
        });
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
