using System.Text;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Schema.Tests;

public sealed partial class LatticeSchemaOperationsIntegrationTests
{
    [Test]
    public async Task Runtime_registry_verbs_require_their_own_grants_and_resolve_tenant_scope()
    {
        var treeId = $"runtime-grants-{Guid.NewGuid():N}";
        var registration = new LatticeSchemaRuntimeRegistration { Descriptors = [new(901, 1, "tenant-v1")] };
        var admin = _fixture.CreateControlWith(new OperationScopedGate(LatticeOperation.SchemaAdmin));
        var reader = _fixture.CreateControlWith(new OperationScopedGate(LatticeOperation.Read));
        await admin.RegisterSchemaAsync(treeId, registration);
        Assert.That((await reader.GetRegistryAsync(treeId)).Descriptors, Does.Contain(registration.Descriptors[0]));
        Assert.That(async () => await reader.RegisterSchemaAsync(treeId, registration),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(async () => await admin.GetRegistryAsync(treeId),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        var acme = _fixture.CreateControlWith(RecordingAccessGate.Allow(), new FixedTenantResolver(TenantId.Parse("acme")));
        Assert.That((await acme.GetRegistryAsync(treeId)).Descriptors.Any(d => d.SchemaId == 901), Is.False);
        await acme.RegisterSchemaAsync(treeId, new() { Descriptors = [new(902, 1, "acme-only")] });
        Assert.That((await acme.GetRegistryAsync(treeId)).Descriptors.Any(d => d.SchemaId == 902), Is.True);
        Assert.That((await reader.GetRegistryAsync(treeId)).Descriptors.Any(d => d.SchemaId == 902), Is.False);
    }

    [Test]
    public async Task Runtime_registration_drives_lazy_reads_and_eager_migration_and_is_tree_scoped()
    {
        var treeId = await SeedDocumentsAsync("runtime", 1);
        var registration = new LatticeSchemaRuntimeRegistration
        {
            Descriptors = [new(900, 1, "runtime-v1"), new(900, 2, "runtime-v2")],
            Upcasters = [new()
            {
                SchemaId = 900, FromVersion = 1, ToVersion = 2, Transform = AddStatus(),
            }],
        };
        await _fixture.Control.RegisterSchemaAsync(treeId, registration);
        await _fixture.Control.RegisterSchemaAsync(treeId, registration);
        await _fixture.Control.SetVersionConfigAsync(treeId, new(900, 1));
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("versioned", "{}"u8.ToArray());
        await _fixture.Control.AdvanceTargetVersionAsync(treeId, 2);

        Assert.That(Encoding.UTF8.GetString((await tree.GetAsync("versioned"))!), Does.Contain("\"status\":\"ok\""));
        var handle = await Operations.StartMigrationAsync(treeId);
        var finished = await UntilTerminalAsync(Operations, handle.OperationId);
        Assert.That(finished.State, Is.EqualTo(LatticeOperationState.Succeeded), finished.FailureReason);
        Assert.That((await _fixture.Control.GetRegistryAsync(treeId)).Descriptors.Count(d => d.SchemaId == 900), Is.EqualTo(2));
        Assert.That((await _fixture.Control.GetRegistryAsync("other-runtime-tree")).Descriptors.Any(d => d.SchemaId == 900), Is.False);

        Assert.That(async () => await _fixture.Control.RegisterSchemaAsync(treeId, new()
        {
            Descriptors = [new(900, 1, "incompatible")],
        }), Throws.ArgumentException);
        Assert.That(async () => await _fixture.Control.RegisterSchemaAsync(treeId, new()
        {
            Upcasters = [new() { SchemaId = 900, FromVersion = 1, ToVersion = 2, TransformId = "arbitrary-clr-type" }],
        }), Throws.ArgumentException);
        var denied = _fixture.CreateControlWith(RecordingAccessGate.Deny());
        Assert.That(async () => await denied.GetRegistryAsync(treeId), Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(async () => await denied.RegisterSchemaAsync(treeId, registration), Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task Missing_migration_configuration_rejects_before_an_operation_is_registered()
    {
        var treeId = await SeedDocumentsAsync("missing-version", 1);
        var operationId = $"missing-{Guid.NewGuid():N}";
        Assert.That(async () => await Operations.StartMigrationAsync(treeId, operationId), Throws.InvalidOperationException);
        Assert.That(await Operations.GetOperationStatusAsync(operationId), Is.Null);
        Assert.That(async () => await Operations.StartAdvanceAndMigrateAsync(treeId, 2, operationId), Throws.InvalidOperationException);
        Assert.That(await Operations.GetOperationStatusAsync(operationId), Is.Null);
        await _fixture.Control.SetVersionConfigAsync(treeId, new(ApiSchemaClusterFixture.SchemaId, 1));
        foreach (var invalidTarget in new uint[] { 0, 99 })
        {
            var invalidId = $"{operationId}-{invalidTarget}";
            Assert.That(async () => await Operations.StartAdvanceAndMigrateAsync(treeId, invalidTarget, invalidId),
                Throws.InvalidOperationException);
            Assert.That(await Operations.GetOperationStatusAsync(invalidId), Is.Null);
        }
    }

    [Test]
    public async Task Cancelling_an_active_migration_leaves_original_tree_and_terminal_cancelled_status()
    {
        var treeId = await SeedDocumentsAsync("migration-cancel", 600);
        await _fixture.Control.SetVersionConfigAsync(treeId, new(ApiSchemaClusterFixture.SchemaId, 1));
        var hold = BuildWriteGate.Arm(treeId);
        LatticeOperationHandle handle;
        try
        {
            handle = await Operations.StartMigrationAsync(treeId);
            await hold.Entered.Task.WaitAsync(InterleaveProbe.HangBound);
            var requested = await Operations.CancelOperationAsync(handle.OperationId);
            Assert.That(requested!.CancelRequested, Is.True);
        }
        finally { hold.Release.TrySetResult(); }
        var finished = await UntilTerminalAsync(Operations, handle.OperationId);
        Assert.That(finished.State, Is.EqualTo(LatticeOperationState.Cancelled), finished.FailureReason);
        Assert.That((await _fixture.Control.GetRemediationStatusAsync(treeId)).WasCancelled, Is.True);
        Assert.That(Encoding.UTF8.GetString((await _fixture.GrainFactory.GetGrain<ILattice>(treeId).GetAsync("k0"))!),
            Is.EqualTo("{\"v\":0}"));
    }
}
