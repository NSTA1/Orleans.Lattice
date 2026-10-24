using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Schema;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Schema.Tests;

[TestFixture]
[Category("Integration")]
public sealed class SchemaRuntimeScaleOutTests
{
    [Test]
    public async Task Enforcement_only_host_reports_independent_support_and_rejects_migration_without_a_handle()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<EnforcementOnlySilo>();
        await using var cluster = builder.Build();
        await cluster.DeployAsync();
        var control = cluster.Silos.OfType<InProcessSiloHandle>().Single()
            .SiloHost.Services.GetRequiredService<LatticeSchemaControl>();
        var caps = await control.ProbeCapabilitiesAsync("not-versioned");
        Assert.Multiple(() =>
        {
            Assert.That(caps.SupportsEnforcement, Is.True);
            Assert.That(caps.SupportsRemediation, Is.True);
            Assert.That(caps.SupportsCompliance, Is.True);
            Assert.That(caps.SupportsDeadLetters, Is.True);
            Assert.That(caps.SupportsVersioning, Is.False);
            Assert.That(caps.SupportsRuntimeRegistration, Is.False);
            Assert.That(caps.CanManageVersion, Is.True, "permission remains independent");
            Assert.That(caps.CanStartMigration, Is.False);
            Assert.That(caps.CanStartRemediation, Is.True);
        });
        Assert.That(async () => await control.StartMigrationAsync("not-versioned", "unsupported-migration"),
            Throws.InvalidOperationException);
        Assert.That(await control.GetOperationStatusAsync("unsupported-migration"), Is.Null);
        Assert.That(async () => await control.GetRegistryAsync("not-versioned"), Throws.InvalidOperationException);
    }

    [Test]
    public async Task Runtime_registry_is_shared_between_silos_and_survives_activation_collection()
    {
        var fixture = new ApiSchemaClusterFixture();
        await fixture.InitializeAsync(2);
        try
        {
            var first = fixture.Control;
            var second = fixture.Cluster.Silos.OfType<InProcessSiloHandle>().Last()
                .SiloHost.Services.GetRequiredService<ILatticeSchemaControl>();
            const string treeId = "shared-runtime-catalog";
            var registration = new LatticeSchemaRuntimeRegistration
            {
                Descriptors = [new(600, 1, "shared-v1"), new(600, 2, "shared-v2")],
                Upcasters = [new()
                {
                    SchemaId = 600, FromVersion = 1, ToVersion = 2, Transform = LatticeValueTransform.Passthrough(),
                }],
            };
            await first.RegisterSchemaAsync(treeId, registration);
            Assert.That((await second.GetRegistryAsync(treeId)).Descriptors, Does.Contain(registration.Descriptors[0]));
            await fixture.GrainFactory.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);
            var after = await second.GetRegistryAsync(treeId);
            Assert.That(after.Descriptors, Does.Contain(registration.Descriptors[1]));
            await second.RegisterSchemaAsync(treeId, registration);
            Assert.That((await first.GetRegistryAsync(treeId)).Upcasters.Count(h => h.SchemaId == 600), Is.EqualTo(1));
            var caps = await second.ProbeCapabilitiesAsync(treeId);
            Assert.Multiple(() =>
            {
                Assert.That(caps.SupportsEnforcement, Is.True);
                Assert.That(caps.SupportsVersioning, Is.True);
                Assert.That(caps.SupportsRemediation, Is.True);
                Assert.That(caps.SupportsCompliance, Is.True);
                Assert.That(caps.SupportsDeadLetters, Is.True);
                Assert.That(caps.SupportsRuntimeRegistration, Is.True);
                Assert.That(caps.CanStartMigration, Is.False, "a catalog alone does not configure the tree");
            });
            await first.SetVersionConfigAsync(treeId, new(600, 1));
            Assert.That((await second.ProbeCapabilitiesAsync(treeId)).CanStartMigration, Is.True);
        }
        finally { await fixture.DisposeAsync(); }
    }

    private sealed class EnforcementOnlySilo : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.AddLatticeSchemaApi();
        }
    }
}
