using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Samples.Explorer;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Real silo startup coverage for tenancy, replication and backup composition.</summary>
[TestFixture]
[Category("Integration")]
public sealed class TenantBackupStartupIntegrationTests
{
    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task Host_tenancy_and_backup_enforce_the_replicated_sink_contract(bool replication, bool sharedSink)
    {
        var builder = new TestClusterBuilder(1);
        if (!replication)
        {
            builder.AddSiloBuilderConfigurator<SingleClusterConfigurator>();
        }
        else if (sharedSink)
        {
            builder.AddSiloBuilderConfigurator<SharedSinkConfigurator>();
        }
        else
        {
            builder.AddSiloBuilderConfigurator<ReplicatedConfigurator>();
        }

        await using var cluster = builder.Build();
        if (replication && !sharedSink)
        {
            var exception = Assert.CatchAsync(async () => await cluster.DeployAsync());
            Assert.That(exception!.ToString(), Does.Contain("sys-tenant-registry")
                .And.Contain("enrolled automatically by tenancy").And.Contain("shared external"));
            return;
        }

        await cluster.DeployAsync();
        var services = cluster.Silos.OfType<InProcessSiloHandle>().Single().SiloHost.Services;
        var trees = services.GetRequiredService<IOptions<LatticeReplicationOptions>>().Value.ReplicatedTrees;
        Assert.That(trees?.ContainsKey(TenantTreeNames.RegistryTree) ?? false, Is.EqualTo(replication));
        using (LatticeSystemOrigin.Enter())
        {
            Assert.That(await services.GetRequiredService<ITenantRegistry>().GetAsync(TenantId.Default), Is.Not.Null);
        }
        await cluster.StopAllSilosAsync();
    }

    private static void Configure(ISiloBuilder silo, bool replication, bool sharedSink)
    {
        silo.AddLattice((builder, name) => builder.AddMemoryGrainStorage(name));
        silo.UseInMemoryReminderService();
        silo.AddLatticeMembership();
        silo.AddLatticeAuth();
        silo.AddLatticeTenancy();
        if (sharedSink)
        {
            silo.Services.AddSingleton<ILatticeBackupSink>(new SampleSharedBackupSink());
        }
        silo.AddLatticeBackup();
        if (replication)
        {
            silo.AddLatticeReplication(options => options.ClusterId = "tenant-backup-startup");
        }
    }

    private sealed class SingleClusterConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => TenantBackupStartupIntegrationTests.Configure(siloBuilder, false, false);
    }

    private sealed class ReplicatedConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => TenantBackupStartupIntegrationTests.Configure(siloBuilder, true, false);
    }

    private sealed class SharedSinkConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => TenantBackupStartupIntegrationTests.Configure(siloBuilder, true, true);
    }
}
