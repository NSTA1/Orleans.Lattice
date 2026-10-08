using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Starts the supported optional-package combinations around replication and
/// tenancy: replication needs neither tenancy nor membership, while tenancy
/// always runs with membership and auth. These exercise the actual Orleans host composition, not only service
/// registration descriptors.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TenantRegionBackfillOptionalCompositionTests
{
    [Test]
    public async Task Replication_starts_without_tenancy_membership_or_auth()
    {
        await AssertStartsAsync<ReplicationOnlyConfigurator>(tenancy: false, replication: true);
    }

    [Test]
    public async Task Tenancy_starts_without_replication()
    {
        await AssertStartsAsync<TenancyOnlyConfigurator>(tenancy: true, replication: false);
    }

    [Test]
    public async Task Tenancy_and_replication_start_together()
    {
        await AssertStartsAsync<TenancyReplicationConfigurator>(tenancy: true, replication: true);
    }

    private static async Task AssertStartsAsync<TConfigurator>(bool tenancy, bool replication)
        where TConfigurator : ISiloConfigurator, new()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.Options.ClusterId = "optional-composition";
        builder.AddSiloBuilderConfigurator<TConfigurator>();
        var cluster = builder.Build();

        try
        {
            await cluster.DeployAsync();
            var services = cluster.Silos.OfType<InProcessSiloHandle>().Single().SiloHost.Services;
            Assert.Multiple(() =>
            {
                Assert.That(services.GetService<IReplicationApplier>() is not null, Is.EqualTo(replication));
                Assert.That(services.GetService<ITenantRegistry>() is not null, Is.EqualTo(tenancy));
            });

            if (!tenancy)
            {
                Assert.That(
                    services.GetRequiredService<ILatticeMembershipContext>().GetType().Assembly,
                    Is.EqualTo(typeof(ILatticeMembershipContext).Assembly),
                    "Replication without tenancy must run on the core anonymous membership fallback.");
            }

            if (replication)
            {
                Assert.That(services.GetRequiredService<IReplicationApplier>(), Is.Not.Null);
            }

            if (tenancy && replication)
            {
                Assert.That(services.GetRequiredService<IReplicationTenantIsolationGate>().IsActive, Is.True);
            }
        }
        finally
        {
            await cluster.StopAllSilosAsync();
            await cluster.DisposeAsync();
        }
    }

    public abstract class BaseConfigurator : ISiloConfigurator
    {
        protected BaseConfigurator()
        {
        }

        public abstract void Configure(ISiloBuilder siloBuilder);

        protected static void ConfigureCore(ISiloBuilder siloBuilder, bool tenancy, bool replication)
        {
            siloBuilder.AddLattice((builder, name) => builder.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            if (replication)
            {
                siloBuilder.AddLatticeReplication(options => options.ClusterId = "optional-composition");
            }

            if (tenancy)
            {
                // Tenancy hard-depends on membership and auth; replication does not.
                siloBuilder.AddLatticeMembership();
                siloBuilder.AddLatticeAuth();
                siloBuilder.AddLatticeTenancy();
            }
        }
    }

    public sealed class ReplicationOnlyConfigurator : BaseConfigurator
    {
        public ReplicationOnlyConfigurator()
        {
        }

        public override void Configure(ISiloBuilder siloBuilder) => ConfigureCore(siloBuilder, tenancy: false, replication: true);
    }

    public sealed class TenancyOnlyConfigurator : BaseConfigurator
    {
        public TenancyOnlyConfigurator()
        {
        }

        public override void Configure(ISiloBuilder siloBuilder) => ConfigureCore(siloBuilder, tenancy: true, replication: false);
    }

    public sealed class TenancyReplicationConfigurator : BaseConfigurator
    {
        public TenancyReplicationConfigurator()
        {
        }

        public override void Configure(ISiloBuilder siloBuilder) => ConfigureCore(siloBuilder, tenancy: true, replication: true);
    }
}
