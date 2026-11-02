using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

public sealed partial class TenantRegionBackfillServiceTests
{
    [TestCase(true, false, false, "t/acme/orders", false)]
    [TestCase(false, false, true, "t/acme/orders", false)]
    [TestCase(true, true, false, "t/acme/orders", false)]
    [TestCase(true, true, false, "t/acme/orders", true)]
    [TestCase(true, false, true, null, false)]
    [TestCase(true, false, true, "t/another/orders", false)]
    public async Task Discovery_uses_derived_provenance_without_weakening_enrollment(
        bool ownerEnrolled, bool copyEnrolled, bool blocked, string? derivedFrom, bool hasAlias)
    {
        const string logical = "t/acme/orders";
        const string copy = "t/acme/opaque-copy";
        var tenant = TenantId.Parse("acme");
        var registry = new FakeTenantRegistry();
        registry.Seed(TenantRecordForBackfill(TenantRegionStatus.Backfilling));
        var trees = Substitute.For<ILatticeRegistry>();
        trees.GetAllTreeIdsAsync(Arg.Any<string?>()).Returns(new[] { logical, copy });
        trees.GetEntryAsync(copy).Returns(new TreeRegistryEntry { DerivedFrom = derivedFrom });
        trees.GetAliasesTargetingAsync(copy).Returns(hasAlias ? new[] { logical } : Array.Empty<string>());
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(trees);
        var context = Substitute.For<ILatticeReplicationContext>();
        if (ownerEnrolled)
        {
            context.ResolveMergeMode(logical).Returns(LatticeMergeMode.LwwRegister);
        }
        if (copyEnrolled)
        {
            context.ResolveMergeMode(copy).Returns(LatticeMergeMode.LwwRegister);
        }
        var bootstrap = Substitute.For<ILatticeBootstrapCoordinator>();
        bootstrap.GetStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new BootstrapCoordinatorStatus(LatticeBootstrapState.LiveIncremental, null)
            {
                CompletedSourceClusterId = "east",
            });
        var options = Options.Create(new ClusterOptions { ClusterId = "west" });
        var deadLetters = Substitute.For<ILatticeReplicationDeadLetters>();
        deadLetters.ListAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<DeadLetterEntry>>([]));
        var service = new TenantRegionBackfillService(
            registry, new TenantRegionLifecycleDriver(registry, options), factory, TenantLocalRegion.ForRegion("west"),
            Substitute.For<ILogger<TenantRegionBackfillService>>(),
            bootstrap, deadLetters, replicationContext: context);

        var progress = await service.GetProgressAsync(tenant, "west");

        if (blocked)
        {
            Assert.That(progress.StallReason,
                Does.Contain(ownerEnrolled ? copy : logical).And.Contain("not enrolled"));
        }
        else
        {
            Assert.That(progress.StallReason, Is.Null);
            Assert.That(progress.Trees.Select(tree => tree.TreeId),
                Is.EquivalentTo(copyEnrolled ? new[] { logical, copy } : new[] { logical }));
        }
    }
}
