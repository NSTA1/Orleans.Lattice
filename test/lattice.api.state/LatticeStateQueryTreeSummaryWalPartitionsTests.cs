using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Regression coverage for <see cref="LatticeStateQuery.GetTreeSummaryAsync"/>
/// reporting the registry-pinned <see cref="TreeConfigSummary.WalPartitions"/>
/// rather than the configured <see cref="LatticeOptions.WalPartitions"/>, as
/// <see cref="LatticeStateQuery.GetTreeCatalogAsync"/>'s own
/// <c>MapCatalogEntry</c> already does for the same field.
/// </summary>
[TestFixture]
public sealed class LatticeStateQueryTreeSummaryWalPartitionsTests
{
    private const string Tree = "orders";

    private static LatticeStateQuery CreateQuery(int pinnedWalPartitions, int configuredWalPartitions)
    {
        var grainFactory = Substitute.For<IGrainFactory>();

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Tree).Returns(Task.FromResult(Tree));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
            MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
            ShardCount = LatticeConstants.DefaultShardCount,
            WalPartitions = pinnedWalPartitions,
        }));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var lattice = Substitute.For<ILattice>();
        lattice.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
        lattice.DiagnoseAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TreeDiagnosticReport
            {
                TreeId = Tree,
                ShardCount = 1,
                VirtualShardCount = LatticeConstants.DefaultVirtualShardCount,
                SampledAt = DateTimeOffset.UtcNow,
            }));
        grainFactory.GetGrain<ILattice>(Tree).Returns(lattice);

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = configuredWalPartitions });

        return new LatticeStateQuery(
            grainFactory,
            options,
            Options.Create(new LatticeApiStateOptions()),
            new ServiceCollection().BuildServiceProvider(),
            new NullTenantContextResolver());
    }

    [Test]
    public async Task Tree_summary_reports_the_registry_pinned_wal_partitions_not_the_configured_value()
    {
        // The tree is pinned at 7 WAL partitions in the registry, but the
        // currently-configured LatticeOptions says 3: the registry pin must
        // win, exactly as the sibling catalog path (MapCatalogEntry) already
        // does for the same field.
        var query = CreateQuery(pinnedWalPartitions: 7, configuredWalPartitions: 3);

        var result = await query.GetTreeSummaryAsync(Tree);

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Summary!.Config!.WalPartitions, Is.EqualTo(7));
    }
}
