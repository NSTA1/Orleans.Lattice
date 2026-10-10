using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Registry round trips paid to open a change-observation subscription.
/// </summary>
public sealed partial class LatticeStateApiEdgeCaseTests
{
    [Test]
    public async Task Observer_open_reads_the_registry_entry_once_and_routes_to_its_physical_tree()
    {
        var tree = Substitute.For<ILattice>();
        tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(true);

        // The alias and the partition count both come from the one entry; the
        // separate ResolveAsync round trip the observer used to pay is a pure
        // projection of it (GetEntryAsync(id)?.PhysicalTreeId ?? id).
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync("tree").Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { PhysicalTreeId = "tree-copy", WalPartitions = 1 }));

        var wal = Substitute.For<IWalShardGrain>();
        wal.GetNextSequenceAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<long>(0));
        wal.ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<WalShardPage>(
            WalPage(new WalShardSequencedEntry
            {
                Sequence = 0,
                Entry = new WalRecord { Op = MutationKind.Set, Key = "key" },
            })));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>("tree").Returns(tree);
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        grainFactory.GetGrain<IWalShardGrain>("tree-copy/0").Returns(wal);

        var observer = new LatticeStateObserver(
            grainFactory,
            OptionsMonitor(),
            Options.Create(new LatticeApiStateOptions { ChangeObservationPollInterval = TimeSpan.Zero }),
            new ServiceCollection().BuildServiceProvider(),
            new NullTenantContextResolver());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var enumerator = observer.ObserveAsync(new StateObserveRequest { TreeId = "tree" }, cts.Token)
            .GetAsyncEnumerator(cts.Token);
        try
        {
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current.Key, Is.EqualTo("key"));
        }
        finally
        {
            await cts.CancelAsync();
            await enumerator.DisposeAsync();
        }

        await registry.Received(1).GetEntryAsync("tree");
        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
        grainFactory.DidNotReceive().GetGrain<IWalShardGrain>("tree/0");
    }
}
