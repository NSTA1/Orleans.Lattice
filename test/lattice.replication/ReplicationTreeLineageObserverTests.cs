using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4586 part 2b: the registry's pre-commit lineage hook forces a gap on
/// the receiver tree frontier of every tree enrolled for replication here, and
/// the registry-backed lineage source reads the entry's lineage.
/// </summary>
[TestFixture]
public sealed class ReplicationTreeLineageObserverTests
{
    private static ILatticeReplicationContext Enrolled(bool enrolled)
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.ResolveMergeMode(Arg.Any<string>()).Returns(enrolled ? LatticeMergeMode.LwwRegister : null);
        return context;
    }

    [Test]
    public async Task An_enrolled_trees_frontier_hears_the_next_lineage_before_it_is_persisted()
    {
        var factory = Substitute.For<IGrainFactory>();
        var frontier = Substitute.For<IReplicationTreeFrontierGrain>();
        factory.GetGrain<IReplicationTreeFrontierGrain>("t1").Returns(frontier);
        var next = Guid.NewGuid();

        await new ReplicationTreeLineageObserver(factory, Enrolled(true)).OnLineageChangingAsync("t1", Guid.NewGuid(), next, CancellationToken.None);

        await frontier.Received(1).OnLineageChangingAsync(next, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_tree_not_enrolled_here_holds_no_frontier_claims_and_is_skipped()
    {
        var factory = Substitute.For<IGrainFactory>();

        await new ReplicationTreeLineageObserver(factory, Enrolled(false)).OnLineageChangingAsync("t1", null, Guid.NewGuid(), CancellationToken.None);

        factory.DidNotReceive().GetGrain<IReplicationTreeFrontierGrain>(Arg.Any<string>(), Arg.Any<string?>());
    }

    [Test]
    public async Task Without_an_enrollment_source_every_tree_is_notified()
    {
        var factory = Substitute.For<IGrainFactory>();
        var frontier = Substitute.For<IReplicationTreeFrontierGrain>();
        factory.GetGrain<IReplicationTreeFrontierGrain>("t1").Returns(frontier);

        await new ReplicationTreeLineageObserver(factory).OnLineageChangingAsync("t1", Guid.NewGuid(), null, CancellationToken.None);

        await frontier.Received(1).OnLineageChangingAsync(null, Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_frontier_that_cannot_force_the_gap_fails_the_lineage_change()
    {
        var factory = Substitute.For<IGrainFactory>();
        var frontier = Substitute.For<IReplicationTreeFrontierGrain>();
        frontier.OnLineageChangingAsync(Arg.Any<Guid?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("frontier down")));
        factory.GetGrain<IReplicationTreeFrontierGrain>("t1").Returns(frontier);

        Assert.That(
            () => new ReplicationTreeLineageObserver(factory, Enrolled(true)).OnLineageChangingAsync("t1", null, Guid.NewGuid(), CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task The_registry_source_reads_the_entrys_lineage_and_none_for_a_missing_row()
    {
        var lineage = Guid.NewGuid();
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync("t1").Returns(new TreeRegistryEntry { Lineage = lineage });
        registry.GetEntryAsync("t2").Returns((TreeRegistryEntry?)null);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var source = new RegistryTreeLineageSource(factory);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await source.GetLineageAsync("t1", CancellationToken.None), Is.EqualTo(lineage));
            Assert.That(await source.GetLineageAsync("t2", CancellationToken.None), Is.Null);
        });
    }

    [Test]
    public void Arguments_are_validated()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.Multiple(() =>
        {
            Assert.That(() => new ReplicationTreeLineageObserver(null!), Throws.ArgumentNullException);
            Assert.That(() => new RegistryTreeLineageSource(null!), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationTreeLineageObserver(factory).OnLineageChangingAsync("", null, null, CancellationToken.None), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => new RegistryTreeLineageSource(factory).GetLineageAsync("", CancellationToken.None), Throws.InstanceOf<ArgumentException>());
        });
    }
}