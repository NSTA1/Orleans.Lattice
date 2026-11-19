using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for <see cref="ReplicationReseedResponderService"/>, the public
/// receive-side seam a custom transport answers the forced-gap re-seed through.
/// </summary>
[TestFixture]
public sealed class ReplicationReseedResponderServiceTests
{
    private const string Tree = "orders";
    private const string Origin = "site-b";

    private static (ReplicationReseedResponderService Responder, IGrainFactory Factory, ILatticeBootstrapCoordinatorGrain Coordinator)
        Create(bool enrolled)
    {
        var coordinator = Substitute.For<ILatticeBootstrapCoordinatorGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(Tree).Returns(coordinator);
        var context = Substitute.For<ILatticeReplicationContext>();
        context.ResolveMergeMode(Tree).Returns(enrolled ? LatticeMergeMode.LwwRegister : null);
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "site-a" });
        return (new ReplicationReseedResponderService(factory, options, context), factory, coordinator);
    }

    private static WalRecord PreparedEntry() => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = "k",
        Value = [1],
        Timestamp = new HybridLogicalClock { WallClockTicks = 1 },
        OriginClusterId = Origin,
        IsPrepared = true,
        TransactionId = Guid.NewGuid(),
        AtomicBatchSize = 1,
    };

    [Test]
    public async Task RespondAsync_echoes_a_completed_bootstrap_past_the_requested_epoch()
    {
        var (responder, _, coordinator) = Create(enrolled: true);
        coordinator.GetCompletedExportEpochAsync(Origin).Returns(9L);

        Assert.That(await responder.RespondAsync(Tree, Origin, reseedAfterEpoch: 5), Is.EqualTo(9L));
        await coordinator.DidNotReceiveWithAnyArgs().BootstrapForReseedAsync(default!, default, default);
    }

    [Test]
    public async Task A_tree_not_enrolled_here_is_answered_with_nothing_and_activates_no_coordinator()
    {
        var (responder, factory, _) = Create(enrolled: false);

        Assert.Multiple(async () =>
        {
            Assert.That(await responder.RespondAsync(Tree, Origin, reseedAfterEpoch: 5), Is.Null);
            Assert.That(await responder.RefusesStragglerAsync(Tree, Origin, [PreparedEntry()]), Is.False);
        });
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILatticeBootstrapCoordinatorGrain>(default(string)!);
    }

    [Test]
    public async Task RefusesStragglerAsync_refuses_a_saga_record_while_the_sender_awaits_a_reseed()
    {
        var (responder, _, coordinator) = Create(enrolled: true);
        coordinator.IsReseedPendingAsync(Origin).Returns(true);

        Assert.That(await responder.RefusesStragglerAsync(Tree, Origin, [PreparedEntry()]), Is.True);
    }

    [Test]
    public void The_seam_rejects_a_missing_tree_or_origin()
    {
        var (responder, _, _) = Create(enrolled: true);

        Assert.Multiple(() =>
        {
            Assert.That(() => responder.RespondAsync("", Origin, 1), Throws.ArgumentException);
            Assert.That(() => responder.RespondAsync(Tree, null!, 1), Throws.ArgumentNullException);
            Assert.That(() => responder.RefusesStragglerAsync(Tree, Origin, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void AddLatticeReplication_registers_the_seam()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var descriptor = services.Single(d => d.ServiceType == typeof(IReplicationReseedResponder));
        Assert.That(descriptor.ImplementationType, Is.EqualTo(typeof(ReplicationReseedResponderService)));
    }
}
