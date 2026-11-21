using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #5832: a re-seed's bootstrap copy takes the tree's alias reservation
/// only at cutover, so an alias operation that started during the drain (a schema
/// remediation, say) refuses the cutover. The coordinator must wait and retry,
/// not fail the bootstrap and discard a complete copy.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    [Test]
    public async Task A_cutover_refused_by_another_alias_holder_keeps_the_bootstrap_and_its_copy_then_retries()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        fake.State.UseShadowCopy = true;
        fake.State.ShadowCopyTreeId = $"{Tree}/shadow";
        var (grain, _, factory, _, _, _, _, _) = Create(fake);
        var resize = factory.GetGrain<ITreeResizeGrain>(Tree);
        resize.CompleteBootstrapCopyAsync(Arg.Any<string>())
            .ThrowsAsync(new LatticeAliasOperationInProgressException(Tree, "remediation:1"));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(fake.State.InProgress, Is.True, "the bootstrap must not be failed");
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
            Assert.That(fake.State.ShadowCopyCutoverComplete, Is.False);
            await resize.DidNotReceive().AbortBootstrapCopyAsync(Arg.Any<string>());
        });

        await grain.ProcessNextPhaseAsync();

        await resize.Received(2).CompleteBootstrapCopyAsync(Arg.Any<string>());
        await resize.DidNotReceive().AbortBootstrapCopyAsync(Arg.Any<string>());
    }
}
