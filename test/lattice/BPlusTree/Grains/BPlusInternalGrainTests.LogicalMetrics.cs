using System.Collections.Concurrent;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusInternalGrainTests
{
    [Test]
    public async Task OnChildDigestPublishedAsync_derived_tree_timeout_tags_logical_owner()
    {
        const string physical = "digest-metric-copy";
        const string logical = "digest-metric-owner";
        var state = new FakePersistentState<InternalNodeState>
        {
            State = { ParentId = DigestParent }
        };
        var (grain, _, factory) = CreateDigestGrain(
            new LatticeOptions { DigestPublishTimeout = TimeSpan.FromMilliseconds(50) }, state);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(physical).Returns(new TreeRegistryEntry { DerivedFrom = logical });
        await grain.SetTreeIdAsync(physical);
        var parent = Substitute.For<IBPlusInternalGrain>();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(new TaskCompletionSource().Task);
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(parent);
        var labels = new ConcurrentQueue<string>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.DigestPublishTimeouts, current => current.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var tag in tags)
                    if (tag.Key == LatticeMetrics.TagTree && tag.Value is string tree
                        && (tree == physical || tree == logical))
                        labels.Enqueue(tree);
            }));

        Assert.That(async () => await grain.OnChildDigestPublishedAsync(
            DigestChild0, new ChildDigestSnapshot { Hash = Bytes16(0x22), EntryCount = 2 }),
            Throws.TypeOf<TimeoutException>());
        Assert.That(labels, Is.EqualTo(new[] { logical }));
        Assert.That(state.State.TreeId, Is.EqualTo(physical));
    }
}
