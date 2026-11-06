using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5876, a5e2e2ef's V2: an internal node's routing snapshot carries its declared high
/// bound, published in the same step as the children it trims, so no snapshot ever pairs a
/// narrowed child list with a missing or wider bound.
/// </summary>
public partial class BPlusInternalGrainTests
{
    [Test]
    public async Task A_split_publishes_its_narrowed_bound_with_its_children_in_one_step()
    {
        var (grain, state, factory) = CreateDigestGrain();
        var rightHalf = Substitute.For<IBPlusInternalGrain, IGrainBase>();
        var rightContext = Substitute.For<IGrainContext>();
        rightContext.GrainId.Returns(GrainId.Create("internal", "v2-right-half"));
        ((IGrainBase)rightHalf).GrainContext.Returns(rightContext);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>()).Returns(rightHalf);
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>(), Arg.Any<string?>()).Returns(rightHalf);
        await grain.InitializeAsync("k000", Child0, Child1, childrenAreLeaves: true);
        state.State.TreeId = "v2-tree";
        const int max = 128; // TestOptionsResolver's default
        for (var i = state.State.Children.Count; i <= max; i++)
        {
            state.State.Children.Add(new ChildEntry { SeparatorKey = $"k{i:D3}", ChildId = GrainId.Create("leaf", $"v2-{i}") });
        }

        var before = await grain.GetRoutingTableAsync();
        var observed = new List<RoutingTableSnapshot>();
        state.OnWriteState = _ => observed.Add(grain.GetRoutingTableAsync().Result);

        var result = await grain.AcceptSplitAsync("k999", GrainId.Create("leaf", "v2-new"));
        var after = await grain.GetRoutingTableAsync();

        Assert.Multiple(() =>
        {
            Assert.That(before.HighKeyExclusive, Is.Null, "precondition: an undivided node declares no bound");
            Assert.That(result?.PromotedKey, Is.Not.Null, "precondition: the accept divided the node");
            Assert.That(after.HighKeyExclusive, Is.EqualTo(result!.PromotedKey), "the divided node's bound is not the key it divided at");
            Assert.That(observed, Is.Not.Empty, "precondition: the split persisted");
            foreach (var snapshot in observed.Append(after))
            {
                if (snapshot.HighKeyExclusive is { } high)
                {
                    Assert.That(
                        snapshot.SeparatorKeys.Where(k => k is not null).All(k => string.CompareOrdinal(k, high) < 0),
                        Is.True,
                        "a snapshot carried a bound below one of its own separators");
                }
                else
                {
                    Assert.That(snapshot.ChildIds.Length, Is.GreaterThan(max), "a snapshot carried trimmed children without their bound");
                }
            }
        });
    }
}
