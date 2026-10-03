using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The routing tier honours an atomic-write saga's binding to one physical copy
/// (issue #4358). Stateless routing activations each cache a routing pair, so
/// across an alias swap one of them can still address the copy the swap left
/// behind; a saga's prepared batch dispatched through it used to land there while
/// the commit went to the copy the saga was bound to.
/// </summary>
public partial class LatticeGrainTests
{
    private const string BindingAlias = "binding-tree";
    private const string BoundCopy = "binding-copy-a";
    private const string OtherCopy = "binding-copy-b";

    private static List<KeyValuePair<string, byte[]>> BindingBatch() =>
        [new("k1", [1]), new("k2", [2]), new("k3", [3])];

    private static async Task SetManyBoundAsync(LatticeGrain grain, string? boundPhysicalTreeId, bool prepared = true)
    {
        using var preparedScope = prepared ? LatticePreparedContext.BeginScope() : null;
        using var binding = LatticeAtomicBindingContext.With(boundPhysicalTreeId);
        await grain.SetManyAsync(BindingBatch());
    }

    private static int ShardResolutionsFor(IGrainFactory factory, string physicalTreeId) =>
        factory.ReceivedCalls().Count(c =>
            c.GetMethodInfo().Name == nameof(IGrainFactory.GetGrain)
            && c.GetArguments().FirstOrDefault() is string key
            && key.StartsWith(physicalTreeId + "/", StringComparison.Ordinal));

    [Test]
    public void SetManyAsync_under_a_saga_binding_refuses_a_tree_that_moved_off_the_bound_copy()
    {
        var (grain, factory, registry) = CreateGrainWithRegistry(BindingAlias);
        registry.ResolveAsync(BindingAlias).Returns(Task.FromResult(OtherCopy));
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, BindingAlias);

        var ex = Assert.ThrowsAsync<StaleTreeRoutingException>(() => SetManyBoundAsync(grain, BoundCopy));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.LogicalTreeId, Is.EqualTo(BindingAlias));
            Assert.That(ex.StalePhysicalTreeId, Is.EqualTo(BoundCopy),
                "the saga recognises a move of its own binding by the stale copy it names");
            Assert.That(ex.DestinationPhysicalTreeId, Is.EqualTo(OtherCopy));
            Assert.That(ShardResolutionsFor(factory, OtherCopy), Is.Zero,
                "the prepared batch must not be placed on a copy the saga is not bound to");
        });
        shardRoot.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task SetManyAsync_under_a_saga_binding_rereads_a_pair_cached_before_a_swap()
    {
        // The activation cached the other copy before the swap; the registry now
        // names the bound copy. The batch must go to the bound copy, not to the
        // copy the stale pair addresses.
        var (grain, factory, registry) = CreateGrainWithRegistry(BindingAlias);
        registry.ResolveAsync(BindingAlias).Returns(Task.FromResult(OtherCopy), Task.FromResult(BoundCopy));
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, BindingAlias);
        Assert.That((await grain.GetRoutingAsync()).PhysicalTreeId, Is.EqualTo(OtherCopy), "precondition: a stale pair is cached");

        await SetManyBoundAsync(grain, BoundCopy);

        Assert.Multiple(() =>
        {
            Assert.That(ShardResolutionsFor(factory, BoundCopy), Is.GreaterThan(0));
            Assert.That(ShardResolutionsFor(factory, OtherCopy), Is.Zero);
        });
        await shardRoot.ReceivedWithAnyArgs().SetManyAsync(default!);
    }

    [Test]
    public async Task SetManyAsync_hands_the_saga_binding_to_the_shards_of_the_bound_copy_only()
    {
        // The bound copy's shards are told the batch is addressed to them, so a
        // copy an online resize has fenced still takes it and mirrors it (#4369).
        // The binding is taken off the request context on entry and handed on only
        // for the dispatch to the bound copy.
        var (grain, factory, registry) = CreateGrainWithRegistry(BindingAlias);
        registry.ResolveAsync(BindingAlias).Returns(Task.FromResult(BoundCopy));
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, BindingAlias);
        var seenByShard = new List<string?>();
        shardRoot.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(_ =>
        {
            seenByShard.Add(LatticeAtomicBindingContext.Current);
            return Task.CompletedTask;
        });

        await SetManyBoundAsync(grain, BoundCopy);

        Assert.That(seenByShard, Is.Not.Empty);
        Assert.That(seenByShard, Is.All.EqualTo(BoundCopy));
    }

    [Test]
    public async Task SetManyAsync_outside_a_prepared_scope_does_not_hand_a_binding_to_the_shards()
    {
        var (grain, factory, registry) = CreateGrainWithRegistry(BindingAlias);
        registry.ResolveAsync(BindingAlias).Returns(Task.FromResult(BoundCopy));
        var shardRoot = SetupShardRoot(factory);
        SetupCompactionGrain(factory, BindingAlias);
        var seenByShard = new List<string?>();
        shardRoot.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(_ =>
        {
            seenByShard.Add(LatticeAtomicBindingContext.Current);
            return Task.CompletedTask;
        });

        await SetManyBoundAsync(grain, BoundCopy, prepared: false);

        Assert.That(seenByShard, Is.Not.Empty);
        Assert.That(seenByShard, Is.All.Null, "only an atomic-write saga's prepared dispatch is bound to a copy");
    }

    [Test]
    public async Task SetManyAsync_ignores_a_binding_outside_a_prepared_scope()
    {
        var (grain, factory, registry) = CreateGrainWithRegistry(BindingAlias);
        registry.ResolveAsync(BindingAlias).Returns(Task.FromResult(OtherCopy));
        SetupShardRoot(factory);
        SetupCompactionGrain(factory, BindingAlias);

        await SetManyBoundAsync(grain, BoundCopy, prepared: false);

        Assert.That(ShardResolutionsFor(factory, OtherCopy), Is.GreaterThan(0),
            "only an atomic-write saga's prepared dispatch is bound to a copy");
    }

    [Test]
    public void LatticeAtomicBindingContext_With_restores_the_previous_binding_and_Take_removes_it()
    {
        Assert.That(LatticeAtomicBindingContext.Current, Is.Null);
        using (LatticeAtomicBindingContext.With(BoundCopy))
        {
            using (LatticeAtomicBindingContext.With(OtherCopy))
            {
                Assert.That(LatticeAtomicBindingContext.Current, Is.EqualTo(OtherCopy));
            }

            Assert.That(LatticeAtomicBindingContext.Current, Is.EqualTo(BoundCopy));
            Assert.That(LatticeAtomicBindingContext.Take(), Is.EqualTo(BoundCopy));
            Assert.That(LatticeAtomicBindingContext.Current, Is.Null);
            Assert.That(LatticeAtomicBindingContext.Take(), Is.Null);
        }

        using (LatticeAtomicBindingContext.With(null))
        {
            Assert.That(RequestContext.Get(LatticeEventConstants.AtomicBoundPhysicalTreeRequestContextKey), Is.Null);
        }
    }
}
