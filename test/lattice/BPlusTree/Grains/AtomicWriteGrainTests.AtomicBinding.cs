using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The saga binds its prepared dispatch to the physical copy it will commit on,
/// and follows that copy when the routing tier reports it moved (issue #4358).
/// Before the binding, a routing activation still addressing a copy an alias swap
/// had left behind placed the prepares there, while the commit decision and
/// terminals went to the bound copy - a transient tear, or a lost batch.
/// </summary>
public partial class AtomicWriteGrainTests
{
    private const string MovedCopy = "atomic-tree-copy";

    private static RoutingInfo RoutingTo(string physicalTreeId) =>
        new(physicalTreeId, ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount));

    private static int ShardAddressesOn(IGrainFactory factory, string physicalTreeId) =>
        factory.ReceivedCalls().Count(c =>
            c.GetMethodInfo().Name == nameof(IGrainFactory.GetGrain)
            && c.GetArguments().FirstOrDefault() is string key
            && key.StartsWith(physicalTreeId + "/", StringComparison.Ordinal));

    [Test]
    public async Task ExecuteAsync_binds_its_prepared_dispatch_to_the_copy_it_prepared_on()
    {
        var (grain, state, _, lattice, _) = CreateGrain();
        var bindings = new List<string?>();
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(_ =>
        {
            bindings.Add(LatticeAtomicBindingContext.Current);
            return Task.CompletedTask;
        });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2])));

        Assert.Multiple(() =>
        {
            Assert.That(bindings, Is.EqualTo(new[] { TreeId }),
                "the prepared batch must carry the physical copy the saga is bound to");
            Assert.That(state.State.BoundPhysicalTreeId, Is.EqualTo(TreeId));
            Assert.That(LatticeAtomicBindingContext.Current, Is.Null, "the binding must not leak past the dispatch");
        });
    }

    [Test]
    public async Task ExecuteAsync_rebinds_and_commits_on_the_new_copy_when_its_bound_copy_moved_during_dispatch()
    {
        IGrainFactory factory = null!;
        var (grain, state, _, lattice, _) = CreateGrain(configureFactory: f => factory = f);
        var current = RoutingTo(TreeId);
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<RoutingInfo>(current));
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<RoutingInfo>(current));

        var bindings = new List<string?>();
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(_ =>
        {
            bindings.Add(LatticeAtomicBindingContext.Current);
            if (bindings.Count == 1)
            {
                // An alias swap lands between the prepare and the dispatch: the
                // routing tier refuses to place the batch off the bound copy.
                current = RoutingTo(MovedCopy);
                throw new StaleTreeRoutingException(TreeId, TreeId, MovedCopy);
            }

            return Task.CompletedTask;
        });
        var retriesPersisted = new List<int>();
        state.OnWriteState = s => retriesPersisted.Add(s.RetriesOnCurrentStep);

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2])));

        Assert.Multiple(() =>
        {
            Assert.That(bindings, Is.EqualTo(new[] { TreeId, MovedCopy }),
                "the batch is re-dispatched once, bound to the copy the tree moved to");
            Assert.That(state.State.BoundPhysicalTreeId, Is.EqualTo(MovedCopy));
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed),
                "a move of the bound copy is not a batch failure and must not compensate");
            Assert.That(retriesPersisted, Is.All.EqualTo(0), "following the binding must not spend the retry budget");
            Assert.That(ShardAddressesOn(factory, MovedCopy), Is.GreaterThan(0),
                "the terminals go to the copy the prepares were placed on");
        });
    }

    [Test]
    public async Task ReceiveReminder_binds_a_saga_resumed_from_state_without_a_binding_before_it_dispatches()
    {
        // State persisted before the binding existed: the resumed dispatch must
        // still be held to one copy, without re-dispatching what is done.
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Execute;
        state.State.TreeId = TreeId;
        state.State.Entries = MakeEntries(("a", [1]), ("b", [2]), ("c", [3]));
        state.State.PreValues =
        [
            new AtomicPreValue { Key = "a", Value = null, Existed = false },
            new AtomicPreValue { Key = "b", Value = null, Existed = false },
            new AtomicPreValue { Key = "c", Value = null, Existed = false },
        ];
        state.State.NextIndex = 1;
        state.State.AtomicBatchSize = 3;
        state.State.TransactionId = Guid.NewGuid();
        state.State.TouchedShards = [0];
        Assert.That(state.State.BoundPhysicalTreeId, Is.Null, "precondition: legacy state carries no binding");

        var (grain, _, _, lattice, _) = CreateGrain(state);
        var dispatched = new List<(string? Binding, List<string> Keys)>();
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(callInfo =>
        {
            dispatched.Add((LatticeAtomicBindingContext.Current,
                ((List<KeyValuePair<string, byte[]>>)callInfo[0]).Select(kv => kv.Key).ToList()));
            return Task.CompletedTask;
        });

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(dispatched, Has.Count.EqualTo(1));
            Assert.That(dispatched[0].Binding, Is.EqualTo(TreeId));
            Assert.That(dispatched[0].Keys, Is.EqualTo(new[] { "b", "c" }), "the bind keeps the resumed position");
            Assert.That(state.State.BoundPhysicalTreeId, Is.EqualTo(TreeId));
            Assert.That(state.State.TouchedShards, Does.Contain(0), "shards an earlier attempt may have prepared on are kept");
        });
    }

    [Test]
    public async Task ExecuteAsync_treats_a_refusal_as_an_ordinary_failure_when_the_tree_still_resolves_to_its_bound_copy()
    {
        var (grain, state, _, lattice, _) = CreateGrain();
        var calls = 0;
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(_ =>
        {
            if (calls++ == 0)
            {
                throw new StaleTreeRoutingException(TreeId, TreeId, MovedCopy);
            }

            return Task.CompletedTask;
        });
        var retriesPersisted = new List<int>();
        state.OnWriteState = s => retriesPersisted.Add(s.RetriesOnCurrentStep);

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2])));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(state.State.BoundPhysicalTreeId, Is.EqualTo(TreeId), "no move, so no re-bind");
            Assert.That(retriesPersisted, Does.Contain(1), "the refusal is retried through the ordinary budget");
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        });
    }
}
