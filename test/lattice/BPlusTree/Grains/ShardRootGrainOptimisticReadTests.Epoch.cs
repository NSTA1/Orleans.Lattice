using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization.Invocation;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Routing-epoch and incoming-call-filter tests for the optimistic point read: every
/// non-exempt calls are bracketed as potential routing mutations.
/// </summary>
public sealed partial class ShardRootGrainOptimisticReadTests
{
    private static IIncomingGrainCallContext CallContext(Type interfaceType, string methodName, Task invocation)
    {
        var request = Substitute.For<IInvokable>();
        request.GetInterfaceType().Returns(interfaceType);
        request.GetMethodName().Returns(methodName);

        var context = Substitute.For<IIncomingGrainCallContext>();
        context.Request.Returns(request);
        context.Invoke().Returns(invocation);
        return context;
    }

    [Test]
    public void BeginRoutingMutation_and_EndRoutingMutation_each_bump_the_epoch()
    {
        var (grain, _, _, _) = CreateGrain();
        var start = grain.RoutingEpoch;

        grain.BeginRoutingMutation();
        var afterBegin = grain.RoutingEpoch;
        grain.EndRoutingMutation();

        Assert.That(afterBegin, Is.EqualTo(start + 1));
        Assert.That(grain.RoutingEpoch, Is.EqualTo(start + 2));
    }

    [TestCase(nameof(IShardRootGrain.TryGetOptimisticAsync))]
    [TestCase(nameof(IShardRootGrain.GetAsync))]
    [TestCase(nameof(IShardRootGrain.GetWithVersionAsync))]
    [TestCase(nameof(IShardRootGrain.ExistsAsync))]
    [TestCase(nameof(IShardRootGrain.GetManyAsync))]
    [TestCase(nameof(IShardRootGrain.GetHotnessAsync))]
    [TestCase(nameof(IShardRootGrain.PublishLeafByteFootprintAsync))]
    [TestCase(nameof(IShardRootGrain.SetAsync))]
    public void IsRoutingNeutralMethod_exempts_only_the_audited_calls(string methodName)
    {
        Assert.That(ShardRootGrain.IsRoutingNeutralMethod(methodName), Is.True);
    }

    [TestCase(nameof(IShardRootGrain.SetManyAsync))]
    [TestCase(nameof(IShardRootGrain.MergeManyAsync))]
    [TestCase(nameof(IShardRootGrain.DeleteAsync))]
    [TestCase(nameof(IShardRootGrain.GetRawEntryAsync))]
    [TestCase("SomeFutureMethodAsync")]
    [TestCase(null)]
    public void IsRoutingNeutralMethod_treats_everything_else_as_a_routing_mutation(string? methodName)
    {
        Assert.That(ShardRootGrain.IsRoutingNeutralMethod(methodName), Is.False);
    }

    [Test]
    public void IsRoutingNeutralMethod_every_exempt_name_exists_on_the_interface()
    {
        var exempt = typeof(IShardRootGrain).GetMethods()
            .Select(m => m.Name)
            .Where(ShardRootGrain.IsRoutingNeutralMethod)
            .Distinct()
            .ToArray();

        Assert.That(exempt, Has.Length.EqualTo(8));
    }

    [Test]
    public void IsRoutingNeutralCall_matches_the_orleans_generated_invokables_exactly()
    {
        // The runtime filter sees Orleans' generated invokables, not hand-built
        // substitutes; check the exemption against those, so a codegen change in how
        // the interface type or method name is reported cannot silently widen or
        // empty the allow-list.
        var invokables = typeof(IShardRootGrain).Assembly.GetTypes()
            .Where(t => !t.IsAbstract && !t.ContainsGenericParameters && typeof(IInvokable).IsAssignableFrom(t))
            .Select(t => Activator.CreateInstance(t) as IInvokable)
            .Where(i => i is not null && i.GetInterfaceType() == typeof(IShardRootGrain))
            .ToArray();

        var neutral = invokables
            .Where(i => ShardRootGrain.IsRoutingNeutralCall(i!))
            .Select(i => i!.GetMethodName())
            .Distinct()
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.That(invokables, Has.Length.GreaterThan(20));
        Assert.That(neutral, Is.EqualTo(new[]
        {
            nameof(IShardRootGrain.ExistsAsync),
            nameof(IShardRootGrain.GetAsync),
            nameof(IShardRootGrain.GetHotnessAsync),
            nameof(IShardRootGrain.GetManyAsync),
            nameof(IShardRootGrain.GetWithVersionAsync),
            nameof(IShardRootGrain.PublishLeafByteFootprintAsync),
            nameof(IShardRootGrain.SetAsync),
            nameof(IShardRootGrain.TryGetOptimisticAsync),
        }));
    }

    [Test]
    public void IsRoutingNeutralCall_requires_the_shard_root_interface()
    {
        var foreign = Substitute.For<IInvokable>();
        foreign.GetInterfaceType().Returns(typeof(IBPlusLeafGrain));
        foreign.GetMethodName().Returns(nameof(IShardRootGrain.GetAsync));

        Assert.That(ShardRootGrain.IsRoutingNeutralCall(foreign), Is.False);
    }

    [Test]
    public async Task Call_filter_brackets_a_suspended_mutating_call_and_refuses_optimistic_reads_meanwhile()
    {
        var (grain, _, leaf, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var mutation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var start = grain.RoutingEpoch;

        var filtered = filter.Invoke(CallContext(typeof(IShardRootGrain), nameof(IShardRootGrain.SetManyAsync), mutation.Task));
        var duringMutation = await grain.TryGetOptimisticAsync("k1");

        mutation.SetResult();
        await filtered;
        leaf.GetWithVersionAsync("k1").Returns(Stamped(Bytes("v1")));
        var afterMutation = await grain.TryGetOptimisticAsync("k1");

        Assert.That(duringMutation.IsValidated, Is.False);
        Assert.That(grain.RoutingEpoch, Is.EqualTo(start + 2));
        Assert.That(afterMutation.IsValidated, Is.True);
    }

    [Test]
    public async Task Call_filter_brackets_a_synchronously_completing_mutating_call()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var start = grain.RoutingEpoch;

        await filter.Invoke(CallContext(typeof(IShardRootGrain), nameof(IShardRootGrain.SetManyAsync), Task.CompletedTask));

        Assert.That(grain.RoutingEpoch, Is.EqualTo(start + 2));
    }

    [Test]
    public async Task Call_filter_closes_the_bracket_when_the_mutating_call_faults()
    {
        var (grain, _, leaf, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var faulted = Task.FromException(new InvalidOperationException("write failed"));

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            filter.Invoke(CallContext(typeof(IShardRootGrain), nameof(IShardRootGrain.SetManyAsync), faulted)));

        leaf.GetWithVersionAsync("k1").Returns(Stamped(Bytes("v1")));
        var result = await grain.TryGetOptimisticAsync("k1");
        Assert.That(result.IsValidated, Is.True);
    }

    [Test]
    public async Task Call_filter_does_not_bracket_a_pure_read()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var start = grain.RoutingEpoch;

        await filter.Invoke(CallContext(typeof(IShardRootGrain), nameof(IShardRootGrain.GetAsync), Task.CompletedTask));

        Assert.That(grain.RoutingEpoch, Is.EqualTo(start));
    }

    [Test]
    public async Task Serial_steady_state_read_leaves_the_epoch_unchanged()
    {
        var (grain, _, _, cache) = CreateGrain();
        var start = grain.RoutingEpoch;
        cache.GetAsync("k1").Returns(Bytes("v1"));

        await grain.GetAsync("k1");

        Assert.That(grain.RoutingEpoch, Is.EqualTo(start));
    }

    [Test]
    public async Task Serial_prepare_slow_path_is_bracketed_and_the_bracket_closes_on_fault()
    {
        // An unseeded root sends the serial read through PrepareForOperationSlowAsync
        // (root creation). Under the fakes the root creation faults part-way; the
        // bracket must still have bumped the epoch and released its in-flight count.
        var (grain, state, leaf, _) = CreateGrain(seedRoot: false);
        var start = grain.RoutingEpoch;

        try
        {
            await grain.GetAsync("k1");
        }
        catch (Exception)
        {
            // Root creation is not wired up in this harness; only the bracket matters.
        }

        Assert.That(grain.RoutingEpoch, Is.EqualTo(start + 2));

        state.State.RootNodeId = RootLeafId;
        state.State.RootIsLeaf = true;
        leaf.GetWithVersionAsync("k1").Returns(Stamped(Bytes("v1")));
        var result = await grain.TryGetOptimisticAsync("k1");
        Assert.That(result.IsValidated, Is.True);
    }
}
