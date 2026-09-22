using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for #3348: a <see cref="LatticeSaturatedException"/>
/// raised by the WAL admission gate must surface from the public
/// <see cref="ILattice"/> write surface untouched, rather than being absorbed
/// by the stale-routing retry.
/// <para>
/// The defect was a type-hierarchy accident with an outsized blast radius.
/// <see cref="LatticeSaturatedException"/> derives from
/// <see cref="InvalidOperationException"/> deliberately, so that a generic
/// handler absorbs it instead of crashing a caller that never anticipated it.
/// The write-path catch in <c>LatticeGrain</c> is typed on exactly that base,
/// and it does not merely absorb: it invalidates every routing cache and
/// re-issues the whole operation. A single refused branch therefore re-fanned
/// the entire batch across every shard into a tree that had just reported it
/// was full, roughly doubling offered write volume at the precise moment the
/// gate asked for less.
/// </para>
/// <para>
/// The tests below pin both halves of the fix, and the second half matters as
/// much as the first: the catch had to be <em>narrowed</em>, not removed. The
/// control tests assert that a plain <see cref="InvalidOperationException"/>
/// still gets its single cache-invalidating retry, so a future change that
/// deletes the catch outright fails here rather than silently regressing the
/// deleted-tree and stale-alias paths the retry exists to serve.
/// </para>
/// </summary>
public partial class LatticeGrainTests
{
    /// <summary>
    /// Finds a key whose virtual slot under a map of
    /// <paramref name="virtualShardCount"/> slots is
    /// <paramref name="slot"/>. The routing hash is not something a test
    /// should hard-code keys against, so the keys are discovered with the
    /// same function the grain routes with. Deterministic at runtime and
    /// immune to a future change of hash.
    /// </summary>
    private static string FindKeyForSlot(int slot, int virtualShardCount)
    {
        for (var i = 0; i < 10_000; i++)
        {
            var key = $"sat-{i}";
            if (LatticeGrain.GetShardIndex(key, virtualShardCount) == slot)
                return key;
        }

        throw new InvalidOperationException(
            $"No key routed to slot {slot} of {virtualShardCount} within the search bound.");
    }

    [Test]
    public async Task SetManyAsync_surfaces_LatticeSaturatedException_without_refanning_the_batch()
    {
        // Two physical shards, one virtual slot each, one entry bound for
        // each. Shard 0 refuses with back-pressure; shard 1 would accept.
        const string treeId = "saturation-setmany-no-refanout";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);
        SetupCompactionGrain(factory, treeId);

        var map = new ShardMap { Slots = [0, 1], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));

        var shard0 = Substitute.For<IShardRootGrain>();
        var shard1 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>()).Returns(shard0);
        factory.GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>()).Returns(shard1);

        shard0.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns<Task>(_ => throw new LatticeSaturatedException(
                "WAL admission gate refused the append.", treeId));
        shard1.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        var batch = new List<KeyValuePair<string, byte[]>>
        {
            new(FindKeyForSlot(0, 2), [1]),
            new(FindKeyForSlot(1, 2), [2]),
        };

        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await grain.SetManyAsync(batch));

        // The typed exception must reach the caller intact, carrying its
        // attribution, because honouring the documented back-off contract is
        // something only the caller can do.
        Assert.That(thrown!.TreeId, Is.EqualTo(treeId));

        // The refusing shard is asked exactly once. Before the fix the catch
        // re-issued the whole operation, so this was 2.
        await shard0.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());

        // This is the amplification assertion, and the one that actually
        // encodes #3348. Shard 1 never refused anything; it is collateral of
        // the re-fanout. Before the fix a single refusal anywhere in the
        // fan-out doubled the write volume offered to every other branch of a
        // tree that had just said it was full.
        await shard1.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task SetManyAsync_still_retries_a_plain_InvalidOperationException_once()
    {
        // Control for the test above: the catch was narrowed, not deleted.
        // A plain InvalidOperationException (the stale-alias and deleted-tree
        // signal the retry exists to serve) must still earn its single
        // cache-invalidating retry.
        const string treeId = "saturation-setmany-control";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 1, virtualShardCount: 1);
        SetupCompactionGrain(factory, treeId);

        var map = new ShardMap { Slots = [0], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));

        var shard0 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>()).Returns(shard0);

        var calls = 0;
        shard0.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns<Task>(_ =>
            {
                calls++;
                if (calls == 1)
                    throw new InvalidOperationException("stale alias");
                return Task.CompletedTask;
            });

        await grain.SetManyAsync([new("k1", [1])]);

        Assert.That(calls, Is.EqualTo(2),
            "A plain InvalidOperationException must still earn exactly one retry; "
            + "narrowing the catch must not disable the stale-alias path.");
    }

    [Test]
    public void SetAsync_surfaces_LatticeSaturatedException_without_retrying()
    {
        // The single-key write path inlines its own copy of the retry loop to
        // elide a per-call allocation, so it carries the defect independently
        // of the shared helper and needs its own regression pin.
        const string treeId = "saturation-set-no-retry";
        var (grain, factory) = CreateGrain(treeId, shardCount: 1);
        SetupCompactionGrain(factory, treeId);
        var shardRoot = SetupShardRoot(factory);

        shardRoot.SetAsync("k1", Arg.Any<byte[]>())
            .Returns<Task>(_ => throw new LatticeSaturatedException(
                "WAL admission gate refused the append.", treeId));

        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await grain.SetAsync("k1", [1]));

        Assert.That(thrown!.TreeId, Is.EqualTo(treeId));
        Assert.That(
            shardRoot.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.SetAsync)),
            Is.EqualTo(1),
            "A saturation refusal must not be retried on the point-write path either.");
    }

    [Test]
    public async Task SetAsync_still_retries_a_plain_InvalidOperationException_once()
    {
        // Control for the point-write path, mirroring the batch control above.
        const string treeId = "saturation-set-control";
        var (grain, factory) = CreateGrain(treeId, shardCount: 1);
        SetupCompactionGrain(factory, treeId);
        var shardRoot = SetupShardRoot(factory);

        var calls = 0;
        shardRoot.SetAsync("k1", Arg.Any<byte[]>())
            .Returns<Task>(_ =>
            {
                calls++;
                if (calls == 1)
                    throw new InvalidOperationException("stale alias");
                return Task.CompletedTask;
            });

        await grain.SetAsync("k1", [1]);

        Assert.That(calls, Is.EqualTo(2),
            "A plain InvalidOperationException must still earn exactly one retry on the point-write path.");
    }

    [Test]
    public async Task SetManyAsync_fails_fast_instead_of_awaiting_the_slowest_branch()
    {
        // #3348's headline mechanism: the fan-out awaited every branch, so a
        // batch that one branch had already doomed still paid the slowest
        // branch. Here shard 0 refuses immediately and shard 1 never completes
        // at all - the unbounded form of "a rare 94-second event becomes the
        // cost of every write batch". Under the previous Task.WhenAll this call
        // could not return, so the assertion is deterministic in the passing
        // direction rather than a wall-clock threshold.
        const string treeId = "saturation-setmany-fail-fast";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);
        SetupCompactionGrain(factory, treeId);

        var map = new ShardMap { Slots = [0, 1], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));

        var shard0 = Substitute.For<IShardRootGrain>();
        var shard1 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>()).Returns(shard0);
        factory.GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>()).Returns(shard1);

        shard0.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns<Task>(_ => throw new LatticeSaturatedException(
                "WAL admission gate refused the append.", treeId));

        // The straggler. Never completes on its own.
        var straggler = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        shard1.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ => straggler.Task);

        var batch = new List<KeyValuePair<string, byte[]>>
        {
            new(FindKeyForSlot(0, 2), [1]),
            new(FindKeyForSlot(1, 2), [2]),
        };

        var call = grain.SetManyAsync(batch);

        // Generous, because it only bounds the FAILING direction; a correct
        // implementation settles this in microseconds.
        var settled = await Task.WhenAny(call, Task.Delay(TimeSpan.FromSeconds(15)));
        Assert.That(settled, Is.SameAs(call),
            "The fan-out is still awaiting a branch that has no bearing on the outcome. "
            + "The batch was already doomed when shard 0 refused.");

        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(async () => await call);
        Assert.That(thrown!.TreeId, Is.EqualTo(treeId),
            "Failing fast must not blur the typed refusal the back-off contract depends on.");

        // Release the straggler so the abandoned aggregate settles through the
        // observation path rather than being left to the finalizer.
        straggler.SetResult();
    }

    [Test]
    public async Task SetManyAsync_still_awaits_every_branch_when_none_fail()
    {
        // Control for the test above, and the invariant that bounds it: the
        // success path must NOT return early. SetManyAsync publishes one Set
        // event per entry only after every shard write has committed, so a
        // fan-out that returned on first completion would let a subscriber
        // observe a Set for a key that had not been persisted.
        const string treeId = "saturation-setmany-fail-fast-control";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);
        SetupCompactionGrain(factory, treeId);

        var map = new ShardMap { Slots = [0, 1], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));

        var shard0 = Substitute.For<IShardRootGrain>();
        var shard1 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>()).Returns(shard0);
        factory.GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>()).Returns(shard1);

        shard0.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        var slow = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        shard1.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ => slow.Task);

        var batch = new List<KeyValuePair<string, byte[]>>
        {
            new(FindKeyForSlot(0, 2), [1]),
            new(FindKeyForSlot(1, 2), [2]),
        };

        var call = grain.SetManyAsync(batch);

        var early = await Task.WhenAny(call, Task.Delay(TimeSpan.FromMilliseconds(500)));
        Assert.That(early, Is.Not.SameAs(call),
            "The fan-out returned before every branch had committed.");

        slow.SetResult();
        await call;

        await shard0.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        await shard1.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    private sealed class FanOutSentinelException(string message) : Exception(message);

    [Test]
    public async Task SetManyAsync_leaves_no_unobserved_fault_when_every_branch_settles_first()
    {
        // The fail-fast race has a corner that is easy to get wrong. When every
        // branch - the faulted one included - has already completed before
        // Task.WhenAny is evaluated, WhenAny resolves the already-settled pair
        // in ARGUMENT order and returns the aggregate, not the fault signal. The
        // signal is then faulted, already bypassed, and about to be dropped. If
        // nothing observes it, it resurfaces on finalization as an unobserved
        // task exception. This is the ordinary case for a mocked or co-located
        // shard, not an exotic one.
        const string treeId = "saturation-setmany-observed-fault";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);
        SetupCompactionGrain(factory, treeId);

        var map = new ShardMap { Slots = [0, 1], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));

        var shard0 = Substitute.For<IShardRootGrain>();
        var shard1 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>()).Returns(shard0);
        factory.GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>()).Returns(shard1);

        // Both settle synchronously, so the whole fan-out is complete before the
        // race is evaluated. A distinctive type keeps this assertion immune to
        // unobserved faults raised by any other fixture in the process.
        shard0.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns<Task>(_ => throw new FanOutSentinelException("branch refused"));
        shard1.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        var unobserved = 0;
        void OnUnobserved(object? _, UnobservedTaskExceptionEventArgs e)
        {
            if (e.Exception?.InnerExceptions.Any(x => x is FanOutSentinelException) != true)
                return;

            Interlocked.Increment(ref unobserved);
            e.SetObserved();
        }

        TaskScheduler.UnobservedTaskException += OnUnobserved;
        try
        {
            var batch = new List<KeyValuePair<string, byte[]>>
            {
                new(FindKeyForSlot(0, 2), [1]),
                new(FindKeyForSlot(1, 2), [2]),
            };

            // Asserting the throw first is what stops this guard going vacuous:
            // it cannot pass by never having run the fan-out at all.
            Assert.ThrowsAsync<FanOutSentinelException>(async () => await grain.SetManyAsync(batch));

            for (var i = 0; i < 3; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                await Task.Yield();
            }
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= OnUnobserved;
        }

        Assert.That(unobserved, Is.Zero,
            "The first-fault signal lost the race and was dropped without being observed. "
            + "It resurfaced on finalization as an unobserved task exception.");
    }
}
