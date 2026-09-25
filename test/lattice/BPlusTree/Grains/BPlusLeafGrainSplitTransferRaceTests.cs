using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the leaf split changes behind issue #3523. A foreground
/// commit can overwrite a row between the split reading it into a transfer
/// batch and removing it, and the post-split digest publish can fault after
/// the division is durable. Each of these used to lose an acknowledged write:
/// the first by removing the newer value the sibling never received, the
/// second by throwing away the <see cref="SplitResult"/> that is the only way
/// the new sibling is ever linked.
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainSplitTransferRaceTests
{
    private sealed record Harness(
        BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        IBPlusLeafGrain Sibling,
        IBPlusInternalGrain Parent,
        List<Dictionary<string, LwwValue<byte[]>>> Merged);

    private static Harness CreateHarness(ICommitLogWriter? commitLog = null)
    {
        var state = new FakePersistentState<LeafNodeState>
        {
            State = { ParentId = GrainId.Create("internal", "transfer-race-parent") },
        };
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(Task.CompletedTask);
        sibling.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);

        var merged = new List<Dictionary<string, LwwValue<byte[]>>>();
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(ci =>
            {
                merged.Add(new Dictionary<string, LwwValue<byte[]>>(ci.Arg<Dictionary<string, LwwValue<byte[]>>>()));
                return Task.FromResult<SplitResult?>(null);
            });

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "transfer-race-leaf"));
        if (commitLog is not null)
        {
            var services = Substitute.For<IServiceProvider>();
            services.GetService(typeof(ICommitLogWriter)).Returns(commitLog);
            context.ActivationServices.Returns(services);
            state.State.TreeId = "transfer-race-tree";
        }

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sibling);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(sibling);
        var parent = Substitute.For<IBPlusInternalGrain>();
        grainFactory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(parent);
        var resolver = TestOptionsResolver.Create(maxLeafKeys: 3, shardCount: 1, factory: grainFactory);
        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            resolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        return new Harness(grain, state, sibling, parent, merged);
    }

    private static async Task FillToCapacityAsync(BPlusLeafGrain grain)
    {
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
    }

    private static string? LastSentValue(Harness harness, string key)
    {
        string? last = null;
        foreach (var batch in harness.Merged)
        {
            if (batch.TryGetValue(key, out var lww))
            {
                last = lww.Value is null ? null : Encoding.UTF8.GetString(lww.Value);
            }
        }

        return last;
    }

    /// <summary>
    /// A commit-log writer whose first append after <paramref name="arm"/> runs
    /// parks until <paramref name="release"/> completes, as a foreground commit's
    /// WAL append does while a split interleaves with it. Every other append
    /// passes straight through.
    /// </summary>
    private static ICommitLogWriter CreateParkingWriter(
        out Action arm, out TaskCompletionSource parked, out TaskCompletionSource release)
    {
        var armed = false;
        var parkedSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        arm = () => armed = true;
        parked = parkedSource;
        release = releaseSource;

        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                if (armed)
                {
                    armed = false;
                    parkedSource.TrySetResult();
                    await releaseSource.Task;
                }

                return 0L;
            });
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IReadOnlyList<long>>(new long[((IReadOnlyList<WalRecord>)ci[0]).Count]));
        return writer;
    }

    [Test]
    public async Task Split_carries_a_row_overwritten_during_its_transfer_to_the_sibling()
    {
        var writer = CreateParkingWriter(out var arm, out var parked, out var release);
        var harness = CreateHarness(writer);
        await FillToCapacityAsync(harness.Grain);

        // A foreground overwrite of "c" is admitted before the split starts and
        // parks in its WAL append, past every split-in-progress check.
        arm();
        var overwrite = harness.Grain.SetAsync("c", Encoding.UTF8.GetBytes("3-newer"));
        await parked.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Its append completes while the batch holding "c" is in flight to the
        // sibling: after the split read the row, before it removed it.
        var released = false;
        harness.Sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(new Func<NSubstitute.Core.CallInfo, Task<SplitResult?>>(async ci =>
            {
                var batch = ci.Arg<Dictionary<string, LwwValue<byte[]>>>();
                harness.Merged.Add(new Dictionary<string, LwwValue<byte[]>>(batch));
                if (!released && batch.ContainsKey("c"))
                {
                    released = true;
                    release.TrySetResult();
                    await overwrite.WaitAsync(TimeSpan.FromSeconds(10));
                }

                return null;
            }));

        var result = await harness.Grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        Assert.That(result, Is.Not.Null, "the overflowing write must split");
        Assert.That(string.CompareOrdinal(result!.PromotedKey, "c"), Is.LessThanOrEqualTo(0),
            "precondition: the overwritten key moves to the sibling");
        Assert.That(released, Is.True, "precondition: the overwrite raced the transfer");
        Assert.Multiple(() =>
        {
            Assert.That(LastSentValue(harness, "c"), Is.EqualTo("3-newer"),
                "the acknowledged newer value must reach the sibling that now owns the key");
            Assert.That(harness.Grain.EntriesForTest.ContainsKey("c"), Is.False,
                "nothing at or above the split key may stay where no read is routed");
            Assert.That(harness.State.State.HighKeyExclusive, Is.EqualTo(result.PromotedKey));
        });
    }

    [Test]
    public async Task Split_removes_transferred_rows_that_did_not_change_during_the_transfer()
    {
        var harness = CreateHarness();
        await FillToCapacityAsync(harness.Grain);

        var result = await harness.Grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        Assert.That(result, Is.Not.Null);
        var sentKeys = harness.Merged.SelectMany(b => b.Keys).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(sentKeys, Is.Unique, "an unchanged row is transferred once, with no straggler pass");
            Assert.That(sentKeys, Is.All.GreaterThanOrEqualTo(result!.PromotedKey).Using<string>(StringComparer.Ordinal));
        });
    }

    [Test]
    public async Task Split_returns_its_result_when_the_post_split_digest_publish_faults()
    {
        var harness = CreateHarness();
        await FillToCapacityAsync(harness.Grain);
        harness.Parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new TimeoutException("parent parked"));

        var result = await harness.Grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        Assert.That(result, Is.Not.Null,
            "the SplitResult is the only way the new sibling is linked, so a publish fault must not discard it");
        await harness.Parent.Received().OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    // --- (#3523) the trailing per-write digest publish after a split ---
    //
    // Every write funnel publishes the write's digest once more after its split
    // check. That publish also runs after the division is durable, so a fault
    // from it used to discard the SplitResult exactly as a fault from the split's
    // own publish did. Perturbation: make PublishDigestUpwardAfterWriteAsync
    // always await PublishDigestUpwardAsync and every *_returns_its_split test
    // below fails with the parent's TimeoutException.

    private static void FaultParentDigestPublishes(Harness harness) =>
        harness.Parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new TimeoutException("parent parked"));

    private static byte[] OrSetAddDelta(string element) =>
        JsonLatticeSerializer<OrSetDelta>.Default.Serialize(new OrSetDelta
        {
            Adds = new[]
            {
                new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes(element), ReplicaId = "r1", Counter = 1 },
            },
            Removes = Array.Empty<OrSetDeltaDot>(),
        });

    /// <summary>
    /// A commit-log writer that, once armed, narrows the leaf's declared span to
    /// <c>[.., "m")</c> on its next append, as a split interleaving with the
    /// append would. The state is bound late because the harness creates it.
    /// </summary>
    private static ICommitLogWriter CreateSpanNarrowingWriter(
        Func<FakePersistentState<LeafNodeState>> state, out Action arm)
    {
        var armed = false;
        arm = () => armed = true;
        void Narrow()
        {
            if (!armed) return;
            armed = false;
            state().State.HighKeyExclusive = "m";
            state().State.NextSibling = GrainId.Create("leaf", "narrowed-successor");
        }

        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                Narrow();
                return Task.FromResult(0L);
            });
        return writer;
    }

    [Test]
    public void Write_without_a_split_still_surfaces_a_digest_publish_fault()
    {
        var harness = CreateHarness();
        FaultParentDigestPublishes(harness);

        Assert.ThrowsAsync<TimeoutException>(async () =>
            await harness.Grain.SetAsync("a", Encoding.UTF8.GetBytes("1")),
            "containment is scoped to writes that split; a plain write keeps its fault semantics");
    }

    [Test]
    public async Task SetManyAsync_returns_its_split_when_the_digest_publishes_fault()
    {
        var harness = CreateHarness();
        FaultParentDigestPublishes(harness);

        var result = await harness.Grain.SetManyAsync(new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
            new("c", Encoding.UTF8.GetBytes("3")),
            new("d", Encoding.UTF8.GetBytes("4")),
        });

        Assert.That(result, Is.Not.Null);
        await harness.Parent.Received().OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task ApplyCrdtDeltaAsync_returns_its_split_when_the_digest_publishes_fault()
    {
        var harness = CreateHarness(new FakeCommitLogWriter());
        foreach (var key in new[] { "a", "b", "c" })
        {
            await harness.Grain.ApplyCrdtDeltaAsync(key, LatticeMergeMode.OrSet, OrSetAddDelta(key));
        }

        FaultParentDigestPublishes(harness);

        var result = await harness.Grain.ApplyCrdtDeltaAsync("d", LatticeMergeMode.OrSet, OrSetAddDelta("d"));

        Assert.That(result.Split, Is.Not.Null);
        await harness.Parent.Received().OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task ApplyCrdtDeltaManyAsync_returns_its_split_when_the_digest_publishes_fault()
    {
        var harness = CreateHarness(new FakeCommitLogWriter());
        FaultParentDigestPublishes(harness);
        var deltas = new List<KeyValuePair<string, byte[]>>();
        foreach (var key in new[] { "a", "b", "c", "d" })
        {
            deltas.Add(new(key, OrSetAddDelta(key)));
        }

        var result = await harness.Grain.ApplyCrdtDeltaManyAsync(deltas, LatticeMergeMode.OrSet);

        Assert.That(result, Is.Not.Null);
        await harness.Parent.Received().OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task DeleteRangeAsync_returns_its_relocation_split_when_the_digest_publish_faults()
    {
        Harness? bound = null;
        var writer = CreateSpanNarrowingWriter(() => bound!.State, out var arm);
        var harness = bound = CreateHarness(writer);
        await harness.Grain.SetAsync("b", Encoding.UTF8.GetBytes("b"));
        await harness.Grain.SetAsync("p", Encoding.UTF8.GetBytes("p"));
        var relocationSplit = new SplitResult
        {
            PromotedKey = "q",
            NewSiblingId = GrainId.Create("leaf", "relocation-split"),
            ChildIsLeaf = true,
        };
        harness.Sibling.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>())
            .Returns(Task.FromResult<SplitResult?>(relocationSplit));
        FaultParentDigestPublishes(harness);
        arm();

        var result = await harness.Grain.DeleteRangeAsync("a", "z");

        Assert.Multiple(() =>
        {
            Assert.That(result.Deleted, Is.EqualTo(2));
            Assert.That(result.Split?.PromotedKey, Is.EqualTo("q"),
                "the relocation's split must still reach the shard root to be linked");
        });
        await harness.Parent.Received().OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }
}
