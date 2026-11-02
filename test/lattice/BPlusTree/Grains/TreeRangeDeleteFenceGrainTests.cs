using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit detectors for <see cref="TreeRangeDeleteFenceGrain"/> (epic #5605, member 2;
/// design note section 2): a generation is claimed at every partition before it is
/// handed out; a close marks its intent closing, durably, before its first re-claim,
/// and from then on the open check answers "not open" (rule 1,
/// <c>ReadPositionHonestRestampDuringClose</c>); a close raises the generation once
/// and records the same heads on every shard's marker; the reminder closes an intent
/// past its deadline.
/// </summary>
[TestFixture]
public sealed class TreeRangeDeleteFenceGrainTests
{
    private const string Tree = "rd-tree";

    private sealed class Harness
    {
        public FakePersistentState<TreeRangeDeleteFenceState> State { get; } = new();

        public IWalShardGrain Wal { get; } = Substitute.For<IWalShardGrain>();

        public IShardRootGrain Shard { get; } = Substitute.For<IShardRootGrain>();

        public List<long> Claimed { get; } = [];

        public TaskCompletionSource? ClaimGate { get; set; }

        public long Head { get; set; } = 7;

        public TreeRangeDeleteFenceGrain Grain { get; }

        public Harness(IGrainStorage? storage = null)
        {
            var factory = Substitute.For<IGrainFactory>();
            factory.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(Wal);
            factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(Shard);
            Wal.ClaimRoutingGenerationAsync(WalRoutingInitiator.TreeRangeDelete, Arg.Any<long>(), Arg.Any<CancellationToken>())
                .Returns(async call =>
                {
                    lock (Claimed)
                    {
                        Claimed.Add(call.ArgAt<long>(1));
                    }

                    if (ClaimGate is { } gate)
                    {
                        await gate.Task;
                    }

                    return WalAppendFenceClaimResult.Accept(Head);
                });

            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("rdfence", Tree));
            if (storage is not null)
            {
                var services = new ServiceCollection();
                services.AddKeyedSingleton(LatticeOptions.StorageProviderName, storage);
                context.ActivationServices.Returns(services.BuildServiceProvider());
            }

            Grain = new TreeRangeDeleteFenceGrain(
                context,
                factory,
                Substitute.For<IReminderRegistry>(),
                TestOptionsResolver.Create(factory: factory),
                NullLogger<TreeRangeDeleteFenceGrain>.Instance,
                State);
        }
    }

    [Test]
    public async Task The_first_intent_is_handed_a_generation_claimed_at_every_partition()
    {
        var harness = new Harness();

        var generation = await harness.Grain.OpenIntentAsync(Guid.NewGuid(), "a", "b", long.MaxValue, [0, 1]);

        Assert.Multiple(() =>
        {
            Assert.That(generation, Is.EqualTo(1));
            Assert.That(harness.Claimed, Is.Not.Empty.And.All.EqualTo(1L), "claimed before it is handed out");
            Assert.That(harness.State.State.ClaimedGeneration, Is.EqualTo(1));
        });
        await harness.Shard.Received(2).OpenHandOffMarkerAsync(Arg.Is<string>(t => t.StartsWith("rd:")), "a", "b");
    }

    [Test]
    public async Task A_close_marks_its_intent_closing_durably_before_its_first_re_claim()
    {
        var harness = new Harness();
        var intent = Guid.NewGuid();
        await harness.Grain.OpenIntentAsync(intent, "a", "b", long.MaxValue, [0]);
        harness.Claimed.Clear();
        harness.ClaimGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writtenClosing = false;
        harness.State.OnWriteState = s =>
        {
            if (s.Intents.TryGetValue(intent, out var written) && written.Closing)
            {
                Volatile.Write(ref writtenClosing, true);
            }
        };

        var close = harness.Grain.CloseIntentAsync(intent);
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (harness.Claimed.Count == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(5);
        }

        // The close is between its first re-claim and its marker close.
        var openCheck = await harness.Grain.GetOpenIntentGenerationAsync(intent);
        // Durably: a write carrying the closing intent landed before the re-claim.
        var persistedClosing = Volatile.Read(ref writtenClosing);
        harness.ClaimGate.SetResult();
        await close;

        Assert.Multiple(() =>
        {
            Assert.That(harness.Claimed, Is.All.EqualTo(2L), "the close re-claims a generation above the one handed out");
            Assert.That(persistedClosing, Is.True, "rule 1: the intent is closing before the close's first re-claim");
            Assert.That(openCheck, Is.Null,
                "ReadPositionHonestRestampDuringClose: an open check during the close must not hand out the post-close generation");
            Assert.That(harness.State.State.Intents, Is.Empty);
        });
        await harness.Shard.Received(1).CloseHandOffMarkerAsync(Arg.Is<string>(t => t.StartsWith("rd:")), Arg.Is<long[]>(h => h.All(x => x == 7)));
    }

    [Test]
    public async Task One_close_fences_every_open_intent_and_an_open_intent_re_stamps_with_the_new_generation()
    {
        var harness = new Harness();
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        var handedOut = await harness.Grain.OpenIntentAsync(first, "a", "m", long.MaxValue, [0]);
        await harness.Grain.OpenIntentAsync(second, "f", "z", long.MaxValue, [0]);

        await harness.Grain.CloseIntentAsync(first);
        var closed = await harness.Grain.GetOpenIntentGenerationAsync(first);
        var stillOpen = await harness.Grain.GetOpenIntentGenerationAsync(second);

        Assert.Multiple(() =>
        {
            Assert.That(closed, Is.Null, "a closed intent re-stamps nothing");
            Assert.That(stillOpen, Is.EqualTo(handedOut + 1),
                "the other delete's walks re-stamp with the generation the close re-claimed");
        });
    }

    [Test]
    public async Task The_reminder_closes_an_intent_past_its_deadline_and_keeps_one_inside_it()
    {
        var harness = new Harness();
        var expired = Guid.NewGuid();
        var live = Guid.NewGuid();
        await harness.Grain.OpenIntentAsync(expired, "a", "b", DateTime.UtcNow.AddMinutes(-1).Ticks, [0]);
        await harness.Grain.OpenIntentAsync(live, "c", "d", DateTime.UtcNow.AddMinutes(10).Ticks, [0]);

        await harness.Grain.ReceiveReminder(TreeRangeDeleteFenceGrain.ReminderName, default);

        Assert.That(harness.State.State.Intents.Keys, Is.EquivalentTo(new[] { live }),
            "a stateless caller that died mid fan-out is closed at its recorded deadline");
    }

    [Test]
    public async Task A_refused_claim_raises_the_generation_above_the_partition_and_claims_again()
    {
        var harness = new Harness();
        harness.Wal.ClaimRoutingGenerationAsync(WalRoutingInitiator.TreeRangeDelete, 1, Arg.Any<CancellationToken>())
            .Returns(WalAppendFenceClaimResult.Refuse(5));

        var generation = await harness.Grain.OpenIntentAsync(Guid.NewGuid(), "a", "b", long.MaxValue, [0]);

        Assert.That(generation, Is.EqualTo(6), "never hand out a generation a partition already holds above");
    }

    [Test]
    public async Task A_fence_of_a_purged_tree_id_seeds_its_first_generation_above_the_floor()
    {
        // #5668: starting at 1 would let a purged incarnation's generation recur.
        var storage = new EnumerableMemoryGrainStorage();
        await WalAppendFenceRows.RaiseRoutingFloorAsync(storage, Tree, 6);
        var harness = new Harness(storage);

        var generation = await harness.Grain.OpenIntentAsync(Guid.NewGuid(), "a", "b", long.MaxValue, [0]);

        Assert.Multiple(() =>
        {
            Assert.That(generation, Is.EqualTo(7));
            Assert.That(harness.Claimed, Is.Not.Empty.And.All.EqualTo(7L), "the seeded generation is the one claimed");
        });
    }

    [Test]
    public async Task EnsureInitialised_claims_the_seeded_generation_everywhere_and_returns_the_heads()
    {
        var storage = new EnumerableMemoryGrainStorage();
        await WalAppendFenceRows.RaiseRoutingFloorAsync(storage, Tree, 3);
        var harness = new Harness(storage) { Head = 11 };

        var heads = await harness.Grain.EnsureInitialisedAsync();
        var again = await harness.Grain.EnsureInitialisedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(heads, Is.Not.Empty.And.All.EqualTo(11L), "one head per partition, as a close records them");
            Assert.That(again, Is.EqualTo(heads), "idempotent");
            Assert.That(harness.Claimed, Is.All.EqualTo(4L), "the seeded generation, re-claimed rather than raised");
            Assert.That(harness.State.State.Generation, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task RetireForPurge_raises_the_floor_over_every_generation_then_clears_the_state()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var harness = new Harness(storage);
        await harness.Grain.OpenIntentAsync(Guid.NewGuid(), "a", "b", long.MaxValue, [0]);
        harness.State.State.Generation = 5;

        await harness.Grain.RetireForPurgeAsync();
        var floor = (await WalAppendFenceRows.ReadRoutingFloorAsync(storage, Tree)).State.Floor;
        var next = await harness.Grain.EnsureInitialisedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(floor, Is.EqualTo(5), "a generation a close raised but never claimed is covered too");
            Assert.That(harness.State.State.Generation, Is.EqualTo(6), "the state was cleared, and re-seeded above the floor");
            Assert.That(harness.State.State.Intents, Is.Empty, "the purged incarnation's intents are dropped");
            Assert.That(next, Is.Not.Empty);
        });
    }
}