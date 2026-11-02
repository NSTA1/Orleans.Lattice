using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
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

        public Harness()
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
}
