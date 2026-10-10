using NSubstitute;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Pins how <see cref="ReplicationCrossTreeDecisionStamper"/> drives the
/// per-tree decision-sequence grains: the frontier registration first, then one
/// issue (or confirm) per participant, all in flight together.
/// </summary>
/// <remarks>
/// A call count cannot tell the concurrent fan-out from the serial loop it
/// replaced - both make one call per participant - so the discriminating
/// instrument is the peak number of calls simultaneously in flight. Each gate
/// releases only once every participant has arrived, so the serial loop cannot
/// complete at all.
/// </remarks>
[TestFixture]
public class ReplicationCrossTreeDecisionStamperTests
{
    private static readonly string[] Participants = ["tree-a", "tree-b", "tree-c", "tree-d"];

    private sealed class Harness
    {
        public readonly IGrainFactory Factory = Substitute.For<IGrainFactory>();
        public readonly ICrossTreePurgeFrontierSourceGrain Frontier = Substitute.For<ICrossTreePurgeFrontierSourceGrain>();
        public readonly Dictionary<string, ICrossTreeDecisionSequenceGrain> Sequences = new(StringComparer.Ordinal);
        public readonly TaskCompletionSource AllArrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Arrived;
        public int InFlight;
        public int Peak;
        public volatile bool Registered;
        public bool CalledBeforeRegistered;

        public Harness()
        {
            Frontier.RegisterTreesAsync(Arg.Any<IReadOnlyCollection<string>>())
                .Returns(_ =>
                {
                    Registered = true;
                    return Task.CompletedTask;
                });
            Factory.GetGrain<ICrossTreePurgeFrontierSourceGrain>(ICrossTreePurgeFrontierSourceGrain.Key, null)
                .Returns(Frontier);

            for (var i = 0; i < Participants.Length; i++)
            {
                var tree = Participants[i];
                var sequence = 100L + i;
                var grain = Substitute.For<ICrossTreeDecisionSequenceGrain>();
                grain.IssueAsync(Arg.Any<string>()).Returns(_ => EnterAsync(sequence));
                grain.ConfirmAsync(Arg.Any<string>()).Returns(_ => (Task)EnterAsync(0));
                Sequences[tree] = grain;
                Factory.GetGrain<ICrossTreeDecisionSequenceGrain>(tree, null).Returns(grain);
            }
        }

        private async Task<long> EnterAsync(long result)
        {
            if (!Registered)
            {
                CalledBeforeRegistered = true;
            }

            var now = Interlocked.Increment(ref InFlight);
            int seen;
            while (now > (seen = Volatile.Read(ref Peak)) &&
                   Interlocked.CompareExchange(ref Peak, now, seen) != seen)
            {
            }

            if (Interlocked.Increment(ref Arrived) == Participants.Length)
            {
                AllArrived.TrySetResult();
            }

            await AllArrived.Task.ConfigureAwait(false);
            Interlocked.Decrement(ref InFlight);
            return result;
        }
    }

    private static async Task<T> WithinDeadline<T>(Task<T> task, string because)
    {
        await WithinDeadline((Task)task, because);
        return await task;
    }

    private static async Task WithinDeadline(Task task, string because)
    {
        var finished = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.That(finished, Is.SameAs(task), because);
        await task;
    }

    [Test]
    public async Task IssueSequencesAsync_issues_every_participant_concurrently_after_registering_the_frontier()
    {
        var harness = new Harness();
        var stamper = new ReplicationCrossTreeDecisionStamper(harness.Factory);

        var sequences = await WithinDeadline(
            stamper.IssueSequencesAsync("op-1", Participants),
            "the per-tree sequence issues were not in flight together");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Peak, Is.EqualTo(Participants.Length));
            Assert.That(harness.CalledBeforeRegistered, Is.False);
            Assert.That(sequences, Is.EquivalentTo(new Dictionary<string, long>
            {
                ["tree-a"] = 100,
                ["tree-b"] = 101,
                ["tree-c"] = 102,
                ["tree-d"] = 103,
            }));
        });
        await harness.Frontier.Received(1).RegisterTreesAsync(Arg.Is<IReadOnlyCollection<string>>(t => t.SequenceEqual(Participants)));
        foreach (var grain in harness.Sequences.Values)
        {
            await grain.Received(1).IssueAsync("op-1");
        }
    }

    [Test]
    public async Task ConfirmSequencesAsync_confirms_every_participant_concurrently()
    {
        var harness = new Harness();
        harness.Registered = true;
        var stamper = new ReplicationCrossTreeDecisionStamper(harness.Factory);

        await WithinDeadline(
            stamper.ConfirmSequencesAsync("op-1", Participants),
            "the per-tree sequence confirms were not in flight together");

        Assert.That(harness.Peak, Is.EqualTo(Participants.Length));
        foreach (var grain in harness.Sequences.Values)
        {
            await grain.Received(1).ConfirmAsync("op-1");
        }
    }

    [Test]
    public void IssueSequencesAsync_propagates_a_participant_fault()
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ICrossTreePurgeFrontierSourceGrain>(ICrossTreePurgeFrontierSourceGrain.Key, null)
            .Returns(Substitute.For<ICrossTreePurgeFrontierSourceGrain>());
        var healthy = Substitute.For<ICrossTreeDecisionSequenceGrain>();
        healthy.IssueAsync(Arg.Any<string>()).Returns(Task.FromResult(7L));
        var faulted = Substitute.For<ICrossTreeDecisionSequenceGrain>();
        faulted.IssueAsync(Arg.Any<string>()).Returns(Task.FromException<long>(new InvalidOperationException("issue failed")));
        factory.GetGrain<ICrossTreeDecisionSequenceGrain>("tree-a", null).Returns(healthy);
        factory.GetGrain<ICrossTreeDecisionSequenceGrain>("tree-b", null).Returns(faulted);
        var stamper = new ReplicationCrossTreeDecisionStamper(factory);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => stamper.IssueSequencesAsync("op-1", ["tree-a", "tree-b"]));
        Assert.That(ex!.Message, Is.EqualTo("issue failed"));
    }

    [Test]
    public void ConfirmSequencesAsync_reports_invalid_arguments_through_the_returned_task()
    {
        var stamper = new ReplicationCrossTreeDecisionStamper(Substitute.For<IGrainFactory>());

        var pending = stamper.ConfirmSequencesAsync(string.Empty, Participants);

        Assert.That(pending.IsFaulted, Is.True);
        Assert.ThrowsAsync<ArgumentException>(() => pending);
    }
}
