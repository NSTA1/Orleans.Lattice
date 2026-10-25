using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Coyote;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.Wal;

public sealed partial class WalLogSubscriberTests
{
    [Test]
    [Category("Coyote")]
    public void Cooperative_view_drains_keep_stable_offered_load_bounded_in_every_pressure_arm()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new StableOfferedLoad(), iterations: 200);
    }

    private sealed class StableOfferedLoad : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var rows = new List<LatticeMutation> { Set(1), Set(2) };
            var reader = Substitute.For<ICommitLogReader>();
            reader.GetHeadOffsetAsync(Tree, 0, Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult((long)rows.Count));
            reader.GetTailOffsetAsync(Tree, 0, Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
            reader.ReadAsync(Tree, 0, Arg.Any<long>(), Arg.Any<CancellationToken>())
                .Returns(call => Read(rows, call.ArgAt<long>(2)));
            var subscriber = new WalLogSubscriber(reader, new InMemoryWalCursorRegistry());
            var checkpoints = new Dictionary<int, long> { [0] = -1 };
            var handler = new CollectingHandler();
            for (var opportunity = 0; opportunity < 12; opportunity++)
            {
                // One opportunity includes storage completion and any configured
                // deferral; this is not an arrival rate per timer tick.
                if (runtime.RandomBoolean()) rows.Add(Set(rows.Count + 1));
                var pressure = runtime.RandomBoolean() ? WalSaturationState.Saturated
                    : runtime.RandomBoolean() ? WalSaturationState.Throttled : WalSaturationState.Healthy;
                var batch = ViewBackpressure.ScaleBatch(pressure, 4, 0.5, 1);
                Specification.Assert(batch is >= 1 and <= 4, "[ConsumerBatchesRespond] positive bounded service");
                var result = Complete(subscriber.DrainAsync(Context(1, checkpoints, batchSize: batch), handler));
                Specification.Assert(result.EntriesRead <= batch, "[ViewPull] one bounded subscriber pass");
                if (result.AdvancedOffsets.TryGetValue(0, out var next)) checkpoints[0] = next;
                Specification.Assert(rows.Count - checkpoints[0] - 1 <= 2,
                    "[LagBounded] at most one offered row per successful minimum-one service opportunity");
            }

            for (var opportunity = 0; opportunity < rows.Count + 1; opportunity++)
            {
                var result = Complete(subscriber.DrainAsync(Context(1, checkpoints, batchSize: 1), handler));
                if (result.AdvancedOffsets.TryGetValue(0, out var next)) checkpoints[0] = next;
            }
            Specification.Assert(checkpoints[0] == rows.Count - 1,
                "[ConsumerCatchesUp] the finite no-arrival service tail reaches the exclusive head");
            Specification.Assert(handler.Entries.Select(entry => entry.Offset)
                .SequenceEqual(Enumerable.Range(0, rows.Count).Select(offset => (long)offset)),
                "[ConsumerCursorHonest] every consumed row is surfaced once in offset order");
        }

        private static T Complete<T>(Task<T> task)
        {
            Specification.Assert(task.IsCompleted, "collaborators must complete without blocking an uncontrolled task");
            return task.GetAwaiter().GetResult();
        }

        private static async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> Read(
            List<LatticeMutation> rows, long after)
        {
            await Task.CompletedTask;
            for (var offset = after + 1; offset < rows.Count; offset++)
                yield return (offset, rows[(int)offset]);
        }
    }
}
