using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalCommitLogWriterAdmissionSaturationTests
{
    [Test]
    [Category("Coyote")]
    public void Terminal_relief_choices_never_depend_on_a_saturation_or_pace_release()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new BackpressureReliefModel(), iterations: 100);
    }

    private sealed class BackpressureReliefModel : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var signal = Substitute.For<IWalSaturationSignal>();
            signal.GetCurrentState(TreeId).Returns(runtime.RandomBoolean()
                ? WalSaturationState.Saturated : WalSaturationState.Throttled);
            signal.WaitForHealthyAsync(TreeId, Arg.Any<CancellationToken>())
                .Returns(Task.FromCanceled(new CancellationToken(true)));
            var shard = CreateHealthyShard();
            var writer = CreateWriter(shard, signal, new LatticeOptions
            {
                WalPartitions = 1, WalMaxPendingBatches = 1, WalBatchedSingleEntryAppends = true,
            });
            var terminal = MakeMutation("0") with
            {
                Op = runtime.RandomBoolean() ? MutationKind.TxCommit : MutationKind.TxAbort,
                TransactionId = Guid.NewGuid(),
            };
            var batchSize = (int)runtime.RandomInteger(3);
            Task append = batchSize == 0
                ? writer.AppendAsync(terminal)
                : writer.AppendManyAsync(Enumerable.Repeat(terminal, batchSize).ToArray());
            Specification.Assert(append.IsCompletedSuccessfully,
                "[ReliefNeverGated/NoBackPressureCycle] terminal relief cannot await its own pressure release");
            Specification.Assert(!signal.ReceivedCalls().Any(),
                "[ReliefNeverGated] neither saturation admission nor throttle pacing may read a signal for relief");
            Specification.Assert(shard.ReceivedCalls().Any(call =>
                call.GetMethodInfo().Name is nameof(IWalShardGrain.AppendAsync) or nameof(IWalShardGrain.AppendBatchAsync)),
                "[ReliefNeverGated] ungated means actual terminal storage dispatch, not dropped work");
        }
    }
}
