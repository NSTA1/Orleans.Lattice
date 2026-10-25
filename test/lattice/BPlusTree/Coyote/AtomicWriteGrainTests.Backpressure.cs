using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class AtomicWriteGrainTests
{
    [Test]
    [Category("Coyote")]
    public void Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new BackpressureQuiesceModel(), iterations: 100);
    }

    [Test]
    [Category("Coyote")]
    public void Terminal_resumption_choices_never_wait_for_pressure()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new BackpressureTerminalResumptionModel(), iterations: 100);
    }

    private sealed class BackpressureTerminalResumptionModel : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var commit = runtime.RandomBoolean();
            var pressure = (WalSaturationState)runtime.RandomInteger(3);
            AssertTerminalResumptionAsync(commit, pressure).GetAwaiter().GetResult();
        }
    }

    private sealed class BackpressureQuiesceModel : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var fallback = runtime.RandomBoolean();
            var saturatedPartition = runtime.RandomBoolean() ? 0 : 1;
            var initialPressure = (WalSaturationState)runtime.RandomInteger(3);
            var recovery = (WalSaturationState)runtime.RandomInteger(3);
            const string key = "backpressure-coyote";
            var capturedPartition = WalPartitionHash.Compute(key, 2);
            var realSignal = new WalSaturationSignal();
            realSignal.UpdateState(TreeId, initialPressure);
            realSignal.UpdatePartitionState(TreeId, saturatedPartition, initialPressure);
            var signal = fallback
                ? Substitute.For<IWalSaturationSignal>()
                : Substitute.For<IWalSaturationSignal, IWalPartitionSaturationSignal>();
            signal.GetCurrentState(TreeId).Returns(_ => realSignal.GetCurrentState(TreeId));
            var waited = false;
            Task Expire()
            {
                waited = true;
                realSignal.UpdateState(TreeId, recovery);
                realSignal.UpdatePartitionState(TreeId, saturatedPartition, recovery);
                return Task.FromCanceled(new CancellationToken(true));
            }
            signal.WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Expire());
            if (signal is IWalPartitionSaturationSignal partitions)
            {
                partitions.GetCurrentState(TreeId, Arg.Any<int>())
                    .Returns(call => realSignal.GetCurrentState(TreeId, (int)call[1]));
                partitions.WaitForHealthyAsync(TreeId, Arg.Any<int>(), Arg.Any<CancellationToken>())
                    .Returns(_ => Expire());
            }
            var (grain, state, lattice, _) = CreateGrainWithSignal(signal,
                options: new LatticeOptions { WalPartitions = 2, TxRegistryShardCount = 1 });
            lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.CompletedTask);
            var entries = MakeEntries((key, [1]));
            var expectedWait = initialPressure == WalSaturationState.Saturated
                && (fallback || capturedPartition == saturatedPartition);
            LatticeSaturatedException? refusal = null;
            try
            {
                grain.ExecuteAsync(TreeId, entries).GetAwaiter().GetResult();
            }
            catch (LatticeSaturatedException error)
            {
                refusal = error;
            }

            Specification.Assert(waited == expectedWait,
                "[SagaScopedToCapturedPartitions] an unrelated partition cannot quiesce a captured saga");
            Specification.Assert((refusal is not null) == (expectedWait && recovery == WalSaturationState.Saturated),
                "[NoSpuriousRefusal] expiry must re-read, including recovery only as far as Throttled");
            if (refusal is not null)
            {
                Specification.Assert(refusal.SaturationSource == LatticeSaturationSource.AtomicWriteSaga,
                    "[RefusalSourceAttributed] saga refusal must identify the saga gate");
                Specification.Assert(state.State.Phase == AtomicWritePhase.Execute && state.State.NextIndex == 0,
                    "[SagaRefusalPreservesProgress] refusal preserves Execute(NextIndex), not compensation");
                Specification.Assert(!lattice.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(ILattice.SetManyAsync)),
                    "[SagaRefusalPreservesProgress] a still-saturated saga must not dispatch or compensate");
                realSignal.UpdateState(TreeId, WalSaturationState.Healthy);
                realSignal.UpdatePartitionState(TreeId, saturatedPartition, WalSaturationState.Healthy);
                grain.ExecuteAsync(TreeId, entries).GetAwaiter().GetResult();
            }
            Specification.Assert(lattice.ReceivedCalls().Count(call =>
                call.GetMethodInfo().Name == nameof(ILattice.SetManyAsync)) == 1,
                "[SagaEventuallyCompletesOrRefuses] a recovered retry resumes exactly one dispatch");
            realSignal.ResetForTesting();
        }
    }
}
