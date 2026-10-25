using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class AtomicWriteGrainTests
{
    [Test]
    [Category("Coyote")]
    public void Saga_delivery_opportunities_complete_the_actual_caller_after_decision_and_terminal()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new SagaCallerDeliveryModel(), iterations: 100);
    }

    private sealed class SagaCallerDeliveryModel : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var (grain, state, _, shard, registry) = CreateGrainForTerminalOrdering();
            var decisions = 0;
            var terminals = 0;
            registry.MarkCommittedAsync(Arg.Any<Guid>()).Returns(_ =>
            {
                decisions++;
                return Task.CompletedTask;
            });
            shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(),
                    Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
                .Returns(_ =>
                {
                    Specification.Assert(decisions > 0, "[SagaCompletionVisible] terminal follows durable decision");
                    terminals++;
                    return Task.FromResult<WalRecord?>(null);
                });
            var repeats = 1 + runtime.RandomInteger(4);
            for (var delivery = 0; delivery < repeats; delivery++)
            {
                // Completed collaborators make each public delivery a real
                // non-reentrant opportunity, never an uncontrolled Coyote await.
                var caller = grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));
                Specification.Assert(caller.IsCompletedSuccessfully,
                    "[SagaCompletionVisible] caller remains outstanding despite completed collaborators");
                caller.GetAwaiter().GetResult();
                Specification.Assert(state.State.Phase == AtomicWritePhase.Completed && terminals == 1 && decisions == 1,
                    "[CallerOnlyAfterTerminals] retry must acknowledge the same completed saga, not decide it again");
            }
        }
    }
}
