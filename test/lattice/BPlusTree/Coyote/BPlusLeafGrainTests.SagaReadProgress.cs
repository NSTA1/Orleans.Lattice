using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    [Category("Coyote")]
    public void Sustained_writer_delivery_cannot_exhaust_the_actual_GetMany_reader()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new SagaReaderProgressModel(), iterations: 50);
    }

    private sealed class SagaReaderProgressModel : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var h = CreateGetManyProgressHarness();
            var expire = runtime.RandomBoolean();
            if (expire)
                h.DuringGatedRead = () => h.Clock.Advance(LatticeGrain.GetManyDecisionGateLease + TimeSpan.FromSeconds(1));
            try
            {
                var read = h.Lattice.GetManyAsync(h.Keys);
                Specification.Assert(read.IsCompleted, "[ReaderEventuallyResolves] completed deliveries must settle the caller");
                var values = read.GetAwaiter().GetResult();
                Specification.Assert(!expire && values.Count == 2 && values[h.Keys[0]].SequenceEqual(values[h.Keys[1]]),
                    "[ReadSnapshotConsistent] certify one whole batch only under a live gate");
            }
            catch (LatticeTransactionOutcomeUnavailableException)
            {
                Specification.Assert(expire, "[ReaderEventuallyAnswers] writer rate alone cannot exhaust retries");
            }
            Specification.Assert(h.OptimisticReads == 3 && h.RefusedCommits == 1 && !h.Gated,
                "[ReaderEventuallyAnswers] bounded fallback must reach and release the production gate");
        }
    }
}
