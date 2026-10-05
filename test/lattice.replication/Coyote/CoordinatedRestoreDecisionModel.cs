using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Replication.Tests.Coyote;

/// <summary>
/// A Coyote model of a coordinated restore's all-or-nothing outcome across
/// three clusters, driving the <b>production</b>
/// <see cref="CrossClusterSagaDecisionCore"/> - the fold
/// <see cref="Grains.CrossClusterSagaCoordinatorGrain"/> routes its collected votes
/// through - under schedule exploration. It is the implementation-level companion
/// of <c>spec/backup/BackupRestore.tla</c>'s <c>RestoreAllOrNothing</c>.
/// <para>
/// Each participant builds its shadow and votes: <see cref="SagaVote.Commit"/>
/// when the build succeeds, <see cref="SagaVote.Abort"/> when its admission
/// probe or build fails, in which case it compensates itself at once. The
/// coordinator may be lost before deciding, which its prepare deadline turns into
/// an abort the participants learn by querying it on fence expiry (issue #4637). Votes arrive, the decision is delivered, and each
/// participant cuts over or compensates, all in an order the runtime picks.
/// </para>
/// <para>
/// The property: no cluster ever serves its restored copy while another has
/// compensated, at any step. When <c>brokenFold</c> is set the model replaces the
/// core with a fold that commits when ANY vote commits - the guard.
/// </para>
/// </summary>
internal sealed class CoordinatedRestoreDecisionModel(bool brokenFold) : ICoyoteModel
{
    private const int Clusters = 3;

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        var votes = new SagaVote[Clusters];
        var compensated = new bool[Clusters];
        var cutOver = new bool[Clusters];
        var voted = 0;

        // Prepare: each participant's vote lands in an explored order.
        var built = new bool[Clusters];
        while (voted < Clusters)
        {
            var c = Pick(runtime, built);
            built[c] = true;
            voted++;
            votes[c] = runtime.RandomBoolean() ? SagaVote.Commit : SagaVote.Abort;
            if (votes[c] != SagaVote.Commit)
            {
                compensated[c] = true;
            }

            AssertNeverMixed();
        }

        // Decide: the coordinator folds the votes, unless it is lost first.
        var coordinatorLost = runtime.RandomBoolean() && runtime.RandomBoolean();
        var commit = !coordinatorLost && (brokenFold
            ? Array.Exists(votes, v => v == SagaVote.Commit)
            : CrossClusterSagaDecisionCore.Decide(votes, out _));

        // Finalize: each participant applies the decision in an explored order.
        var finalized = new bool[Clusters];
        for (var step = 0; step < Clusters; step++)
        {
            var c = Pick(runtime, finalized);
            finalized[c] = true;
            if (commit && votes[c] == SagaVote.Commit)
            {
                cutOver[c] = true;
            }
            else if (!commit)
            {
                compensated[c] = true;
            }

            AssertNeverMixed();
        }

        void AssertNeverMixed()
        {
            var anyCut = Array.Exists(cutOver, x => x);
            var anyCompensated = Array.Exists(compensated, x => x);
            Specification.Assert(
                !(anyCut && anyCompensated),
                $"a coordinated restore left one cluster on its restored copy and another compensated (brokenFold={brokenFold})");
        }
    }

    private static int Pick(ICoyoteRuntime runtime, bool[] done)
    {
        var first = -1;
        for (var i = 0; i < done.Length; i++)
        {
            if (done[i])
            {
                continue;
            }

            if (first < 0)
            {
                first = i;
            }

            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return first;
    }
}
