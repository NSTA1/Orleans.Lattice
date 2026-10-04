namespace Orleans.Lattice.Replication;

/// <summary>
/// The dependency-free single global decision of a cross-cluster saga - the
/// coordinated restore among them: commit only when every participant voted
/// <see cref="SagaVote.Commit"/>, otherwise abort.
/// <see cref="Grains.CrossClusterSagaCoordinatorGrain"/> folds its collected votes
/// through it, and the coordinated-restore Coyote model drives the same method, so
/// the all-or-nothing rule the backup specification checks
/// (<c>spec/backup/BackupRestore.tla</c>, <c>Decide</c>) is the one that runs.
/// </summary>
internal static class CrossClusterSagaDecisionCore
{
    /// <summary>
    /// Folds the participants' votes into the single global decision.
    /// </summary>
    /// <param name="votes">One vote per participant, in participant order.</param>
    /// <param name="firstDissent">
    /// The index of the first vote that is not <see cref="SagaVote.Commit"/>, or
    /// <c>-1</c> when every vote commits.
    /// </param>
    /// <returns><c>true</c> to commit; <c>false</c> to abort.</returns>
    public static bool Decide(ReadOnlySpan<SagaVote> votes, out int firstDissent)
    {
        for (var i = 0; i < votes.Length; i++)
        {
            if (votes[i] != SagaVote.Commit)
            {
                firstDissent = i;
                return false;
            }
        }

        firstDissent = -1;
        return true;
    }
}
