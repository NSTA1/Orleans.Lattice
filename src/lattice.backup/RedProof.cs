namespace Orleans.Lattice.Backup;

// RED-PROOF: temporary switches that undo one part of the round-10 fixes each,
// so a CI run shows every new detector failing against the behaviour it pins.
// Set by the detectors themselves; removed with every switch site next commit.
internal static class RedProof
{
    internal static bool NoSyncRenew;
    internal static bool RenewErrorProceeds;
    internal static bool EndKeptReportsDeleted;
    internal static bool RenewRevives;

    internal static void Reset()
    {
        NoSyncRenew = false;
        RenewErrorProceeds = false;
        EndKeptReportsDeleted = false;
        RenewRevives = false;
    }
}
