namespace Orleans.Lattice.Backup;

// RED-PROOF: temporary switches that undo one part of the round-7 fixes each, so
// a CI run shows every new detector failing against the behaviour it pins. Set
// by the detectors themselves; removed with every switch site in the next commit.
internal static class RedProof
{
    internal static bool OwnerNoPending;
    internal static bool ReleaseClearsAll;
    internal static bool DeleteInGrain;
    internal static bool NoResume;
    internal static bool NoPendingLease;
    internal static bool NoTimeoutRetry;

    internal static void Reset()
    {
        OwnerNoPending = false;
        ReleaseClearsAll = false;
        DeleteInGrain = false;
        NoResume = false;
        NoPendingLease = false;
        NoTimeoutRetry = false;
    }
}
