namespace Orleans.Lattice.Backup;

// RED-PROOF: temporary switches that undo one part of the round-9 fixes each, so
// a CI run shows every new detector failing against the behaviour it pins. Set
// by the detectors themselves; removed with every switch site in the next commit.
internal static class RedProof
{
    internal static bool RebuildUnprobed;
    internal static bool NoRenewal;
    internal static bool NoStopOnLoss;
    internal static bool LeaseNeverLapses;
    internal static bool ExpiryDropsEveryLease;
    internal static bool BeginMintsToken;
    internal static bool ClaimNeverCleared;
    internal static bool EndIgnoresRow;

    internal static void Reset()
    {
        RebuildUnprobed = false;
        NoRenewal = false;
        NoStopOnLoss = false;
        LeaseNeverLapses = false;
        ExpiryDropsEveryLease = false;
        BeginMintsToken = false;
        ClaimNeverCleared = false;
        EndIgnoresRow = false;
    }
}
