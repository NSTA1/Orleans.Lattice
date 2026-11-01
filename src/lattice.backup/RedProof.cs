namespace Orleans.Lattice.Backup;

// RED-PROOF: temporary switches that undo one part of the fix of #5418 / #5620
// each, so a CI run shows every detector failing against the behaviour it pins.
// Set by the detectors themselves; removed with every switch site in the next
// commit.
internal static class RedProof
{
    internal static bool PerCaptureIds;
    internal static bool BypassHolds;
    internal static bool NoRankRefresh;
    internal static bool NoClaim;
    internal static bool NoRetry;
    internal static bool Routing;
    internal static bool InPlaceWithdraw;

    internal static void Reset()
    {
        PerCaptureIds = false;
        BypassHolds = false;
        NoRankRefresh = false;
        NoClaim = false;
        NoRetry = false;
        Routing = false;
        InPlaceWithdraw = false;
    }
}
