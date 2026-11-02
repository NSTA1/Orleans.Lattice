namespace Orleans.Lattice.Backup;

// RED-PROOF: temporary switches that undo one part of the round-8 fixes each, so
// a CI run shows every new detector failing against the behaviour it pins. Set
// by the detectors themselves; removed with every switch site in the next commit.
internal static class RedProof
{
    internal static bool SharedDeleteLease;
    internal static bool EndIgnoresRow;
    internal static bool EndIgnoresScope;
    internal static bool BeginIgnoresScope;
    internal static bool ClaimsNotPersisted;

    internal static void Reset()
    {
        SharedDeleteLease = false;
        EndIgnoresRow = false;
        EndIgnoresScope = false;
        BeginIgnoresScope = false;
        ClaimsNotPersisted = false;
    }
}
