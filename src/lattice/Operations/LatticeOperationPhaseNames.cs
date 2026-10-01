namespace Orleans.Lattice.Operations;

/// <summary>Phase names the coordinated-operation engine itself assigns.</summary>
internal static class LatticeOperationPhaseNames
{
    /// <summary>The phase of an accepted operation its runner has not reported on yet.</summary>
    internal const string Queued = "Queued";

    /// <summary>The phase of a succeeded operation.</summary>
    internal const string Completed = "Completed";
}
