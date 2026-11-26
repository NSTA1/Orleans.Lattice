namespace Orleans.Lattice;

/// <summary>
/// Receives the consumers <see cref="InMemoryWalCursorRegistry.VisitReportedSince{TVisitor}"/>
/// walks, in place, under the registry's lock (issue #6166). Implemented by a struct, so a
/// visit copies no snapshot and allocates nothing; <see cref="Visit"/> must not call back
/// into the registry.
/// </summary>
internal interface IWalCursorVisitor
{
    /// <summary>Visits one consumer's current snapshot.</summary>
    /// <param name="snapshot">The consumer's snapshot, read in place.</param>
    void Visit(in WalCursorSnapshot snapshot);
}
