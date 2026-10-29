namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Holder for the test-only atomic-commit trace seam used by the
/// trace-validation spike (<c>spec/atomic-commit/TraceValidation.md</c>).
/// <para>
/// ZERO COST WHEN UNUSED. Every emission site reads
/// <see cref="SinkForTest"/> and calls it with the null-conditional operator,
/// so when no sink is installed - always, in production - an emission is one
/// static field load and a null test: its arguments are never evaluated,
/// nothing is allocated and no behaviour changes.
/// </para>
/// </summary>
internal static class AtomicCommitTrace
{
    /// <summary>
    /// The installed trace receiver. Always <see langword="null"/> in production;
    /// set only by the trace-validation prototype test, which owns its lifetime.
    /// </summary>
    internal static IAtomicCommitTraceSink? SinkForTest;
}
