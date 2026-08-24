namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Builds deterministic <see cref="HybridLogicalClock"/> stamps for the CRDT
/// convergence tests. Convergence is a property of the stamp order alone, so the
/// tests construct clocks by hand (never via wall-clock <c>Tick</c> / <c>Merge</c>
/// or timing) and drive merges directly, so they are deterministic and never
/// ordering- or delay-sensitive.
/// </summary>
internal static class TestClocks
{
    /// <summary>Builds a clock at the given wall-clock ticks and counter.</summary>
    internal static HybridLogicalClock Clock(long wallClockTicks, int counter = 0) =>
        new() { WallClockTicks = wallClockTicks, Counter = counter };
}
