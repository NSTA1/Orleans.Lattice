namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

/// <summary>Whether a pending wrapper fails the run.</summary>
public static class PendingWrapperGate
{
    /// <summary>
    /// Off until I1 (#5400), which requires zero <see cref="KnownDefect"/> and zero
    /// <see cref="KnownGap"/> wrappers; until then the list is reported without failing.
    /// </summary>
    public static bool Hard => false;
}
