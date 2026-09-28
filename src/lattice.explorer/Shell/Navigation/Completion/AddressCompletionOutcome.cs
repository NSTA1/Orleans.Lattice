namespace Orleans.Lattice.Explorer.Shell.Navigation.Completion;

/// <summary>How one completion source answered.</summary>
internal enum AddressCompletionOutcome
{
    /// <summary>The source answered in time.</summary>
    Completed = 0,

    /// <summary>The source did not answer in time and was cancelled.</summary>
    TimedOut = 1,

    /// <summary>The source threw.</summary>
    Failed = 2,
}
