namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// One snapshot of the area rail: the areas it offers as activable tabs, and how many
/// it has demoted below the divider because a gate refused them.
/// </summary>
/// <remarks>
/// Both halves are read in a single DOM evaluation, so they describe the same render.
/// Reading them separately would let a gate report in between and produce a pair that
/// never existed at any instant - which matters because the anti-vacuity guard reasons
/// about their sum.
/// </remarks>
internal sealed record AreaRailSnapshot
{
    /// <summary>The labels of the areas offered as activable tabs, in render order.</summary>
    public string[] Tabs { get; init; } = [];

    /// <summary>How many areas the rail demoted below the divider with a stated remedy.</summary>
    public int Demoted { get; init; }
}
