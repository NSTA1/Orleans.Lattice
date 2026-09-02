namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// One tab strip as read in a single DOM snapshot: the name it publishes, the axis its
/// arrow keys run along, and how many of its tabs are operable.
/// </summary>
/// <remarks>
/// Snapshotting the strips and then addressing each by its published name is what keeps
/// a strip walk free of index races. The rail and the detail strip re-render whenever a
/// gate reports or a surface resolves, so an index resolved against one render can
/// address an element that no longer exists in the next.
/// </remarks>
internal sealed record StripSnapshot
{
    /// <summary>The strip's <c>aria-label</c>, which is also how it is addressed again.</summary>
    public string Label { get; init; } = string.Empty;

    /// <summary><see langword="true"/> when the strip runs vertically, so Up/Down move focus.</summary>
    public bool Vertical { get; init; }

    /// <summary>How many of the strip's tabs are not disabled.</summary>
    public int Operable { get; init; }
}

