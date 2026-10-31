namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// An area's one-line Home status as the directory read it: the line itself, or
/// the last line this caller was given when the latest read met a transient
/// fault, or a sentence saying it could not be checked.
/// </summary>
internal sealed record ExplorerAreaStatus
{
    /// <summary>What Home shows when a status could not be read and none was read before.</summary>
    public const string UncheckedText = "Status could not be checked: the cluster did not answer.";

    /// <summary>A status could not be checked, and no earlier one stands in.</summary>
    public static ExplorerAreaStatus Unchecked { get; } = new() { Text = UncheckedText, IsUnchecked = true };

    /// <summary>The sentence to show.</summary>
    public required string Text { get; init; }

    /// <summary>Whether <see cref="Text"/> is the last line read, shown because the latest read met a transient fault.</summary>
    public bool IsStale { get; init; }

    /// <summary>Whether the status could not be checked at all, so <see cref="Text"/> says so.</summary>
    public bool IsUnchecked { get; init; }

    /// <summary>Whether the directory is still waiting to read the status again.</summary>
    public bool NeedsRecheck => IsStale || IsUnchecked;
}
