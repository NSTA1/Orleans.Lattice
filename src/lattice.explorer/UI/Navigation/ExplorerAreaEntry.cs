namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>One shown stop on the directory spine: an area and its current availability.</summary>
/// <param name="Area">The area.</param>
/// <param name="Availability">
/// <see cref="AreaAvailabilityKind.Visible"/>, <see cref="AreaAvailabilityKind.Unavailable"/>
/// or <see cref="AreaAvailabilityKind.Unchecked"/>; hidden areas have no entry.
/// </param>
/// <param name="Badge">The short figure beside the stop, or <see langword="null"/>.</param>
internal sealed record ExplorerAreaEntry(IExplorerArea Area, AreaAvailability Availability, string? Badge = null)
{
    /// <summary>Whether the area can be opened now.</summary>
    public bool IsVisible => Availability.Kind == AreaAvailabilityKind.Visible;

    /// <summary>Whether the cluster did not answer, and no earlier answer for this caller stands in.</summary>
    public bool IsUnchecked => Availability.Kind == AreaAvailabilityKind.Unchecked;

    /// <summary>
    /// Whether <see cref="Availability"/> is the last answer this caller was given,
    /// shown because the latest check met a transient fault. Only a shown answer is
    /// ever carried forward; a hidden one stays hidden.
    /// </summary>
    public bool IsStale { get; init; }

    /// <summary>Whether the directory is still waiting to check this area again.</summary>
    public bool NeedsRecheck => IsStale || IsUnchecked;
}
