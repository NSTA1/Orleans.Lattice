namespace Orleans.Lattice.Explorer.Shell.Navigation;

/// <summary>One shown stop on the directory spine: an area and its current availability.</summary>
/// <param name="Area">The area.</param>
/// <param name="Availability">
/// <see cref="AreaAvailabilityKind.Visible"/> or
/// <see cref="AreaAvailabilityKind.Unavailable"/>; hidden areas have no entry.
/// </param>
/// <param name="Badge">The short figure beside the stop, or <see langword="null"/>.</param>
internal sealed record ExplorerAreaEntry(IExplorerArea Area, AreaAvailability Availability, string? Badge = null)
{
    /// <summary>Whether the area can be opened now.</summary>
    public bool IsVisible => Availability.Kind == AreaAvailabilityKind.Visible;
}
