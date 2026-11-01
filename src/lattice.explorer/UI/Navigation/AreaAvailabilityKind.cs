namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>Whether an area appears in the directory, and how.</summary>
internal enum AreaAvailabilityKind
{
    /// <summary>The area is not shown at all: its facade is absent or the caller may not see it.</summary>
    Hidden = 0,

    /// <summary>The area is shown and can be opened.</summary>
    Visible = 1,

    /// <summary>The area is shown, demoted, with the reason it cannot be opened now.</summary>
    Unavailable = 2,

    /// <summary>
    /// The area is shown, demoted, and cannot be opened, because the cluster did
    /// not answer whether this caller may see it (a transient fault or no answer in
    /// time). It is checked again; it is never an answer that admits the caller.
    /// </summary>
    Unchecked = 3,
}
