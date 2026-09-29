namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// An area's answer to "may this caller see you, now?" - visible, hidden, or
/// shown but unavailable with a reason.
/// </summary>
/// <remarks>
/// The default value is <see cref="Hidden"/>, so an answer that was never given
/// fails closed.
/// </remarks>
internal readonly record struct AreaAvailability
{
    private AreaAvailability(AreaAvailabilityKind kind, string? reason)
    {
        Kind = kind;
        Reason = reason;
    }

    /// <summary>The area is shown and can be opened.</summary>
    public static AreaAvailability Visible { get; } = new(AreaAvailabilityKind.Visible, null);

    /// <summary>The area is not shown at all.</summary>
    public static AreaAvailability Hidden { get; } = new(AreaAvailabilityKind.Hidden, null);

    /// <summary>Which of the three answers this is.</summary>
    public AreaAvailabilityKind Kind { get; }

    /// <summary>For <see cref="AreaAvailabilityKind.Unavailable"/>, the one-sentence reason; otherwise <see langword="null"/>.</summary>
    public string? Reason { get; }

    /// <summary>Whether the area appears in the directory at all.</summary>
    public bool IsShown => Kind != AreaAvailabilityKind.Hidden;

    /// <summary>The area is shown, demoted, and cannot be opened now, for <paramref name="reason"/>.</summary>
    /// <param name="reason">One plain sentence saying why, such as "Sign in to see backups."</param>
    /// <exception cref="ArgumentException"><paramref name="reason"/> is <see langword="null"/> or white space.</exception>
    public static AreaAvailability Unavailable(string reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return new AreaAvailability(AreaAvailabilityKind.Unavailable, reason);
    }
}
