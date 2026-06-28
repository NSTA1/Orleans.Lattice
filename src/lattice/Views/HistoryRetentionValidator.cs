namespace Orleans.Lattice.Views;

/// <summary>
/// Validates a durable-history retention override before it is persisted to the
/// tree registry. The constraints mirror the documented contract: the retention
/// <em>window</em> must be strictly positive when supplied (a zero or negative
/// window is expressed by clearing the override, not by storing it), and the
/// retention <em>mode</em> must be a defined <see cref="HistoryRetentionMode"/>.
/// </summary>
internal static class HistoryRetentionValidator
{
    /// <summary>
    /// Throws when <paramref name="window"/> is supplied but not strictly
    /// positive, or when <paramref name="mode"/> is supplied but not a defined
    /// enum value. A <see langword="null"/> argument clears that half of the
    /// override and is always valid.
    /// </summary>
    /// <param name="mode">The mode to set, or <see langword="null"/> to clear it.</param>
    /// <param name="window">The window to set, or <see langword="null"/> to clear it.</param>
    public static void Validate(HistoryRetentionMode? mode, TimeSpan? window)
    {
        if (window is { } w && w <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(window),
                w,
                "The durable-history retention window must be strictly positive; pass null to clear the override (no age bound).");
        }

        if (mode is { } m && !Enum.IsDefined(m))
        {
            throw new ArgumentOutOfRangeException(
                nameof(mode),
                m,
                "The durable-history retention mode must be a defined HistoryRetentionMode value.");
        }
    }
}
