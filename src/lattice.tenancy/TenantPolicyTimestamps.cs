namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Saturating arithmetic over <see cref="TimeProvider.GetTimestamp"/> values, shared
/// by the tenant-policy epoch ledger and the per-silo lease it grants.
/// </summary>
internal static class TenantPolicyTimestamps
{
    /// <summary>
    /// Returns <paramref name="timestamp"/> advanced by <paramref name="duration"/>
    /// on <paramref name="time"/>'s timestamp scale, saturating at
    /// <see cref="long.MaxValue"/> instead of overflowing.
    /// </summary>
    /// <param name="time">The clock whose timestamp frequency applies.</param>
    /// <param name="timestamp">The starting timestamp.</param>
    /// <param name="duration">The non-negative duration to add.</param>
    /// <returns>The advanced timestamp.</returns>
    public static long Add(TimeProvider time, long timestamp, TimeSpan duration)
    {
        var ticks = duration.TotalSeconds * time.TimestampFrequency;
        return ticks >= (double)(long.MaxValue - timestamp) ? long.MaxValue : timestamp + (long)ticks;
    }
}
