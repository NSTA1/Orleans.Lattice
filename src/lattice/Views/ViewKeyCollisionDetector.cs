namespace Orleans.Lattice;

/// <summary>
/// Detects re-key collisions in a batch of <see cref="ViewWrite"/>s: a view key
/// produced by two or more <b>distinct</b> source keys. For a filter /
/// re-project view the re-key must be injective, so a collision is a
/// configuration error rather than a legitimate many-to-one mapping (that
/// belongs to the aggregation view kind). The maintainer surfaces a detected
/// collision through a metric and a warning, then falls back to source-HLC
/// last-writer-wins so the view stays well-defined.
/// <para>
/// Detection is purely a function of the <see cref="ViewWrite.Key"/> /
/// <see cref="ViewWrite.SourceKey"/> attribution within a single drain batch.
/// A write whose <see cref="ViewWrite.SourceKey"/> is <see langword="null"/> is
/// not attributable to a single source key and is ignored. A view key written
/// repeatedly by the <i>same</i> source key (an update stream) is not a
/// collision.
/// </para>
/// </summary>
public static class ViewKeyCollisionDetector
{
    /// <summary>
    /// Returns the view keys in <paramref name="writes"/> that were produced by
    /// more than one distinct source key, in first-seen order. An empty result
    /// means the batch is collision-free.
    /// </summary>
    /// <param name="writes">The batch of writes to inspect. Must not be <see langword="null"/>.</param>
    public static IReadOnlyList<string> Detect(IEnumerable<ViewWrite> writes)
    {
        ArgumentNullException.ThrowIfNull(writes);

        // Track the first source key seen for each view key; on observing a
        // second distinct source key, record the view key as colliding (once).
        var firstSource = new Dictionary<string, string>(StringComparer.Ordinal);
        var colliding = new List<string>();
        var collidingSet = new HashSet<string>(StringComparer.Ordinal);

        foreach (var write in writes)
        {
            if (write.SourceKey is not { } source)
            {
                continue;
            }

            if (!firstSource.TryGetValue(write.Key, out var existing))
            {
                firstSource[write.Key] = source;
                continue;
            }

            if (!string.Equals(existing, source, StringComparison.Ordinal)
                && collidingSet.Add(write.Key))
            {
                colliding.Add(write.Key);
            }
        }

        return colliding;
    }
}
