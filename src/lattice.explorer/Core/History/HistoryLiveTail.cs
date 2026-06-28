using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// The filter-and-de-duplicate engine behind the History tab's live follow mode.
/// It scopes the forward change feed to a single key (the subscription itself is
/// one-tree-per-subscription, so the key match is applied client-side) and drops
/// any notification whose revision is already on the timeline.
/// </summary>
/// <remarks>
/// De-duplication keys on the revision's <see cref="HybridLogicalClock"/>, the
/// one identifier both a loaded history page row and a live notification carry
/// (a notification also exposes an opaque <see cref="StateChangeNotification.Position"/>,
/// but loaded page rows do not, so the clock is the only cross-source common
/// denominator). Seed the tail with the already-loaded rows so a retrospective
/// page and the live tail never double-count where they overlap. The seen-set is
/// a <see cref="HashSet{T}"/> so each notification is an O(1) lookup; no per-row
/// allocation occurs for a notification that fails the key filter.
/// </remarks>
public sealed class HistoryLiveTail
{
    private readonly HashSet<HybridLogicalClock> _seen = new();
    private readonly string _key;

    /// <summary>
    /// Creates a tail for <paramref name="key"/>, optionally seeding the
    /// de-duplication set with the clocks of the already-loaded revisions so the
    /// live tail does not re-surface a revision a loaded page already shows.
    /// </summary>
    /// <param name="key">The key whose live changes to follow.</param>
    /// <param name="alreadyLoaded">The revisions already on the timeline, or <see langword="null"/> to start with an empty set.</param>
    public HistoryLiveTail(string key, IEnumerable<HistoryRevisionRow>? alreadyLoaded = null)
    {
        ArgumentNullException.ThrowIfNull(key);
        _key = key;

        if (alreadyLoaded is null)
        {
            return;
        }

        foreach (var row in alreadyLoaded)
        {
            _seen.Add(row.Hlc);
        }
    }

    /// <summary>The key this tail is scoped to.</summary>
    public string Key => _key;

    /// <summary>The number of distinct revision clocks the tail has recorded.</summary>
    public int SeenCount => _seen.Count;

    /// <summary>
    /// Tests a live notification against the tail. Returns <see langword="true"/>
    /// and a fresh live-tail row when the notification targets the followed key
    /// and its clock has not been seen before; returns <see langword="false"/>
    /// (with <paramref name="row"/> set to <see langword="null"/>) when the
    /// notification is for a different key or is a duplicate of a revision already
    /// on the timeline.
    /// </summary>
    public bool TryAccept(StateChangeNotification notification, out HistoryRevisionRow? row)
    {
        ArgumentNullException.ThrowIfNull(notification);
        row = null;

        if (!Covers(notification, _key))
        {
            return false;
        }

        // De-duplicate on the hybrid-logical clock (a value-equatable struct, so
        // the seen-set needs no per-notification string key). The clock is the one
        // identifier both a loaded page row and a live notification carry.
        if (!_seen.Add(notification.Hlc))
        {
            return false;
        }

        row = HistoryRevisionRow.FromLive(notification);
        return true;
    }

    /// <summary>Records a revision clock as already-seen without producing a row (used when seeding from additional loaded pages).</summary>
    public void MarkSeen(HybridLogicalClock hlc) => _seen.Add(hlc);

    /// <summary>
    /// Whether a notification applies to <paramref name="key"/>: an exact key
    /// match, or a range delete whose half-open swept range
    /// <c>[Key, EndExclusiveKey)</c> contains the key.
    /// </summary>
    public static bool Covers(StateChangeNotification notification, string key)
    {
        ArgumentNullException.ThrowIfNull(notification);
        ArgumentNullException.ThrowIfNull(key);

        if (string.Equals(notification.Key, key, StringComparison.Ordinal))
        {
            return true;
        }

        if (notification.Kind == StateChangeKind.DeleteRange && notification.EndExclusiveKey is { } end)
        {
            return string.CompareOrdinal(notification.Key, key) <= 0
                && string.CompareOrdinal(key, end) < 0;
        }

        return false;
    }
}
