namespace Orleans.Lattice.Views;

/// <summary>
/// Silo-wide admission for the reclamation of superseded view-tree generations
/// (issue #6097). Every view maintainer on a silo draws from this one pacer
/// before it inspects or deletes a superseded generation, so a backlog of
/// generations that earlier releases left behind - often several per view, and
/// all of them discovered together when the silo starts - is retired one pass
/// at a time rather than as a burst of tree deletions.
/// </summary>
/// <remarks>
/// <para>
/// A pass costs at most one tree deletion (which marks the tree's shards,
/// scans the registry for aliases, and replaces the tree's autonomic reminders
/// with one purge reminder) plus a bounded number of registry point reads for
/// generations that are already gone. Admitting at most one pass per
/// <see cref="Interval"/> therefore bounds the extra load a silo spends on
/// reclamation regardless of how many views it hosts or how many generations
/// they leaked.
/// </para>
/// <para>
/// Lock-free and allocation-free: an admission is a single compare-and-swap on
/// the next admissible timestamp. A refused caller simply tries again on its
/// next drain; nothing queues.
/// </para>
/// </remarks>
internal sealed class ViewGenerationReclaimPacer
{
    /// <summary>The default minimum spacing between two admitted passes on one silo (2 seconds).</summary>
    internal static readonly TimeSpan DefaultInterval = TimeSpan.FromSeconds(2);

    private long _nextAdmissibleTimestamp = long.MinValue;

    /// <summary>The clock admissions are measured against. Settable for tests.</summary>
    internal TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>The minimum spacing between two admitted passes. Settable for tests.</summary>
    internal TimeSpan Interval { get; init; } = DefaultInterval;

    /// <summary>
    /// Admits one reclamation pass when the previous admission is at least
    /// <see cref="Interval"/> old, and returns <see langword="false"/> otherwise.
    /// </summary>
    public bool TryAdmit()
    {
        var now = TimeProvider.GetTimestamp();
        var spacing = (long)(Interval.TotalSeconds * TimeProvider.TimestampFrequency);
        while (true)
        {
            var next = Volatile.Read(ref _nextAdmissibleTimestamp);
            if (now < next)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _nextAdmissibleTimestamp, now + spacing, next) == next)
            {
                return true;
            }
        }
    }
}
