namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Evaluates a predicate against the stored tenant record that a registry write
/// merged into, on the attempt that committed. A facade uses it to tell whether
/// its own write changed anything: a removal changed the record only when the
/// entry was live in the stored record it merged against, not in the facade's
/// earlier read, which a concurrent writer may already have overtaken.
/// </summary>
/// <param name="matches">The predicate over the stored record; it must not mutate it.</param>
internal sealed class TenantStoredRecordProbe(Func<TenantRecord, bool> matches)
{
    private readonly Func<TenantRecord, bool> _matches = matches ?? throw new ArgumentNullException(nameof(matches));

    /// <summary>
    /// Whether the predicate held for the stored record of the last observed attempt.
    /// <see langword="false"/> when no record was stored.
    /// </summary>
    public bool Matched { get; private set; }

    /// <summary>Observes the stored record an attempt is about to merge into.</summary>
    /// <param name="stored">The stored record, or <see langword="null"/> when none is stored.</param>
    internal void Observe(TenantRecord? stored) => Matched = stored is not null && _matches(stored);

    /// <summary>
    /// Records a match without observing a stored record, for a host-supplied
    /// registry that cannot report the record it merged into: the caller falls back
    /// to the check it made against its own read.
    /// </summary>
    internal void AssumeMatched() => Matched = true;
}
