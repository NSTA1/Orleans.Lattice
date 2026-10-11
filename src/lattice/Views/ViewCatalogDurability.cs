namespace Orleans.Lattice.Views;

/// <summary>
/// How far a catalog entry's durable runtime-registry record is known to back
/// it, as tracked by <see cref="IViewCatalog"/> for the synchronous factory path
/// that persists the record in the background.
/// </summary>
internal enum ViewCatalogDurability
{
    /// <summary>
    /// No persist is outstanding for the entry: the durable registry is the
    /// authority for it (or it is catalog-only by design).
    /// </summary>
    Settled = 0,

    /// <summary>
    /// The background persist has not finished. Any durable record that is absent
    /// or different may simply predate the write, so it is not evidence that the
    /// entry is stale.
    /// </summary>
    PersistInFlight = 1,

    /// <summary>
    /// The background persist failed and will not be retried. An absent record is
    /// still not evidence of a delete (the entry is deliberately catalog-only until
    /// a restart), but a present, different record can only have been written by a
    /// later create or a definition the failed write never replaced, so it wins.
    /// </summary>
    PersistFailed = 2,
}