namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The application-supplied source of the grain keys a background backfill
/// crawls. Register one per index with
/// <see cref="GrainIndexServiceCollectionExtensions.AddGrainIndexKeySource{TSource}(Hosting.ISiloBuilder, string)"/>
/// or one of its overloads.
/// </summary>
/// <remarks>
/// <para>
/// Orleans cannot enumerate the arbitrary grain ids of a grain type: a grain
/// exists because something addressed it, and the runtime keeps no list of the
/// ones that are merely durable. The population an index has to onboard is
/// therefore knowledge the application holds - a users table, a tenant roster, a
/// key range it allocates from - and this is the seam that hands it over.
/// </para>
/// <para>
/// The seam is deliberately narrow so later strategies (deriving keys from a key
/// scheme, or piggybacking on an existing lattice tree) can be added as
/// implementations without the backfill grain changing.
/// </para>
/// <para>
/// An implementation must satisfy three things, because the crawl's
/// resumability rests on them:
/// </para>
/// <list type="number">
/// <item>
/// <description>
/// Keys are the <b>encoded</b> grain keys the index stores, as
/// <see cref="IGrainKeyCodec.Encode(Runtime.GrainId)"/> produces them - not the
/// grain's raw primary key, unless the codec passes it through.
/// </description>
/// </item>
/// <item>
/// <description>
/// Keys are yielded in <b>ascending ordinal order</b>, each at most once.
/// Ordering is what lets a checkpoint be a single resume key rather than a set
/// of visited ones.
/// </description>
/// </item>
/// <item>
/// <description>
/// Enumeration starting after a given key yields exactly the keys ordinally
/// greater than it, so a resumed crawl neither repeats nor skips.
/// </description>
/// </item>
/// </list>
/// <para>
/// A source that yields a key for a grain with no persisted state is harmless:
/// the grain is visited, contributes nothing, and is revisited by a later
/// rebuild rather than being recorded as indexed.
/// </para>
/// </remarks>
public interface IGrainKeySource
{
    /// <summary>
    /// Streams the encoded grain keys to crawl, in ascending ordinal order,
    /// beginning strictly after <paramref name="resumeAfterExclusive"/>.
    /// </summary>
    /// <param name="resumeAfterExclusive">
    /// The last key the crawl already visited, or <c>null</c> to start at the
    /// beginning of the range. When non-<c>null</c>, the sequence must begin at
    /// the first key ordinally greater than it.
    /// </param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>The encoded grain keys to visit.</returns>
    IAsyncEnumerable<string> EnumerateKeysAsync(
        string? resumeAfterExclusive,
        CancellationToken cancellationToken);
}
