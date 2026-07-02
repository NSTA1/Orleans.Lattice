using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// Reads entries for the Data tab over the public state-API entry surface.
/// Trees and views are addressed uniformly by id; no grain access.
/// </summary>
public interface IDataReader
{
    /// <summary>
    /// Scans a page of entries for <paramref name="treeId"/>. Pass the
    /// <paramref name="continuationToken"/> from a prior page to resume the same
    /// cursor, or <see langword="null"/> to open a fresh scan. The
    /// <paramref name="mode"/> selects the cursor isolation for a fresh scan and
    /// defaults to <see cref="EntryScanMode.Live"/> (a cheap, baseline-free
    /// browse); pass <see cref="EntryScanMode.Snapshot"/> for a consistent
    /// point-in-time view (it is ignored on a continuation, which resumes the
    /// mode the cursor opened with). When a <paramref name="tagFilter"/> is
    /// supplied, only the rows of <paramref name="treeId"/> tagged with that
    /// value (in the named index) are returned. When a non-empty
    /// <paramref name="keyPrefix"/> is supplied (and no
    /// <paramref name="tagFilter"/> is active), the scan is bounded to the keys
    /// that start with that prefix, served as a ranged seek over the sorted keys.
    /// </summary>
    Task<DataPage> ScanAsync(
        string treeId,
        int pageSize,
        string? continuationToken = null,
        TagFilter? tagFilter = null,
        string? keyPrefix = null,
        EntryScanMode mode = EntryScanMode.Live,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Fetches the full record for a single key, or <see langword="null"/> when
    /// the key is absent.
    /// </summary>
    Task<DataEntry?> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Best-effort release of the snapshot scan cursor named by
    /// <paramref name="continuationToken"/> (as returned by a prior
    /// <see cref="ScanAsync"/> page), so its server-side WAL pin and baseline are
    /// freed promptly instead of lingering until the cursor's idle TTL. A
    /// <see langword="null"/> or empty token, or one naming an already-drained or
    /// unknown cursor, is a no-op. Never throws for an unknown cursor.
    /// </summary>
    Task CancelScanAsync(string treeId, string? continuationToken, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the tag indexes that cover <paramref name="treeId"/>, for the Data
    /// tab's tag filter. Each entry carries the clean index name and the id of
    /// its membership tree so the tab can navigate to the index's detail view.
    /// Returns an empty list when the table has no associated tag indexes.
    /// </summary>
    Task<IReadOnlyList<TagIndexRef>> ListTagIndexesForTreeAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the distinct tag values carried by the tag index
    /// <paramref name="indexName"/> over <paramref name="treeId"/>, in ascending
    /// ordinal order, for the Data tab's tag-value picker. Returns an empty list
    /// when the index has no members in that tree.
    /// </summary>
    Task<IReadOnlyList<string>> ListTagValuesForIndexAsync(
        string treeId,
        string indexName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the subject trees the tag index <paramref name="indexName"/>
    /// covers, in ascending ordinal order, for the tag-index detail view.
    /// Returns an empty list when the index covers no trees.
    /// </summary>
    Task<IReadOnlyList<string>> ListCoveredTreesForIndexAsync(
        string indexName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the distinct tags carried by the tag index
    /// <paramref name="indexName"/> across every tree it covers, in ascending
    /// ordinal order, for the tag-index detail view. Returns an empty list when
    /// the index has no members.
    /// </summary>
    Task<IReadOnlyList<string>> ListTagsForIndexAsync(
        string indexName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a page of the live members of the tag <paramref name="tag"/> across
    /// the tag index <paramref name="indexName"/>, ordered by <c>(tree id, key)</c>
    /// ordinal, for the tag-index detail view. Pass the
    /// <paramref name="continuationToken"/> from a prior page to resume, or
    /// <see langword="null"/> to open a fresh scan.
    /// </summary>
    Task<TagMemberPage> ScanTagMembersAsync(
        string indexName,
        string tag,
        int pageSize,
        string? continuationToken = null,
        CancellationToken cancellationToken = default);
}
