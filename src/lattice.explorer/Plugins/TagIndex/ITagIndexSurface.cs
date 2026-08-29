using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Plugins.TagIndex;

/// <summary>
/// The controlled domain model of the tag-index browsing surface: the three
/// index reads it renders, the one-shot tag seed a sibling surface may leave for
/// it, and the two navigations it offers back into the data the index covers.
/// <para>
/// This is the whole of the plugin's reach (epic decision D3). Note what the
/// navigations are <em>not</em>: they are not a preference store handed to the
/// plugin. The surface states the intent ("open this tree", "open this member")
/// and the adapter behind this contract owns the retained state that intent
/// implies, so no view holds another surface's key scheme.
/// </para>
/// </summary>
public interface ITagIndexSurface
{
    /// <summary>
    /// Lists the subject trees the index <paramref name="indexName"/> covers, in
    /// ascending ordinal order.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<IReadOnlyList<string>> ListCoveredTreesAsync(string indexName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the distinct tags the index <paramref name="indexName"/> carries
    /// across every tree it covers, in ascending ordinal order.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<IReadOnlyList<string>> ListTagsAsync(string indexName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans a page of the live members carrying <paramref name="tag"/> in
    /// <paramref name="indexName"/>, ordered by <c>(tree id, key)</c>.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <see langword="null"/>.</param>
    /// <param name="tag">The tag whose members to list. Must not be <see langword="null"/>.</param>
    /// <param name="pageSize">The maximum number of members to return.</param>
    /// <param name="continuationToken">The prior page's cursor, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<TagMemberPage> ScanMembersAsync(
        string indexName,
        string tag,
        int pageSize,
        string? continuationToken = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Takes the one-shot tag another surface left to preselect for this
    /// membership tree, clearing it so a later refresh starts clean. Returns
    /// <see langword="null"/> when nothing was seeded.
    /// </summary>
    /// <param name="membershipTreeId">The selected membership tree id. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<string?> TakeSeededTagAsync(string membershipTreeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens <paramref name="treeId"/> on the value drill-down surface with no
    /// tag filter applied.
    /// </summary>
    /// <param name="treeId">The covered tree to open. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task GoToTreeAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens <paramref name="member"/> on the value drill-down surface with its
    /// key inspected and the scan pre-filtered by <paramref name="indexName"/>
    /// and <paramref name="tag"/>.
    /// </summary>
    /// <param name="member">The member row to open. Must not be <see langword="null"/>.</param>
    /// <param name="indexName">The index the filter is applied through. Must not be <see langword="null"/>.</param>
    /// <param name="tag">The tag the filter is applied for. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task GoToMemberAsync(
        TagMemberRow member,
        string indexName,
        string tag,
        CancellationToken cancellationToken = default);
}
