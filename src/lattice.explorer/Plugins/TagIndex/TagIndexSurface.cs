using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Plugins.TagIndex;

/// <summary>
/// The one place in this package that touches an Explorer service. It adapts the
/// shared data reader, the durable preference store, the session store and the
/// shell's selection onto <see cref="ITagIndexSurface"/>, so no view in this
/// package holds a store, a reader, or another surface's retained-state scheme.
/// </summary>
/// <param name="reader">The shared data reader.</param>
/// <param name="preferences">The durable UI preference store.</param>
/// <param name="session">The session-scoped UI state store.</param>
/// <param name="selection">The shell's current-selection service.</param>
internal sealed class TagIndexSurface(
    IDataReader reader,
    IUiPreferenceStore preferences,
    IUiSessionStore session,
    IExplorerSelection selection) : ITagIndexSurface
{
    private readonly IDataReader _reader = reader ?? throw new ArgumentNullException(nameof(reader));

    private readonly IUiPreferenceStore _preferences =
        preferences ?? throw new ArgumentNullException(nameof(preferences));

    private readonly IUiSessionStore _session = session ?? throw new ArgumentNullException(nameof(session));

    private readonly IExplorerSelection _selection =
        selection ?? throw new ArgumentNullException(nameof(selection));

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListCoveredTreesAsync(
        string indexName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        return _reader.ListCoveredTreesForIndexAsync(indexName, cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListTagsAsync(string indexName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        return _reader.ListTagsForIndexAsync(indexName, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TagMemberPage> ScanMembersAsync(
        string indexName,
        string tag,
        int pageSize,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(tag);
        return _reader.ScanTagMembersAsync(indexName, tag, pageSize, continuationToken, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<string?> TakeSeededTagAsync(
        string membershipTreeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(membershipTreeId);

        var key = SeededTagKey(membershipTreeId);
        var seeded = _preferences.GetOrDefault(key, string.Empty);
        if (string.IsNullOrEmpty(seeded))
        {
            return null;
        }

        // One-shot: consumed on read so a later refresh or manual navigation
        // starts clean rather than re-applying a stale seed.
        await _preferences.RemoveAsync(key, cancellationToken).ConfigureAwait(false);
        return seeded;
    }

    /// <inheritdoc />
    public async Task GoToTreeAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        await OpenOnDataSurfaceAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task GoToMemberAsync(
        TagMemberRow member,
        string indexName,
        string tag,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(member);
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(tag);

        // Seed the value drill-down surface's retained filter for the target
        // tree, then the key to inspect, before selecting it. Owned by the target
        // tree id so the seed is garbage-collected with it.
        await _preferences
            .SetAsync(DataTagIndexKey(member.TreeId), indexName, owner: member.TreeId, cancellationToken)
            .ConfigureAwait(false);
        await _preferences
            .SetAsync(DataTagValueKey(member.TreeId, indexName), tag, owner: member.TreeId, cancellationToken)
            .ConfigureAwait(false);

        _session.Set(DataSelection.SelectedKey(member.TreeId), member.Key);

        await OpenOnDataSurfaceAsync(member.TreeId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Seeds the active per-selection surface so the panel opens on the value
    /// drill-down (it re-applies the retained surface on a selection change)
    /// rather than on whichever surface was last active, then selects the tree.
    /// </summary>
    private async Task OpenOnDataSurfaceAsync(string treeId, CancellationToken cancellationToken)
    {
        await _preferences
            .SetAsync(SelectionPluginKeys.ActivePluginPreferenceKey, SelectionPluginKeys.Data, owner: null, cancellationToken)
            .ConfigureAwait(false);

        _selection.Select(new CatalogItem { Id = treeId, Kind = CatalogKind.Trees });
    }

    /// <summary>
    /// The durable-preference key a sibling surface writes to preselect a tag
    /// here, owned by and keyed on the membership tree id so it never collides
    /// with another index's seed.
    /// </summary>
    private static string SeededTagKey(string membershipTreeId) => $"tagindex-tag:{membershipTreeId}";

    /// <summary>
    /// The value drill-down surface's retained tag-index key for a tree. Named
    /// here because a hand-off writes the target surface's retained state; the
    /// two spellings are pinned together by
    /// <c>TagIndexHandOffKeyContractTests</c>.
    /// </summary>
    private static string DataTagIndexKey(string treeId) => $"data-tagindex:{treeId}";

    /// <summary>The value drill-down surface's retained tag value for a tree and index.</summary>
    private static string DataTagValueKey(string treeId, string indexName) =>
        $"data-tagvalue:{treeId}:{indexName}";
}
