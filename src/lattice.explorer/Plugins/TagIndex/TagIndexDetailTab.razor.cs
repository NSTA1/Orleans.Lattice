using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Plugins.TagIndex;

/// <summary>
/// The tag-index surface's index-level state: the covered trees, the tags, the
/// one-shot seeded tag another surface may have left, and the tree navigation.
/// The member list and its paging live in <c>TagIndexDetailTab.Members.cs</c>
/// and the markup in <c>TagIndexDetailTab.razor</c>; all three are one partial
/// class.
/// </summary>
public partial class TagIndexDetailTab
{
    private static readonly IReadOnlyList<string> NoStrings = Array.Empty<string>();

    /// <summary>
    /// What the surface says when the selection handed to it is not a tag index
    /// at all. Built once: it names the situation and the way out of it, which
    /// the bare "This selection is not a tag index." did not.
    /// </summary>
    private static readonly ExplorerStateMessage NotATagIndex =
        ExplorerStateCopy.Empty(ExplorerSubjects.TagIndexes) with
        {
            Headline = "Not a tag index",
            Explanation = "This surface browses a tag index, and the current selection is not one, "
                + "so there is no membership to show. The selection may have been renamed or "
                + "removed since the catalog was listed.",
            Remedy = "Choose a tag index from the catalog, or pick another surface for this selection.",
        };

    private readonly ExplorerBadge[] _badges = new ExplorerBadge[ExplorerBadges.MaxCatalogBadges];

    private IReadOnlyList<string> _coveredTrees = NoStrings;
    private IReadOnlyList<string> _tags = NoStrings;
    private bool _loading;
    private bool _loaded;
    private string? _error;
    private ExplorerStateMessage? _failure;
    private int _badgeCount;

    private string? _selectedTag;

    // Bound once in OnInitialized rather than composed per render, so the three
    // child sections do not allocate a delegate each on every pass.
    private EventCallback<string> _openTree;
    private EventCallback<string> _selectTag;
    private EventCallback _retryLoad;

    /// <summary>
    /// The ambient shell context, so the three sections stack at compact width
    /// by name rather than by measuring a viewport.
    /// </summary>
    [CascadingParameter]
    public LatticeAdaptiveContext? AdaptiveContext { get; set; }

    private bool IsCompact =>
        (AdaptiveContext?.Breakpoint ?? LatticeBreakpoints.Default) == LatticeBreakpoint.Compact;

    /// <summary>The logical index name this membership tree carries, or empty when the selection is not one.</summary>
    private string IndexName => Selection.IndexName ?? string.Empty;

    /// <summary>
    /// The state the surface is in, or <see langword="null"/> when it has an
    /// index to render. The kind is picked rather than defaulted: a selection
    /// that is not an index, a read in flight and a read that failed are three
    /// different situations, and the reader is told which one they are in.
    /// </summary>
    /// <remarks>
    /// Read on the render path, and twice per pass, so it only ever selects an
    /// already-built message. The failure copy quotes the cluster's own words
    /// and therefore has to be composed; it is composed once, where the failure
    /// is caught.
    /// </remarks>
    private ExplorerStateMessage? State
    {
        get
        {
            if (IndexName.Length == 0)
            {
                return NotATagIndex;
            }

            if (_failure is not null)
            {
                return _failure;
            }

            return _loading && !_loaded
                ? ExplorerStateCopy.Loading(ExplorerSubjects.TagIndexes)
                : null;
        }
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _openTree = EventCallback.Factory.Create<string>(this, OpenTreeAsync);
        _selectTag = EventCallback.Factory.Create<string>(this, SelectTagAsync);
        _retryLoad = EventCallback.Factory.Create(this, LoadAsync);
        BindMemberCallbacks();

        // The selection is stable for this view's lifetime, so its badges are
        // composed exactly once into the buffer the render path reads.
        _badgeCount = ExplorerBadges.ForCatalogItem(Selection, deadLetterCount: 0, _badges);

        await LoadAsync();
    }

    private async Task LoadAsync()
    {
        var indexName = IndexName;
        if (indexName.Length == 0)
        {
            return;
        }

        _loading = true;
        _error = null;
        _failure = null;
        StateHasChanged();

        try
        {
            _coveredTrees = await Surface.ListCoveredTreesAsync(indexName, TabToken);
            _tags = await Surface.ListTagsAsync(indexName, TabToken);
            _loaded = true;

            // Take the one-shot tag another surface left so this view opens with
            // it preselected. Only honoured on the first load (when nothing is
            // selected yet); the take clears it either way.
            if (_selectedTag is null)
            {
                var seededTag = await Surface.TakeSeededTagAsync(Selection.Id, TabToken);
                if (seededTag is not null && _tags.Contains(seededTag))
                {
                    _selectedTag = seededTag;
                }
            }

            // Drop a stale member view if the previously selected tag is gone.
            if (_selectedTag is not null && !_tags.Contains(_selectedTag))
            {
                ClearMembers();
            }
            else if (_selectedTag is not null)
            {
                await ReloadMembersAsync();
            }
        }
        catch (OperationCanceledException) when (TabToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _error = ex.Message;

            // Composed here, not in the State property: the failure copy quotes
            // the cluster's words and so has to be built, and State is read on
            // every render pass.
            _failure = ExplorerStateCopy.Failed(ExplorerSubjects.TagIndexes, _error);
        }
        finally
        {
            _loading = false;
            StateHasChanged();
        }
    }

    private async Task SelectTagAsync(string tag)
    {
        if (string.Equals(tag, _selectedTag, StringComparison.Ordinal))
        {
            return;
        }

        _selectedTag = tag;
        await ReloadMembersAsync();
    }

    /// <summary>
    /// Opens a covered tree on the value drill-down surface with no tag filter.
    /// The surface owns what "open there" implies, so this view names only the
    /// intent.
    /// </summary>
    private Task OpenTreeAsync(string treeId) => Surface.GoToTreeAsync(treeId, TabToken);

    /// <summary>
    /// Opens a member's key on the value drill-down surface, pre-filtered by this
    /// index and tag.
    /// </summary>
    private Task OpenMemberAsync(TagMemberRow member) =>
        _selectedTag is null || IndexName.Length == 0
            ? Task.CompletedTask
            : Surface.GoToMemberAsync(member, IndexName, _selectedTag, TabToken);
}
