using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Data;
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

    private IReadOnlyList<string> _coveredTrees = NoStrings;
    private IReadOnlyList<string> _tags = NoStrings;
    private bool _loading;
    private bool _loaded;
    private string? _error;

    private string? _selectedTag;

    // Bound once in OnInitialized rather than composed per render, so the three
    // child sections do not allocate a delegate each on every pass.
    private EventCallback<string> _openTree;
    private EventCallback<string> _selectTag;

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

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _openTree = EventCallback.Factory.Create<string>(this, OpenTreeAsync);
        _selectTag = EventCallback.Factory.Create<string>(this, SelectTagAsync);
        BindMemberCallbacks();

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
