using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Plugins.Data;

/// <summary>
/// The value drill-down surface's tag-filter concern: the indexes covering this
/// selection, the values of the chosen index, the active filter, and the hand-off
/// into the tag-index browser.
/// <para>
/// The whole filter is a progressive enhancement: every probe here degrades to
/// "no tag filter offered" rather than failing the surface, so a cluster with no
/// tag indexes - or one whose catalogue probe fails - still browses keys.
/// </para>
/// </summary>
public partial class DataTab
{
    private static readonly IReadOnlyList<TagIndexRef> NoIndexes = Array.Empty<TagIndexRef>();
    private static readonly IReadOnlyList<string> NoValues = Array.Empty<string>();

    private IReadOnlyList<TagIndexRef> _tagIndexes = NoIndexes;
    private IReadOnlyList<string> _tagValues = NoValues;
    private string? _selectedIndex;
    private string _tagValue = string.Empty;
    private bool _tagValuesLoading;
    private TagFilter? _activeTagFilter;

    private EventCallback<string> _tagIndexChanged;
    private EventCallback<string> _tagValueChanged;
    private EventCallback _exploreTagIndex;

    private void BindTagCallbacks()
    {
        _tagIndexChanged = EventCallback.Factory.Create<string>(this, OnTagIndexChangedAsync);
        _tagValueChanged = EventCallback.Factory.Create<string>(this, ApplyTagFilterAsync);
        _exploreTagIndex = EventCallback.Factory.Create(this, ExploreTagIndexAsync);
    }

    private async Task LoadTagIndexesAsync(string? retainedIndexName)
    {
        try
        {
            _tagIndexes = await Surface.ListTagIndexesAsync(Selection.Id, TabToken);
            _selectedIndex = ResolveSelectedIndex(retainedIndexName);
        }
        catch (OperationCanceledException) when (TabToken.IsCancellationRequested)
        {
            // The view navigated away mid-load; nothing to surface.
        }
        catch
        {
            // The tag filter is a progressive enhancement: if the catalogue probe
            // fails the surface still works as an unfiltered scan.
            _tagIndexes = NoIndexes;
            _selectedIndex = null;
        }
    }

    /// <summary>
    /// The retained index when it still exists, otherwise the first available
    /// one, otherwise none.
    /// </summary>
    private string? ResolveSelectedIndex(string? retainedIndexName)
    {
        if (retainedIndexName is not null)
        {
            for (var i = 0; i < _tagIndexes.Count; i++)
            {
                if (string.Equals(_tagIndexes[i].IndexName, retainedIndexName, StringComparison.Ordinal))
                {
                    return retainedIndexName;
                }
            }
        }

        return _tagIndexes.Count > 0 ? _tagIndexes[0].IndexName : null;
    }

    /// <summary>
    /// Loads the distinct tag values for the selected index and restores this
    /// selection's retained value (default: no filter).
    /// </summary>
    private async Task LoadRetainedTagFilterAsync()
    {
        if (string.IsNullOrEmpty(_selectedIndex))
        {
            _tagValues = NoValues;
            _tagValue = string.Empty;
            _activeTagFilter = null;
            return;
        }

        _tagValuesLoading = true;
        StateHasChanged();
        try
        {
            _tagValues = await Surface.ListTagValuesAsync(Selection.Id, _selectedIndex, TabToken);
        }
        catch (OperationCanceledException) when (TabToken.IsCancellationRequested)
        {
            return;
        }
        catch
        {
            _tagValues = NoValues;
        }
        finally
        {
            _tagValuesLoading = false;
        }

        var retained = Surface.GetRetainedTagValue(Selection.Id, _selectedIndex);
        if (retained is not null && _tagValues.Contains(retained))
        {
            _tagValue = retained;
            _activeTagFilter = new TagFilter(_selectedIndex, retained);
        }
        else
        {
            _tagValue = string.Empty;
            _activeTagFilter = null;
        }
    }

    /// <summary>
    /// The operator picked a different tag index: retain it, load its values,
    /// restore the retained value for that index, and refresh the display.
    /// </summary>
    private async Task OnTagIndexChangedAsync(string indexName)
    {
        _selectedIndex = string.IsNullOrEmpty(indexName) ? null : indexName;

        if (_selectedIndex is not null)
        {
            await Surface.SetRetainedTagIndexAsync(Selection.Id, _selectedIndex, TabToken);
        }

        await LoadRetainedTagFilterAsync();
        await ReloadFirstPageAsync();
    }

    /// <summary>
    /// The operator picked a tag value (or the empty "(any)" option, which clears
    /// the filter). Retain per selection and index, then refresh.
    /// </summary>
    private async Task ApplyTagFilterAsync(string tag)
    {
        if (string.IsNullOrEmpty(_selectedIndex))
        {
            return;
        }

        _tagValue = tag ?? string.Empty;
        _activeTagFilter = _tagValue.Length == 0 ? null : new TagFilter(_selectedIndex, _tagValue);

        await Surface.SetRetainedTagValueAsync(
            Selection.Id,
            _selectedIndex,
            _tagValue.Length == 0 ? null : _tagValue,
            TabToken);

        await ReloadFirstPageAsync();
    }

    /// <summary>
    /// Opens the selected tag index's dedicated browser, seeding the selected tag
    /// so it opens with that tag preselected. A no-op when the selected index has
    /// no known membership tree (it was dropped from the catalogue mid-session).
    /// </summary>
    private async Task ExploreTagIndexAsync()
    {
        if (string.IsNullOrEmpty(_selectedIndex))
        {
            return;
        }

        TagIndexRef? target = null;
        for (var i = 0; i < _tagIndexes.Count; i++)
        {
            if (string.Equals(_tagIndexes[i].IndexName, _selectedIndex, StringComparison.Ordinal))
            {
                target = _tagIndexes[i];
                break;
            }
        }

        if (target is null)
        {
            return;
        }

        await Surface.ExploreTagIndexAsync(
            target,
            _tagValue.Length == 0 ? null : _tagValue,
            TabToken);
    }
}
