using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Plugins.Data;

/// <summary>
/// The value drill-down surface's paging concern: opening a fresh scan, stepping
/// the cached pager, and retaining the page position for the session.
/// </summary>
public partial class DataTab
{
    private async Task ReloadFirstPageAsync()
    {
        if (_pager is null)
        {
            return;
        }

        ClearSelection();

        // A key prefix only applies to an untagged scan; the state-API range is
        // ignored under a tag filter (and the search box is disabled then).
        var prefix = _activeTagFilter is null && _keyPrefix.Length > 0 ? _keyPrefix : null;
        await RunAsync(() => _pager.ResetAsync(
            Selection.Id, _pageSize, _activeTagFilter, prefix, _scanMode, TabToken));
        PersistPage();
    }

    private async Task OnKeySearchAsync(string prefix)
    {
        _keyPrefix = prefix;
        await Surface.SetRetainedKeyPrefixAsync(Selection.Id, prefix, TabToken);
        await ReloadFirstPageAsync();
    }

    private async Task OnPageSizeChangedAsync(int size)
    {
        _pageSize = DataPaging.Normalize(size);
        await Surface.SetRetainedPageSizeAsync(Selection.Id, _pageSize, TabToken);

        // A page-size change opens a fresh snapshot from the first page.
        await ReloadFirstPageAsync();
    }

    private async Task OnScanModeChangedAsync(EntryScanMode mode)
    {
        _scanMode = mode;
        await Surface.SetRetainedScanModeAsync(Selection.Id, mode, TabToken);
        await ReloadFirstPageAsync();
    }

    private async Task NextPageAsync()
    {
        if (_pager is null)
        {
            return;
        }

        ClearSelection();
        await RunAsync(() => _pager.NextAsync(TabToken));
        PersistPage();
    }

    private void PreviousPage()
    {
        if (_pager is null)
        {
            return;
        }

        ClearSelection();
        _pager.Previous();
        PersistPage();
        StateHasChanged();
    }

    /// <summary>Retains the current page index for the session.</summary>
    private void PersistPage()
    {
        if (_pager is not null)
        {
            Surface.SetSessionPage(Selection.Id, _pager.PageIndex);
        }
    }

    /// <summary>
    /// Advances the freshly opened snapshot forward to the retained page on
    /// mount, bounded by the pages actually available (the step is a no-op past
    /// the last page, so a shrunk dataset simply lands on the new last page).
    /// </summary>
    private async Task RestoreToPageAsync(int targetPage)
    {
        if (_pager is null)
        {
            return;
        }

        await RunAsync(async () =>
        {
            while (_pager.PageIndex < targetPage && _pager.CanGoNext)
            {
                await _pager.NextAsync(TabToken);
            }
        });
        PersistPage();
    }

    private async Task RunAsync(Func<Task> operation)
    {
        _loading = true;
        _error = null;
        StateHasChanged();

        try
        {
            await operation();
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
}
