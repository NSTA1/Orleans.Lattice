using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Plugins.Data;

/// <summary>
/// The value drill-down surface's selected-entry concern: the per-key read, the
/// forward-only live tail that keeps it current, and the debounced silent
/// refetch a change triggers.
/// <para>
/// Both the tail and the refetch are linked to the view's own lifetime token, so
/// a superseded selection, a key change or a surface switch abandons them.
/// </para>
/// </summary>
public partial class DataTab
{
    /// <summary>
    /// How long a burst of change signals for the followed key is allowed to
    /// coalesce before the silent refetch runs.
    /// </summary>
    private static readonly TimeSpan RefreshDebounce = TimeSpan.FromMilliseconds(400);

    private string? _selectedKey;
    private DataEntry? _detail;
    private RenderedValue? _rendered;
    private bool _detailLoading;
    private string? _detailError;

    // When true, the selected key's detail panel shows its revision history
    // instead of the value view. Reset whenever the selected key changes.
    private bool _showHistory;

    // The parameter set handed to the nested revision timeline, and the key that
    // re-mounts it. Both are rebuilt only when the inspected key changes, never
    // per render: the detail panel re-renders on every live refresh and every
    // connection-state change, and composing either here would allocate on all
    // of them.
    private Dictionary<string, object?>? _historyParameters;
    private string _historyKey = string.Empty;

    private CancellationTokenSource? _liveCts;
    private CancellationTokenSource? _refreshCts;
    private string? _liveError;

    private Task RetrySelectedAsync() =>
        _selectedKey is null ? Task.CompletedTask : SelectAsync(_selectedKey);

    private async Task SelectAsync(string? key)
    {
        if (key is null)
        {
            return;
        }

        // A new selection tears down the previous key's live subscription before
        // the swap, so no follow loop or pending refresh outlives its key.
        StopFollowing();

        _selectedKey = key;
        _detail = null;
        _rendered = null;
        _detailError = null;
        _liveError = null;
        _detailLoading = true;
        _showHistory = false;

        // One dictionary for the view's lifetime rather than one per key: the
        // nested timeline is re-mounted on every key change (see the key), so a
        // fresh instance reads whatever is currently filed here.
        _historyParameters ??= new Dictionary<string, object?>(StringComparer.Ordinal);
        _historyParameters[nameof(SelectionPluginViewBase.Selection)] = Selection;
        _historyKey = $"history:{Selection.Id}:{key}";

        // Publish the inspected key so the revision-timeline surface opens on the
        // same key when the operator hands off to it.
        Surface.SetInspectedKey(Selection.Id, key);

        StateHasChanged();

        try
        {
            _detail = await Surface.GetEntryAsync(Selection.Id, key, TabToken);
            _rendered = _detail is null ? null : ValueRenderer.Render(_detail.Value, _detail.Truncated);
        }
        catch (OperationCanceledException) when (TabToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _detailError = ex.Message;
        }
        finally
        {
            _detailLoading = false;
            StateHasChanged();
        }

        // Begin the live tail for the freshly selected key. Following an absent
        // key is intentional: a later write that creates it refreshes the panel.
        StartFollowing(key);
    }

    /// <summary>
    /// Opens the live subscription for the current key. The loop is linked to the
    /// view token so a tear-down cancels it; <see cref="StopFollowing"/> cancels
    /// it on a key change. A no-op when the head registered no follower.
    /// </summary>
    private void StartFollowing(string key)
    {
        if (!_showLiveIndicator)
        {
            return;
        }

        var cts = CancellationTokenSource.CreateLinkedTokenSource(TabToken);
        _liveCts = cts;
        _ = FollowLoopAsync(key, cts);
    }

    private async Task FollowLoopAsync(string key, CancellationTokenSource cts)
    {
        var treeId = Selection.Id;
        try
        {
            await foreach (var _ in Surface.FollowEntryAsync(treeId, key, cts.Token).ConfigureAwait(false))
            {
                // Marshal to the renderer's context so the refresh-debounce
                // bookkeeping is only ever touched on the same thread as
                // StopFollowing, avoiding a teardown race.
                await InvokeAsync(() => ScheduleRefresh(key, cts.Token));
            }
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
            // Expected teardown on key change or view dispose. The cancelled
            // stream may surface either as OperationCanceledException or as a
            // transport "Cancelled" status; both are silent teardown, not a fault.
        }
        catch (Exception ex)
        {
            await InvokeAsync(() =>
            {
                _liveError = ex.Message;
                StateHasChanged();
            });
        }
        finally
        {
            cts.Dispose();
        }
    }

    /// <summary>
    /// Debounced silent refetch: a burst of notifications for the followed key
    /// coalesces into a single re-read that swaps the displayed value and decoded
    /// CRDT members without a spinner.
    /// </summary>
    private void ScheduleRefresh(string key, CancellationToken followToken)
    {
        _refreshCts?.Cancel();
        _refreshCts?.Dispose();
        var cts = CancellationTokenSource.CreateLinkedTokenSource(followToken);
        _refreshCts = cts;
        _ = RefreshDetailAsync(key, cts);
    }

    private async Task RefreshDetailAsync(string key, CancellationTokenSource cts)
    {
        try
        {
            await Task.Delay(RefreshDebounce, cts.Token).ConfigureAwait(false);
            var entry = await Surface.GetEntryAsync(Selection.Id, key, cts.Token).ConfigureAwait(false);
            await InvokeAsync(() =>
            {
                // Ignore a refresh that raced past a key change.
                if (!string.Equals(_selectedKey, key, StringComparison.Ordinal))
                {
                    return;
                }

                _detail = entry;
                _rendered = entry is null ? null : ValueRenderer.Render(entry.Value, entry.Truncated);
                _liveError = null;
                StateHasChanged();
            });
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
            // Superseded by a newer change or torn down; leave the panel as-is.
        }
        catch (Exception ex)
        {
            await InvokeAsync(() =>
            {
                _liveError = ex.Message;
                StateHasChanged();
            });
        }
        finally
        {
            if (ReferenceEquals(_refreshCts, cts))
            {
                _refreshCts = null;
            }

            cts.Dispose();
        }
    }

    private void StopFollowing()
    {
        _liveCts?.Cancel();
        _liveCts = null;
        _refreshCts?.Cancel();
        _refreshCts = null;
    }

    private void ClearSelection()
    {
        StopFollowing();
        _selectedKey = null;
        _detail = null;
        _rendered = null;
        _detailError = null;
        _liveError = null;
        _showHistory = false;
    }

    /// <summary>
    /// Swaps the selected row's detail panel between its value view and the
    /// nested revision timeline for the same key.
    /// </summary>
    private void ToggleHistory() => _showHistory = !_showHistory;
}
