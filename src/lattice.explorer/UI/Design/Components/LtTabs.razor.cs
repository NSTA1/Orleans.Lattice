using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A set of tabs following the WAI-ARIA tabs pattern with automatic activation:
/// the arrow keys move between tabs and activate the one they land on, Home and
/// End jump to the first and last, and only the active tab is in the tab order.
/// </summary>
/// <remarks>
/// <para>
/// The active tab is ink at a heavier weight with a marker bar on its lower edge,
/// as the documentation site's header marks its current section. Tabs are
/// declared as <see cref="LtTab"/> children in display order; only the active
/// tab's panel is rendered.
/// </para>
/// <para>
/// The row never wraps: one wider than its column scrolls inside its own frame,
/// and the active tab is kept in that frame's view whenever it changes, without
/// scrolling the page.
/// </para>
/// </remarks>
public partial class LtTabs : IAsyncDisposable
{
    private readonly string _id = LtIds.Next("lt-tabs");
    private readonly List<LtTab> _tabs = [];
    private readonly Dictionary<string, ElementReference> _buttons = new(StringComparer.Ordinal);
    private ElementReference _list;
    private IJSObjectReference? _module;
    private string? _focusPending;
    private string? _revealed;
    private bool _disposed;

    /// <summary>The tab list's accessible name, such as "Tree views".</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>The tabs: <see cref="LtTab"/> components, in display order.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }

    /// <summary>
    /// The <see cref="LtTab.Id"/> of the active tab. When it names no enabled
    /// tab, the first enabled tab is active.
    /// </summary>
    [Parameter]
    public string? ActiveId { get; set; }

    /// <summary>Raised with the newly active tab's id when the reader changes tab.</summary>
    [Parameter]
    public EventCallback<string> ActiveIdChanged { get; set; }

    /// <summary>The id of the tab that is active now, after the fall-back to the first enabled tab.</summary>
    internal string? EffectiveActiveId
    {
        get
        {
            string? firstEnabled = null;
            foreach (var tab in _tabs)
            {
                if (tab.Disabled)
                {
                    continue;
                }

                if (string.Equals(tab.Id, ActiveId, StringComparison.Ordinal))
                {
                    return tab.Id;
                }

                firstEnabled ??= tab.Id;
            }

            return firstEnabled;
        }
    }

    [Inject]
    internal IJSRuntime JS { get; set; } = default!;

    internal bool IsActive(LtTab tab) => string.Equals(tab.Id, EffectiveActiveId, StringComparison.Ordinal);

    internal string TabElementId(LtTab tab) => _id + "-tab-" + tab.Id;

    internal string PanelElementId(LtTab tab) => _id + "-panel-" + tab.Id;

    internal void Register(LtTab tab)
    {
        if (_tabs.Any(existing => string.Equals(existing.Id, tab.Id, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Two tabs in one {nameof(LtTabs)} share the id '{tab.Id}'.");
        }

        _tabs.Add(tab);
        StateHasChanged();
    }

    internal void Unregister(LtTab tab)
    {
        if (_tabs.Remove(tab))
        {
            _buttons.Remove(tab.Id);
            StateHasChanged();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        if (_module is { } module)
        {
            _module = null;
            try
            {
                await module.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException or ObjectDisposedException)
            {
                // Best effort: the page or the circuit has gone.
            }
        }

        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (_focusPending is { } id && _buttons.TryGetValue(id, out var button))
        {
            _focusPending = null;
            await button.FocusSafelyAsync();
        }

        if (EffectiveActiveId is { } active && !string.Equals(active, _revealed, StringComparison.Ordinal))
        {
            _revealed = active;
            await RevealActiveAsync();
        }
    }

    // Keeps the active tab in view in a row that scrolls in its own frame. Script
    // is an enhancement: without it the row still scrolls, and focus reveals a tab.
    private async Task RevealActiveAsync()
    {
        try
        {
            _module ??= await JS.InvokeAsync<IJSObjectReference?>("import", ShellDesignAssets.TabsModuleSpecifier).ConfigureAwait(true);
            if (_module is { } module && !_disposed)
            {
                await module.InvokeVoidAsync("reveal", _list).ConfigureAwait(true);
            }
        }
        catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException or InvalidOperationException)
        {
            // The page or the circuit has gone, or there is no browser (prerendering).
        }
    }

    private async Task HandleKeyDownAsync(KeyboardEventArgs args)
    {
        var enabled = _tabs.Where(tab => !tab.Disabled).ToList();
        if (enabled.Count == 0)
        {
            return;
        }

        var current = enabled.FindIndex(IsActive);
        var target = args.Key switch
        {
            "ArrowRight" => enabled[(current + 1) % enabled.Count],
            "ArrowLeft" => enabled[(current - 1 + enabled.Count) % enabled.Count],
            "Home" => enabled[0],
            "End" => enabled[^1],
            _ => null,
        };

        if (target is not null)
        {
            await ActivateAsync(target, moveFocus: true);
        }
    }

    private async Task ActivateAsync(LtTab tab, bool moveFocus)
    {
        if (tab.Disabled)
        {
            return;
        }

        if (moveFocus)
        {
            _focusPending = tab.Id;
        }

        if (IsActive(tab))
        {
            return;
        }

        ActiveId = tab.Id;
        await ActiveIdChanged.InvokeAsync(tab.Id);
    }
}
