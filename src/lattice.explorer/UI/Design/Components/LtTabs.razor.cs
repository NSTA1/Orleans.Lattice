using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A set of tabs following the WAI-ARIA tabs pattern with automatic activation:
/// the arrow keys move between tabs and activate the one they land on, Home and
/// End jump to the first and last, and only the active tab is in the tab order.
/// </summary>
/// <remarks>
/// The active tab is ink at a heavier weight with a marker bar on its lower edge,
/// as the documentation site's header marks its current section. Tabs are
/// declared as <see cref="LtTab"/> children in display order; only the active
/// tab's panel is rendered.
/// </remarks>
public partial class LtTabs
{
    private readonly string _id = LtIds.Next("lt-tabs");
    private readonly List<LtTab> _tabs = [];
    private readonly Dictionary<string, ElementReference> _buttons = new(StringComparer.Ordinal);
    private string? _focusPending;

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
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (_focusPending is { } id && _buttons.TryGetValue(id, out var button))
        {
            _focusPending = null;
            await button.FocusSafelyAsync();
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
