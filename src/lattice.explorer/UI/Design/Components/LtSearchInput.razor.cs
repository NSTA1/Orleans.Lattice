using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A search or filter input on sunken paper, with an optional key hint in the
/// manner of the documentation site's "/" search. Enter submits the query and
/// Escape clears it.
/// </summary>
/// <remarks>
/// The label is visible, in the label row every field primitive draws, so a search
/// box lines up with the fields and buttons beside it (issue #4120). The key
/// hint is only a hint: the shortcut itself is bound by whoever owns the page,
/// and <c>aria-keyshortcuts</c> announces it.
/// </remarks>
public partial class LtSearchInput
{
    private readonly string _id = LtIds.Next("lt-search");

    /// <summary>The visible label, such as "Filter trees", which is also the field's accessible name.</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>The current query.</summary>
    [Parameter]
    public string? Value { get; set; }

    /// <summary>Raised on every edit, and on Escape with an empty query.</summary>
    [Parameter]
    public EventCallback<string> ValueChanged { get; set; }

    /// <summary>Raised when Enter is pressed, with the current query.</summary>
    [Parameter]
    public EventCallback<string> OnSubmit { get; set; }

    /// <summary>An example query, shown while the field is empty.</summary>
    [Parameter]
    public string? Placeholder { get; set; }

    /// <summary>
    /// The page's shortcut for this field, such as <c>/</c>: shown as a key hint
    /// and announced through <c>aria-keyshortcuts</c>. Leave <see langword="null"/> for none.
    /// </summary>
    [Parameter]
    public string? KeyShortcut { get; set; }

    /// <summary>Whether this field is the page's search landmark. Leave false for a table filter.</summary>
    [Parameter]
    public bool Landmark { get; set; }

    /// <summary>Whether the input is disabled.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    /// <summary>Any further attributes for the <c>input</c> element.</summary>
    [Parameter(CaptureUnmatchedValues = true)]
    public IReadOnlyDictionary<string, object>? AdditionalAttributes { get; set; }

    private Task HandleInputAsync(ChangeEventArgs args)
    {
        Value = args.Value as string ?? string.Empty;
        return ValueChanged.InvokeAsync(Value);
    }

    private async Task HandleKeyDownAsync(KeyboardEventArgs args)
    {
        switch (args.Key)
        {
            case "Enter":
                await OnSubmit.InvokeAsync(Value ?? string.Empty);
                break;
            case "Escape" when !string.IsNullOrEmpty(Value):
                Value = string.Empty;
                await ValueChanged.InvokeAsync(Value);
                break;
        }
    }
}
