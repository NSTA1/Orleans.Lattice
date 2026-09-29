using System.Globalization;
using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// The confirmation every irreversible action goes through: an alert dialog
/// that states the consequence and enables its destructive action only once the
/// reader has typed the object's exact name.
/// </summary>
/// <remarks>
/// Typing the name, rather than clicking a second button, is the point: it makes
/// the reader read which object they are about to destroy. The match is exact
/// and case-sensitive, because tree ids and app slugs are. Enter submits, but
/// only while the name matches.
/// </remarks>
public partial class LtConfirmDestructive
{
    private LtTextInput? _input;
    private string _typed = string.Empty;
    private bool _wasOpen;
    private bool _focusOnRender;

    /// <summary>Whether the confirmation is showing.</summary>
    [Parameter]
    public bool Open { get; set; }

    /// <summary>Raised with <see langword="false"/> when the confirmation closes, confirmed or not.</summary>
    [Parameter]
    public EventCallback<bool> OpenChanged { get; set; }

    /// <summary>The dialog title, naming the action, such as "Delete tree".</summary>
    [Parameter, EditorRequired]
    public string Title { get; set; } = string.Empty;

    /// <summary>What kind of object is affected, such as "tree"; used in the field label.</summary>
    [Parameter, EditorRequired]
    public string ObjectKind { get; set; } = string.Empty;

    /// <summary>The object's exact name, which the reader must type to confirm.</summary>
    [Parameter, EditorRequired]
    public string ObjectName { get; set; } = string.Empty;

    /// <summary>The destructive action's label, such as "Delete tree". Defaults to "Delete".</summary>
    [Parameter]
    public string ConfirmText { get; set; } = "Delete";

    /// <summary>The consequence: what will happen and whether it can be undone.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }

    /// <summary>Raised once the reader has typed the name and confirmed.</summary>
    [Parameter]
    public EventCallback OnConfirm { get; set; }

    /// <summary>The element focus returns to when the confirmation closes.</summary>
    [Parameter]
    public ElementReference? ReturnFocus { get; set; }

    /// <summary>Whether what has been typed matches <see cref="ObjectName"/> exactly.</summary>
    internal bool Matches => ObjectName.Length > 0 && string.Equals(_typed, ObjectName, StringComparison.Ordinal);

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (Open && !_wasOpen)
        {
            _typed = string.Empty;
            _focusOnRender = true;
        }

        _wasOpen = Open;
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (_focusOnRender && Open && _input is not null)
        {
            _focusOnRender = false;
            await _input.FocusAsync();
        }
    }

    private static string Capitalise(string text) =>
        text.Length == 0 ? text : char.ToUpper(text[0], CultureInfo.InvariantCulture) + text[1..];

    private Task HandleTypedAsync(string value)
    {
        _typed = value;
        return Task.CompletedTask;
    }

    private Task HandleOpenChangedAsync(bool open) => OpenChanged.InvokeAsync(open);

    private Task CancelAsync() => OpenChanged.InvokeAsync(false);

    private async Task ConfirmAsync()
    {
        if (!Matches)
        {
            return;
        }

        _typed = string.Empty;
        await OnConfirm.InvokeAsync();
        await OpenChanged.InvokeAsync(false);
    }
}
