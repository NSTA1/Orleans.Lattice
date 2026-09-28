using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// A modal dialog: a raised surface above the page - one of the few that may
/// cast the design system's shadow - holding a titled question and its actions.
/// </summary>
/// <remarks>
/// <para>
/// Focus moves to the dialog when it opens, stays inside it (a focus sentinel at
/// each end returns Tab to the dialog), and returns to <see cref="ReturnFocus"/>
/// when it closes. Escape closes it unless <see cref="DismissOnEscape"/> is
/// false. A click outside it does nothing: in an operator console an accidental
/// dismissal loses work.
/// </para>
/// <para>
/// The dialog is controlled: it shows while <see cref="Open"/> is true and asks
/// its host to close it through <see cref="OpenChanged"/>.
/// </para>
/// </remarks>
public partial class LtDialog
{
    private readonly string _id = LtIds.Next("lt-dialog");
    private ElementReference _dialog;
    private bool _wasOpen;
    private bool _focusOnRender;
    private bool _returnFocusOnRender;

    /// <summary>Whether the dialog is showing.</summary>
    [Parameter]
    public bool Open { get; set; }

    /// <summary>Raised with <see langword="false"/> when the dialog asks to close.</summary>
    [Parameter]
    public EventCallback<bool> OpenChanged { get; set; }

    /// <summary>The dialog's title, which is also its accessible name.</summary>
    [Parameter, EditorRequired]
    public string Title { get; set; } = string.Empty;

    /// <summary>A sentence stating what the dialog is asking, announced with the title.</summary>
    [Parameter]
    public string? Description { get; set; }

    /// <summary>The dialog's content.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }

    /// <summary>The dialog's actions, placed at its foot, primary action last.</summary>
    [Parameter]
    public RenderFragment? Actions { get; set; }

    /// <summary>
    /// Whether the dialog interrupts to demand a decision - a destructive
    /// confirmation - and is announced with the <c>alertdialog</c> role.
    /// </summary>
    [Parameter]
    public bool Alert { get; set; }

    /// <summary>Whether the header carries a Close button. Defaults to <see langword="true"/>.</summary>
    [Parameter]
    public bool ShowClose { get; set; } = true;

    /// <summary>Whether Escape closes the dialog. Defaults to <see langword="true"/>.</summary>
    [Parameter]
    public bool DismissOnEscape { get; set; } = true;

    /// <summary>
    /// Whether the dialog takes focus when it opens. Set false when the content
    /// focuses its own first field. Defaults to <see langword="true"/>.
    /// </summary>
    [Parameter]
    public bool AutoFocus { get; set; } = true;

    /// <summary>The element focus returns to when the dialog closes, usually the control that opened it.</summary>
    [Parameter]
    public ElementReference? ReturnFocus { get; set; }

    /// <summary>
    /// Where the dialog sits: centred (the default), or as a full-height sheet on
    /// the inline-start or inline-end edge, which takes the full width of a narrow
    /// screen. A sheet keeps the dialog's focus trap and focus return.
    /// </summary>
    [Parameter]
    public LtDialogPlacement Placement { get; set; } = LtDialogPlacement.Center;

    private string TitleId => _id + "-title";

    private string LayerClass => Placement == LtDialogPlacement.Center ? "lt-dialog-layer" : "lt-dialog-layer lt-dialog-layer--sheet";

    private string DialogClass => Placement switch
    {
        LtDialogPlacement.Start => "lt-dialog lt-dialog--sheet lt-dialog--start",
        LtDialogPlacement.End => "lt-dialog lt-dialog--sheet lt-dialog--end",
        _ => "lt-dialog",
    };

    private string DescriptionId => _id + "-description";

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (Open && !_wasOpen)
        {
            _focusOnRender = AutoFocus;
        }
        else if (!Open && _wasOpen)
        {
            _returnFocusOnRender = ReturnFocus is not null;
        }

        _wasOpen = Open;
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (_focusOnRender && Open)
        {
            _focusOnRender = false;
            await _dialog.FocusAsync();
        }
        else if (_returnFocusOnRender && ReturnFocus is { } target)
        {
            _returnFocusOnRender = false;
            await target.FocusAsync();
        }
    }

    /// <summary>Asks the host to close the dialog.</summary>
    /// <returns>A task that completes when the host has been told.</returns>
    public Task CloseAsync() => OpenChanged.InvokeAsync(false);

    private async Task FocusDialogAsync() => await _dialog.FocusAsync();

    private Task HandleKeyDownAsync(KeyboardEventArgs args) =>
        args.Key == "Escape" && DismissOnEscape ? CloseAsync() : Task.CompletedTask;
}
