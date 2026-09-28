using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// A piece of data - a key, a tree id, an address, a digest - set in Cascadia
/// Mono with a copy button, which appears on hover or focus as the
/// documentation site's code-block copy button does.
/// </summary>
/// <remarks>
/// Copying uses the browser's asynchronous clipboard API. The outcome is
/// announced through a polite status message ("Copied" or "Copy failed"), so it
/// is never conveyed by a transient visual alone.
/// </remarks>
public partial class LtMonoCell
{
    /// <summary>The JavaScript function the copy button invokes with the value.</summary>
    internal const string ClipboardWriteFunction = "navigator.clipboard.writeText";

    private readonly string _id = LtIds.Next("lt-mono");
    private string? _announcement;

    /// <summary>The data to show and copy.</summary>
    [Parameter, EditorRequired]
    public string Value { get; set; } = string.Empty;

    /// <summary>The copy button's text. Defaults to "Copy".</summary>
    [Parameter]
    public string CopyLabel { get; set; } = "Copy";

    /// <summary>
    /// Whether a value too long for its column is cut with an ellipsis. The full
    /// value stays in the document for assistive technology and in a tooltip.
    /// Defaults to <see langword="true"/>.
    /// </summary>
    [Parameter]
    public bool Truncate { get; set; } = true;

    [Inject]
    private IJSRuntime JS { get; set; } = default!;

    private string CellClass => Truncate ? "lt-mono-cell lt-mono-cell--truncate" : "lt-mono-cell";

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        _announcement = null;
    }

    private async Task CopyAsync()
    {
        try
        {
            await JS.InvokeVoidAsync(ClipboardWriteFunction, Value);
            _announcement = "Copied";
        }
        catch (JSException)
        {
            _announcement = "Copy failed";
        }
    }
}
