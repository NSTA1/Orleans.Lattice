using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// The page's one notification region: a polite live region, so a new toast is
/// announced without interrupting what the reader is doing. Place it once, in
/// the layout.
/// </summary>
/// <remarks>
/// Each toast names its tone in words ("Done", "Failed") as well as colour, and
/// stays until dismissed. The region itself is always in the document, empty or
/// not, because a live region added at the same moment as its content is often
/// not announced.
/// </remarks>
public partial class LtToastRegion : IDisposable
{
    private IReadOnlyList<LtToast> _toasts = [];

    /// <summary>The region's accessible name. Defaults to "Notifications".</summary>
    [Parameter]
    public string Label { get; set; } = "Notifications";

    [Inject]
    private LtToastService Toasts { get; set; } = default!;

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        _toasts = Toasts.Toasts;
        Toasts.Changed += HandleChanged;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Toasts.Changed -= HandleChanged;
        GC.SuppressFinalize(this);
    }

    private static string ToneKey(LtToastTone tone) => tone switch
    {
        LtToastTone.Success => "success",
        LtToastTone.Warning => "warning",
        LtToastTone.Danger => "danger",
        _ => "info",
    };

    private static string ToneLabel(LtToastTone tone) => tone switch
    {
        LtToastTone.Success => "Done",
        LtToastTone.Warning => "Warning",
        LtToastTone.Danger => "Failed",
        _ => "Notice",
    };

    private void Dismiss(long id) => Toasts.Dismiss(id);

    private void HandleChanged() => _ = InvokeAsync(() =>
    {
        _toasts = Toasts.Toasts;
        StateHasChanged();
    });
}
