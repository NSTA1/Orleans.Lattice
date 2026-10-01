using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Explorer.UI.Operations;

/// <summary>
/// Draws one <see cref="LatticeOperationStatus"/> (#4122): a status pill, the step
/// among the operation's declared phases, and an <c>LtProgress</c> bar over the
/// current phase's units while it runs; the phase and units it stopped at and why
/// once it has failed or been cancelled. Progress is the cluster's own count - the
/// bar is determinate only when the phase total is known, and never shows an
/// invented percentage. Shared by every area that runs long operations.
/// </summary>
public partial class LtOperationProgress
{
    /// <summary>The status to draw.</summary>
    [Parameter, EditorRequired]
    public LatticeOperationStatus Status { get; set; } = default!;

    /// <summary>What is progressing, as the bar's accessible name, such as "Backup progress".</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>
    /// Maps a phase name to the words shown for it, or <see langword="null"/> to
    /// split the name into words (<c>CapturingMembers</c> reads "Capturing members").
    /// </summary>
    [Parameter]
    public Func<string, string>? PhaseName { get; set; }

    private string PhaseText => PhaseName?.Invoke(Status.Phase) ?? OperationText.Phase(Status.Phase);

    private string StoppedText => "Stopped during " + PhaseText
        + (OperationText.Units(Status) is { } units ? ", after " + units : string.Empty) + ".";

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (Status is null)
        {
            throw new InvalidOperationException($"{nameof(LtOperationProgress)} needs a {nameof(Status)}.");
        }

        if (string.IsNullOrWhiteSpace(Label))
        {
            throw new InvalidOperationException($"{nameof(LtOperationProgress)} needs a {nameof(Label)}: a progress bar must say what is progressing.");
        }
    }
}
