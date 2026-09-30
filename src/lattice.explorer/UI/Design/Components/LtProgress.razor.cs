using System.Globalization;
using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// The progress of a long-running operation: an ink bar that fills a hairline
/// track, with the operation's phase and percentage written above it. When the
/// total is known the bar is determinate and, for a small total, drawn as one
/// segment per unit, as nodes on a chain; when it is not known the track is
/// hatched and carries the phase alone, never an invented percentage.
/// </summary>
/// <remarks>
/// The track is an ARIA <c>progressbar</c>: a determinate bar reports
/// <c>aria-valuenow</c> as a percentage between <c>aria-valuemin</c> 0 and
/// <c>aria-valuemax</c> 100, and every bar describes itself in
/// <c>aria-valuetext</c>. A change of <see cref="Phase"/> is announced once
/// through a polite live region; a moving percentage is not, so a reader is not
/// interrupted on every poll.
/// </remarks>
public partial class LtProgress
{
    /// <summary>
    /// The largest total drawn as separate segments. Above it the bar is
    /// continuous, since segments narrower than a hairline would read as texture.
    /// </summary>
    public const long MaximumSegments = 32;

    private string? _lastPhase;
    private string? _announcement;
    private bool _initialised;

    /// <summary>What is progressing, as the bar's accessible name, such as "Purge progress".</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>The step the operation is on, such as "Copying shards", or <see langword="null"/>.</summary>
    [Parameter]
    public string? Phase { get; set; }

    /// <summary>The units finished. Clamped to between 0 and <see cref="Maximum"/>.</summary>
    [Parameter]
    public long Value { get; set; }

    /// <summary>
    /// The units the operation consists of, or <see langword="null"/> (or a value
    /// of 0 or less) when that is not known, which draws an indeterminate bar.
    /// </summary>
    [Parameter]
    public long? Maximum { get; set; }

    /// <summary>A line under the bar naming the units, such as "3 of 8 shards", or <see langword="null"/>.</summary>
    [Parameter]
    public string? Detail { get; set; }

    /// <summary>Whether the bar is determinate: its total is known.</summary>
    public bool IsDeterminate => Maximum is > 0;

    /// <summary>
    /// The whole percentage finished, rounded down so a bar never reads 100 until
    /// every unit is done, or <see langword="null"/> for an indeterminate bar.
    /// </summary>
    public int? Percent => Maximum is > 0 and var maximum
        ? (int)(Math.Clamp(Value, 0, maximum) * 100 / maximum)
        : null;

    private string ModeKey => IsDeterminate ? "determinate" : "indeterminate";

    private bool Segmented => Maximum is > 1 and <= MaximumSegments;

    private string? FillStyle => Percent is { } percent
        ? "inline-size: " + percent.ToString(CultureInfo.InvariantCulture) + "%"
        : null;

    private string? TicksStyle => Segmented
        ? "--lt-progress-segments: " + Maximum!.Value.ToString(CultureInfo.InvariantCulture)
        : null;

    private string? PercentText => Percent is { } percent
        ? percent.ToString(CultureInfo.InvariantCulture) + "%"
        : null;

    private string ValueText
    {
        get
        {
            var text = PercentText;
            text = Append(text, Phase);
            text = Append(text, Detail);
            return text ?? "In progress";

            static string? Append(string? text, string? part) =>
                string.IsNullOrWhiteSpace(part) ? text : text is null ? part : text + ", " + part;
        }
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (string.IsNullOrWhiteSpace(Label))
        {
            throw new InvalidOperationException($"{nameof(LtProgress)} needs a {nameof(Label)}: a progress bar must say what is progressing.");
        }

        // Announce a phase only when it changes after the first set of
        // parameters, so a page that opens on a running operation does not speak
        // over its own heading.
        if (!string.Equals(Phase, _lastPhase, StringComparison.Ordinal))
        {
            _announcement = _initialised ? Phase : null;
            _lastPhase = Phase;
        }

        _initialised = true;
    }
}
