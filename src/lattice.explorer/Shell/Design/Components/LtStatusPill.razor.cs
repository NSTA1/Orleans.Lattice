using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// A small pill naming a lifecycle or health state. The state is always written
/// as text and preceded by its role's glyph; its role's colour is the third cue,
/// never the only one.
/// </summary>
/// <remarks>
/// With no <see cref="State"/> it is the documentation site's neutral status
/// label ("in progress", "unreleased"), and <see cref="Text"/> is required.
/// </remarks>
public partial class LtStatusPill
{
    /// <summary>The state to show, or <see langword="null"/> for a neutral label.</summary>
    [Parameter]
    public LtStateRole? State { get; set; }

    /// <summary>
    /// The visible text. Defaults to the state's own label, such as "Enabled";
    /// required when <see cref="State"/> is <see langword="null"/>.
    /// </summary>
    [Parameter]
    public string? Text { get; set; }

    private string StateKey => State is { } role ? LtStateRoles.Key(role) : "neutral";

    private string DisplayText => Text ?? (State is { } role ? LtStateRoles.Label(role) : string.Empty);

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (State is null && string.IsNullOrWhiteSpace(Text))
        {
            throw new InvalidOperationException(
                $"{nameof(LtStatusPill)} needs either a {nameof(State)} or a {nameof(Text)}: a pill with neither says nothing.");
        }
    }
}
