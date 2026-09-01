using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// <b>The Explorer's appearance state.</b> What palette, contrast and density the
/// operator has chosen, remembered per user through the shell's preference
/// contract and applied to the document on every load.
/// </summary>
/// <remarks>
/// <para>
/// <b>Three independent axes, not one enumeration.</b> Palette and contrast are
/// orthogonal in the token layer - the high-contrast overlay layers over whichever
/// palette is active - so "high contrast" is
/// <see cref="ExplorerContrastChoice.More"/> and never a fourth
/// <see cref="ExplorerThemeChoice"/>. Density is independent of both.
/// </para>
/// <para>
/// <b>Following the environment is the default, and is a real state.</b> An
/// operator who has never chosen gets the palette their machine asks for, the
/// contrast their operating system asks for, and the density each adaptive root
/// derives from its breakpoint. Choosing to follow the system is stored, so it is
/// distinguishable from never having chosen and survives a later default change.
/// </para>
/// <para>
/// <b>Reads are render-path safe.</b> <see cref="Theme"/>,
/// <see cref="Contrast"/>, <see cref="Density"/> and <see cref="Effective"/> are
/// field reads returning value types, so a component may read them on every
/// render without allocating.
/// </para>
/// </remarks>
public interface IExplorerAppearance
{
    /// <summary>
    /// Whether the durable contract has hydrated. Until it is
    /// <see langword="true"/> the choices below are the defaults, and must not be
    /// persisted back over a real choice that has not loaded yet.
    /// </summary>
    bool IsLoaded { get; }

    /// <summary>The palette the operator chose.</summary>
    ExplorerThemeChoice Theme { get; }

    /// <summary>The contrast overlay the operator chose.</summary>
    ExplorerContrastChoice Contrast { get; }

    /// <summary>The density the operator chose.</summary>
    ExplorerDensityChoice Density { get; }

    /// <summary>
    /// The choices as the document should carry them, after any host theme has
    /// been folded in.
    /// </summary>
    /// <remarks>
    /// Differs from the raw choices only when the operator is following the
    /// system <em>and</em> the head's host platform reports a theme of its own:
    /// the desktop head resolves "follow system" to the application's own theme,
    /// while the web head leaves it unresolved for the document's
    /// <c>prefers-color-scheme</c> query to answer.
    /// </remarks>
    ExplorerAppearanceState Effective { get; }

    /// <summary>
    /// The sentence to show when a remembered choice could not be used, or
    /// <see langword="null"/> when there is nothing to explain. Set when a stored
    /// value is not one this build knows - a name from a newer build, or a
    /// corrupted entry - which is forgotten rather than left to resurface.
    /// </summary>
    string? Notice { get; }

    /// <summary>
    /// Raised when any of the choices may have changed: a set, a hydration, a
    /// scope change, a reset, or a host theme switch.
    /// </summary>
    event Action? Changed;

    /// <summary>
    /// Hydrates from the durable contract and applies the result to the document.
    /// Safe to await from component initialization and safe to call repeatedly.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task EnsureLoadedAsync(CancellationToken cancellationToken = default);

    /// <summary>Chooses a palette, remembers it, and applies it.</summary>
    /// <param name="theme">The palette to use.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="theme"/> is not a declared choice.</exception>
    Task SetThemeAsync(ExplorerThemeChoice theme, CancellationToken cancellationToken = default);

    /// <summary>Chooses a contrast overlay, remembers it, and applies it.</summary>
    /// <param name="contrast">The contrast overlay to use.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="contrast"/> is not a declared choice.</exception>
    Task SetContrastAsync(ExplorerContrastChoice contrast, CancellationToken cancellationToken = default);

    /// <summary>Chooses a density, remembers it, and applies it.</summary>
    /// <param name="density">The density to use.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="density"/> is not a declared choice.</exception>
    Task SetDensityAsync(ExplorerDensityChoice density, CancellationToken cancellationToken = default);
}
