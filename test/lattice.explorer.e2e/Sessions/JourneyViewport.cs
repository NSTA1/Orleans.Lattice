namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

/// <summary>
/// The fixed viewport a journey's browser runs at, so every screenshot of a step is taken at
/// the same size: 1440x900 on a desktop run, 390x844 on a narrow run.
/// </summary>
/// <param name="Name">The viewport's name in the screenshot manifest: <c>desktop</c> or <c>narrow</c>.</param>
/// <param name="Width">The CSS width.</param>
/// <param name="Height">The CSS height.</param>
public sealed record JourneyViewport(string Name, int Width, int Height)
{
    /// <summary>
    /// The environment variable that selects the run's viewport: <c>narrow</c> runs every
    /// session at <see cref="Narrow"/>; anything else, or nothing, at <see cref="Desktop"/>.
    /// </summary>
    public const string EnvironmentVariable = "LATTICE_E2E_VIEWPORT";

    /// <summary>The desktop viewport: the Explorer's expanded chrome.</summary>
    public static JourneyViewport Desktop { get; } = new("desktop", 1440, 900);

    /// <summary>The narrow viewport: a phone, where the chrome is compact.</summary>
    public static JourneyViewport Narrow { get; } = new("narrow", 390, 844);

    /// <summary>The viewport this run uses, from <see cref="EnvironmentVariable"/>.</summary>
    public static JourneyViewport Run { get; } =
        string.Equals(Environment.GetEnvironmentVariable(EnvironmentVariable), Narrow.Name, StringComparison.OrdinalIgnoreCase) ? Narrow : Desktop;

    /// <summary>Whether the Explorer draws its compact chrome at this width.</summary>
    public bool IsCompact => Width < 600;
}
