namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>Reads the run mode from <c>LATTICE_E2E_MODE</c>.</summary>
public static class JourneyRunModes
{
    /// <summary>The environment variable naming the mode: <c>isolated</c> (the default) or <c>story</c>.</summary>
    public const string Variable = "LATTICE_E2E_MODE";

    /// <summary>This run's mode.</summary>
    public static JourneyRunMode Current => Parse(Environment.GetEnvironmentVariable(Variable));

    /// <summary>
    /// Parses a mode: empty means <see cref="JourneyRunMode.Isolated"/>; anything but
    /// <c>isolated</c> or <c>story</c> fails, so a typo in a workflow cannot silently run the
    /// wrong mode.
    /// </summary>
    /// <param name="text">The variable's value.</param>
    public static JourneyRunMode Parse(string? text) => text?.Trim().ToLowerInvariant() switch
    {
        null or "" or "isolated" => JourneyRunMode.Isolated,
        "story" => JourneyRunMode.Story,
        _ => throw new InvalidOperationException($"{Variable}='{text}' is not a run mode; use 'isolated' or 'story'."),
    };

    /// <summary>The mode's name as the variable spells it.</summary>
    /// <param name="mode">The mode.</param>
    public static string Name(JourneyRunMode mode) => mode == JourneyRunMode.Story ? "story" : "isolated";
}
