namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

/// <summary>How a <see cref="PersonaSession"/> is opened.</summary>
public sealed class PersonaSessionOptions
{
    /// <summary>The viewport, or <see langword="null"/> for the run's (<see cref="JourneyViewport.Run"/>).</summary>
    public JourneyViewport? Viewport { get; init; }

    /// <summary>Which of the region's Explorer heads to use, from 0.</summary>
    public int Explorer { get; init; }

    /// <summary>The head-relative path the session opens at.</summary>
    public string Path { get; init; } = "/";
}
