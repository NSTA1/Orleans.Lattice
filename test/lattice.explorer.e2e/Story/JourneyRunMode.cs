namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>How the journeys run, selected by <c>LATTICE_E2E_MODE</c> (#5361).</summary>
public enum JourneyRunMode
{
    /// <summary>
    /// The default. Each chapter gets a fresh estate; the F6 contracts establish and verify its
    /// consumes through the APIs, the chapter's UI steps run, and its own contract verifies
    /// what it produced.
    /// </summary>
    Isolated,

    /// <summary>
    /// One estate per storyline; the chapters run in dependency order with nothing seeded, each
    /// consuming what the chapters before it produced through the UI. A failed chapter blocks
    /// its dependants, which are reported blocked by name and never as passed.
    /// </summary>
    Story,
}
