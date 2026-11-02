namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>One checkpoint of a journey, in the screenshot manifest.</summary>
/// <param name="Index">The checkpoint's place in the journey, from 1.</param>
/// <param name="Step">The step id (<see cref="JourneyStepAttribute.Id"/>).</param>
/// <param name="Substep">The substep id.</param>
/// <param name="Title">The caption: what the screenshot shows.</param>
/// <param name="Persona">The persona whose session was captured, or <see langword="null"/>.</param>
/// <param name="File">The screenshot's file name, beside the manifest.</param>
/// <param name="Sha256">The lower-case hex SHA-256 of the screenshot file.</param>
/// <param name="Passed">Whether every check at the checkpoint passed.</param>
public sealed record JourneyManifestEntry(
    int Index,
    string Step,
    string Substep,
    string Title,
    string? Persona,
    string File,
    string Sha256,
    bool Passed);
