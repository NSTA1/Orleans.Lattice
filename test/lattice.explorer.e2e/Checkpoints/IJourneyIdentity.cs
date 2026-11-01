namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// What gives a journey fixture its journey id, the directory its screenshots and manifest
/// are written to (<c>artifacts/screenshots/{jid}/</c>). <see cref="JourneyAttribute"/> is the
/// plain form; the story framework's chapter attribute implements it too.
/// </summary>
public interface IJourneyIdentity
{
    /// <summary>The journey id, lower-case kebab-case, such as <c>a3</c>.</summary>
    string JourneyId { get; }
}
