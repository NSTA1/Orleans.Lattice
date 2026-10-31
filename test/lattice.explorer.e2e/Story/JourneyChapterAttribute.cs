namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// Marks a journey fixture as a chapter of the story:
/// <c>[JourneyChapter("A3", After = new[] { "A2" })]</c>. <see cref="After"/> lists the chapters
/// whose produces this one consumes, exactly as its issue's contract (and
/// <see cref="StoryOutline"/>) lists them; a test fails if the two disagree.
/// </summary>
/// <param name="id">The chapter id, such as <c>A3</c>.</param>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class JourneyChapterAttribute(string id) : Attribute
{
    /// <summary>The chapter id.</summary>
    public string Id { get; } = id;

    /// <summary>The chapters this one consumes.</summary>
    public string[] After { get; set; } = [];

    /// <summary>
    /// The storyline, or <see langword="null"/> to take it from <see cref="StoryOutline"/>. Only a
    /// self-test chapter outside the outline names one.
    /// </summary>
    public string? Storyline { get; set; }
}
