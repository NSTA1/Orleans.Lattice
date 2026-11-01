namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// One chapter of the story as the epic's decision record (#5401) defines it: where it sits,
/// which issue specifies it, and the chapters whose produces it consumes.
/// </summary>
/// <param name="Id">The chapter id, such as <c>A3</c>.</param>
/// <param name="Storyline">The storyline it belongs to (<see cref="StoryNames.Storylines"/>).</param>
/// <param name="Issue">The journey issue that specifies it.</param>
/// <param name="Title">The chapter's title.</param>
/// <param name="After">The chapters it consumes, exactly as its contract lists them.</param>
public sealed record OutlineChapter(string Id, string Storyline, int Issue, string Title, IReadOnlyList<string> After);
