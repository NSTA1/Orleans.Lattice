namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// The story's shape: every chapter of both storylines, in the serial story order the epic
/// (#5401) fixes, each with the chapters it consumes. A journey's
/// <see cref="JourneyChapterAttribute"/> must agree with its entry here, so the dependency graph
/// has one source and cannot drift chapter by chapter.
/// </summary>
public static class StoryOutline
{
    private static readonly Dictionary<string, OutlineChapter> ById;

    static StoryOutline()
    {
        Contoso =
        [
            Chapter("A1", 5371, "First contact"),
            Chapter("A2", 5372, "The access model", "A1"),
            Chapter("A9", 5373, "Access in depth", "A2"),
            Chapter("A3", 5374, "Provisioning the factory", "A2"),
            Chapter("A4", 5375, "The schema baseline", "A3"),
            Chapter("A5", 5376, "Installing the task board", "A2"),
            Chapter("A6", 5377, "Backup posture", "A3", "A4"),
            Chapter("A10", 5378, "Backups in depth", "A6"),
            Chapter("A7", 5379, "Going multi-region", "A3", "A5"),
            Chapter("A8", 5380, "Capacity and WAL operations", "A3"),
            Chapter("A11", 5381, "Telemetry in depth", "A3", "A8"),
            Chapter("A12", 5382, "Cluster in depth", "A8", "A10"),
            Chapter("U1", 5383, "A data steward's day", "A2", "A3", "A4"),
            Chapter("U2", 5384, "A schema owner's week", "A4", "U1"),
            Chapter("U6", 5385, "The schema rule workshop", "A3", "U2"),
            Chapter("U3", 5386, "A team on the task board", "A5"),
            Chapter("U5", 5387, "Personalising the console", "U3"),
            Chapter("U4", 5388, "The auditor's review", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10", "A11", "A12", "U1", "U2", "U3", "U5", "U6"),
            Chapter("C1", 5389, "Incident and recovery", "U1", "U2", "A6", "A7"),
            Chapter("C2", 5390, "Resilience", "A6", "A7", "A8"),
            Chapter("C3", 5391, "Offboarding and lifecycle", "U3", "A5", "A6", "A10", "A12", "C1"),
            Chapter("C4", 5392, "Running the console", "C3"),
        ];
        Fabrikam =
        [
            Chapter("T1", 5393, "Tenant onboarding"),
            Chapter("T2", 5394, "A tenant administrator's first week", "T1"),
            Chapter("T7", 5395, "The tenant-rooted walk", "T2"),
            Chapter("T3", 5396, "Cross-tenant sharing", "T2"),
            Chapter("T4", 5397, "Operator support visit", "T1", "T2"),
            Chapter("T5", 5398, "Residency change and tenant lifecycle", "T2", "T3"),
            Chapter("T6", 5399, "Region lifecycle", "T5"),
        ];
        ById = Contoso.Concat(Fabrikam).ToDictionary(chapter => chapter.Id, StringComparer.Ordinal);
    }

    /// <summary>The Contoso storyline, in serial story order.</summary>
    public static IReadOnlyList<OutlineChapter> Contoso { get; }

    /// <summary>The Fabrikam storyline, in serial story order.</summary>
    public static IReadOnlyList<OutlineChapter> Fabrikam { get; }

    /// <summary>Every chapter, Contoso then Fabrikam.</summary>
    public static IEnumerable<OutlineChapter> All => Contoso.Concat(Fabrikam);

    /// <summary>The chapters of <paramref name="storyline"/>, in serial story order.</summary>
    /// <param name="storyline">A storyline from <see cref="StoryNames.Storylines"/>.</param>
    public static IReadOnlyList<OutlineChapter> For(string storyline) => storyline switch
    {
        StoryNames.Storylines.Contoso => Contoso,
        StoryNames.Storylines.Fabrikam => Fabrikam,
        _ => throw new ArgumentException($"'{storyline}' is not a storyline of the epic.", nameof(storyline)),
    };

    /// <summary>The chapter named <paramref name="id"/>, or <see langword="null"/>.</summary>
    /// <param name="id">The chapter id.</param>
    public static OutlineChapter? Find(string id) => ById.GetValueOrDefault(id);

    /// <summary>The storyline a chapter id belongs to: <c>T</c> chapters are Fabrikam's, the rest Contoso's.</summary>
    /// <param name="id">The chapter id.</param>
    public static string StorylineOf(string id) =>
        id.StartsWith('T') ? StoryNames.Storylines.Fabrikam : StoryNames.Storylines.Contoso;

    private static OutlineChapter Chapter(string id, int issue, string title, params string[] after) =>
        new(id, StorylineOf(id), issue, title, after);
}
