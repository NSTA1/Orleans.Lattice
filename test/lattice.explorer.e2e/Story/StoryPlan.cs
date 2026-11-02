namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>Builds a story run's plan and orders it.</summary>
public static class StoryPlan
{
    /// <summary>
    /// The plan of a real storyline: every outline chapter in story order, each with its journey
    /// when one is declared in this assembly.
    /// </summary>
    /// <param name="storyline">A storyline from <see cref="StoryNames.Storylines"/>.</param>
    public static IReadOnlyList<StoryPlanEntry> For(string storyline)
    {
        var declared = DeclaredChapter.Discover(typeof(StoryPlan).Assembly).ToDictionary(chapter => chapter.Id, StringComparer.Ordinal);
        return Order(StoryOutline.For(storyline).Select(chapter =>
            new StoryPlanEntry(chapter.Id, chapter.After, declared.GetValueOrDefault(chapter.Id)?.Type)));
    }

    /// <summary>The chapter graph of a real storyline, for a <see cref="StorySession"/>.</summary>
    /// <param name="storyline">A storyline from <see cref="StoryNames.Storylines"/>.</param>
    public static IReadOnlyDictionary<string, IReadOnlyList<string>> GraphFor(string storyline) => Graph(StoryOutline.For(storyline).Select(chapter =>
        new StoryPlanEntry(chapter.Id, chapter.After, null)));

    /// <summary>The chapter graph of <paramref name="plan"/>.</summary>
    /// <param name="plan">The plan.</param>
    public static IReadOnlyDictionary<string, IReadOnlyList<string>> Graph(IEnumerable<StoryPlanEntry> plan) =>
        plan.ToDictionary(entry => entry.Id, entry => entry.After, StringComparer.Ordinal);

    /// <summary>
    /// Orders <paramref name="entries"/> so every chapter follows everything it consumes; among
    /// chapters that are ready together, the one given first goes first (so the outline's serial
    /// story order is kept wherever the dependencies allow).
    /// </summary>
    /// <param name="entries">The plan, in its preferred order.</param>
    /// <exception cref="InvalidOperationException">A chapter is duplicated, consumes an unknown chapter, or the graph has a cycle.</exception>
    public static IReadOnlyList<StoryPlanEntry> Order(IEnumerable<StoryPlanEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var given = entries.ToList();
        var position = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var entry in given)
        {
            if (!position.TryAdd(entry.Id, position.Count))
            {
                throw new InvalidOperationException($"Chapter {entry.Id} appears twice in the plan.");
            }
        }

        foreach (var entry in given)
        {
            var unknown = entry.After.Where(dependency => !position.ContainsKey(dependency)).ToList();
            if (unknown.Count > 0)
            {
                throw new InvalidOperationException($"Chapter {entry.Id} consumes {string.Join(", ", unknown)}, which the plan does not hold.");
            }
        }

        var ordered = new List<StoryPlanEntry>(given.Count);
        var placed = new HashSet<string>(StringComparer.Ordinal);
        while (ordered.Count < given.Count)
        {
            var next = given.FirstOrDefault(entry => !placed.Contains(entry.Id) && entry.After.All(placed.Contains))
                ?? throw new InvalidOperationException(
                    $"The chapters {string.Join(", ", given.Where(entry => !placed.Contains(entry.Id)).Select(entry => entry.Id))} consume each other in a cycle.");
            ordered.Add(next);
            placed.Add(next.Id);
        }

        return ordered;
    }
}
