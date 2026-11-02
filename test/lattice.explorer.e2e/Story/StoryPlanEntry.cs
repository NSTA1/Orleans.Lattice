namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// One chapter of a story run's plan: what it consumes and, once its journey exists, how to
/// create it. A chapter with no implementation is stood in by its F6 contract.
/// </summary>
/// <param name="Id">The chapter id.</param>
/// <param name="After">The chapters it consumes.</param>
/// <param name="Implementation">The journey type, or <see langword="null"/> when it is not implemented yet.</param>
public sealed record StoryPlanEntry(string Id, IReadOnlyList<string> After, Type? Implementation)
{
    /// <summary>Whether the chapter's journey exists.</summary>
    public bool IsImplemented => Implementation is not null;

    /// <summary>A new instance of the chapter's journey.</summary>
    public IStoryChapter Create() =>
        Implementation is null
            ? throw new InvalidOperationException($"Chapter {Id} has no implementation.")
            : (IStoryChapter)Activator.CreateInstance(Implementation)!;
}
