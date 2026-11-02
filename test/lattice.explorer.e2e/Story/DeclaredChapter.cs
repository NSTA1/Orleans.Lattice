using System.Reflection;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>A journey type declared as a chapter with <see cref="JourneyChapterAttribute"/>.</summary>
/// <param name="Id">The chapter id.</param>
/// <param name="Storyline">The storyline: the attribute's, else the outline's.</param>
/// <param name="After">The chapters it declares it consumes.</param>
/// <param name="Type">The journey type.</param>
public sealed record DeclaredChapter(string Id, string Storyline, IReadOnlyList<string> After, Type Type)
{
    /// <summary>
    /// Every chapter <paramref name="assembly"/> declares: concrete types implementing
    /// <see cref="IStoryChapter"/> that carry <see cref="JourneyChapterAttribute"/>.
    /// </summary>
    /// <param name="assembly">The assembly to search.</param>
    /// <exception cref="InvalidOperationException">A declaration is malformed, or two types declare one chapter.</exception>
    public static IReadOnlyList<DeclaredChapter> Discover(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        var declared = new List<DeclaredChapter>();
        foreach (var type in assembly.GetTypes().OrderBy(type => type.FullName, StringComparer.Ordinal))
        {
            var attribute = type.GetCustomAttribute<JourneyChapterAttribute>(inherit: false);
            if (attribute is null)
            {
                continue;
            }

            if (type.IsAbstract || !typeof(IStoryChapter).IsAssignableFrom(type) || type.GetConstructor(Type.EmptyTypes) is null)
            {
                throw new InvalidOperationException(
                    $"{type.FullName} declares chapter {attribute.Id} but is not a concrete IStoryChapter with a parameterless constructor.");
            }

            var storyline = attribute.Storyline ?? StoryOutline.Find(attribute.Id)?.Storyline
                ?? throw new InvalidOperationException($"{type.FullName} declares chapter {attribute.Id}, which is not in the story outline.");
            declared.Add(new DeclaredChapter(attribute.Id, storyline, attribute.After, type));
        }

        var duplicate = declared.GroupBy(chapter => chapter.Id, StringComparer.Ordinal).FirstOrDefault(group => group.Count() > 1);
        if (duplicate is not null)
        {
            throw new InvalidOperationException(
                $"Chapter {duplicate.Key} is declared by {string.Join(" and ", duplicate.Select(chapter => chapter.Type.FullName))}.");
        }

        return declared;
    }
}
