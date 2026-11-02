using System.Reflection;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// The <see cref="IChapterContract"/>s a run can call, by chapter. <see cref="Discovered"/> holds
/// every contract in this assembly: any concrete type implementing the interface with a public
/// parameterless constructor.
/// </summary>
public sealed class ChapterContracts
{
    private static readonly Lazy<ChapterContracts> DiscoveredContracts = new(() => Discover(typeof(ChapterContracts).Assembly));
    private readonly Dictionary<string, IChapterContract> _byChapter;

    /// <summary>Holds <paramref name="contracts"/>; two contracts for one chapter are refused.</summary>
    /// <param name="contracts">The contracts.</param>
    public ChapterContracts(IEnumerable<IChapterContract> contracts)
    {
        ArgumentNullException.ThrowIfNull(contracts);
        _byChapter = new Dictionary<string, IChapterContract>(StringComparer.Ordinal);
        foreach (var contract in contracts)
        {
            if (!_byChapter.TryAdd(contract.Chapter, contract))
            {
                throw new InvalidOperationException(
                    $"Two contracts claim chapter {contract.Chapter}: {_byChapter[contract.Chapter].GetType().FullName} and {contract.GetType().FullName}.");
            }
        }
    }

    /// <summary>Every contract in this assembly.</summary>
    public static ChapterContracts Discovered => DiscoveredContracts.Value;

    /// <summary>The chapters that have a contract, in ordinal order.</summary>
    public IReadOnlyList<string> Chapters => [.. _byChapter.Keys.Order(StringComparer.Ordinal)];

    /// <summary>The contract for <paramref name="chapter"/>, or <see langword="null"/>.</summary>
    /// <param name="chapter">The chapter id.</param>
    public IChapterContract? Find(string chapter) => _byChapter.GetValueOrDefault(chapter);

    /// <summary>Every contract <paramref name="assembly"/> declares.</summary>
    /// <param name="assembly">The assembly to search.</param>
    public static ChapterContracts Discover(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        var contracts = assembly.GetTypes()
            .Where(type => type is { IsClass: true, IsAbstract: false, ContainsGenericParameters: false }
                && typeof(IChapterContract).IsAssignableFrom(type)
                && type.GetConstructor(Type.EmptyTypes) is { IsPublic: true })
            .OrderBy(type => type.FullName, StringComparer.Ordinal)
            .Select(type => (IChapterContract)Activator.CreateInstance(type)!);
        return new ChapterContracts(contracts);
    }
}
