namespace Orleans.Lattice.Apps.Tests;

/// <summary>A scripted <see cref="IAppTreeFacts"/>: registered trees, derivations and aliases held in memory.</summary>
internal sealed class FakeAppTreeFacts : IAppTreeFacts
{
    public HashSet<string> Registered { get; } = new(StringComparer.Ordinal);

    public Dictionary<string, string> DerivedFrom { get; } = new(StringComparer.Ordinal);

    /// <summary>Logical tree id to the physical tree it is aliased to.</summary>
    public Dictionary<string, string> Aliases { get; } = new(StringComparer.Ordinal);

    public Task<bool> ExistsAsync(string treeId) => Task.FromResult(Registered.Contains(treeId));

    public Task<string?> GetDerivedFromAsync(string treeId) =>
        Task.FromResult(DerivedFrom.TryGetValue(treeId, out var source) ? source : null);

    public Task<string> ResolveAsync(string treeId) =>
        Task.FromResult(Aliases.TryGetValue(treeId, out var physical) ? physical : treeId);

    public Task<IReadOnlyList<string>> GetAliasesTargetingAsync(string physicalTreeId) =>
        Task.FromResult<IReadOnlyList<string>>(Aliases.Where(pair => pair.Value == physicalTreeId).Select(pair => pair.Key).ToArray());
}
