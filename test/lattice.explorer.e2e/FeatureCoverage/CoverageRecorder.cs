using System.Collections.Concurrent;
using System.Text.Json;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>
/// Collects a run's committed coverage hits. <see cref="Default"/> is the run's recorder, written
/// to <c>coverage-hits.json</c> when the run ends; a self-test swaps in its own with
/// <see cref="Use"/> so it never pollutes the run's evidence.
/// </summary>
public sealed class CoverageRecorder
{
    private static readonly Lazy<CoverageRecorder> DefaultRecorder = new(() => new CoverageRecorder(() => FeatureCatalogue.Load()));
    private static readonly AsyncLocal<CoverageRecorder?> Override = new();
    private readonly Lazy<FeatureCatalogue> _catalogue;
    private readonly AsyncLocal<CoverageScope?> _scope = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _hits = new(StringComparer.Ordinal);

    /// <summary>Creates a recorder validating hits against the catalogue <paramref name="catalogue"/> loads.</summary>
    /// <param name="catalogue">Loads the catalogue, on the first hit.</param>
    public CoverageRecorder(Func<FeatureCatalogue> catalogue)
    {
        ArgumentNullException.ThrowIfNull(catalogue);
        _catalogue = new Lazy<FeatureCatalogue>(catalogue);
    }

    /// <summary>The run's recorder.</summary>
    public static CoverageRecorder Default => DefaultRecorder.Value;

    /// <summary>Whether the run's recorder was created.</summary>
    public static bool HasDefault => DefaultRecorder.IsValueCreated;

    /// <summary>The recorder hits go to here: an override from <see cref="Use"/>, else <see cref="Default"/>.</summary>
    public static CoverageRecorder Current => Override.Value ?? Default;

    /// <summary>The committed hits: each feature id with the scopes that proved it.</summary>
    public IReadOnlyDictionary<string, IReadOnlyList<string>> Hits =>
        _hits.OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .ToDictionary(pair => pair.Key, pair => (IReadOnlyList<string>)[.. pair.Value.Keys.Order(StringComparer.Ordinal)], StringComparer.Ordinal);

    /// <summary>Routes hits on the current flow to <paramref name="recorder"/> until the result is disposed.</summary>
    /// <param name="recorder">The recorder.</param>
    public static IDisposable Use(CoverageRecorder recorder)
    {
        ArgumentNullException.ThrowIfNull(recorder);
        var prior = Override.Value;
        Override.Value = recorder;
        return new Restore(() => Override.Value = prior);
    }

    /// <summary>Opens a scope on the current flow.</summary>
    /// <param name="name">The scope's name.</param>
    public CoverageScope BeginScope(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        var scope = new CoverageScope(this, name, _scope.Value);
        _scope.Value = scope;
        return scope;
    }

    /// <summary>Proposes a hit to the current flow's open scope.</summary>
    /// <param name="featureId">A catalogue feature id.</param>
    public void Hit(string featureId)
    {
        ArgumentException.ThrowIfNullOrEmpty(featureId);
        if (!_catalogue.Value.Contains(featureId))
        {
            throw new ArgumentException($"'{featureId}' is not a feature in {FeatureCatalogue.FileName}.", nameof(featureId));
        }

        var scope = _scope.Value ?? throw new InvalidOperationException(
            $"Coverage.Hit(\"{featureId}\") outside a coverage scope: a hit is evidence only inside a chapter or step scope that completes.");
        scope.Propose(featureId);
    }

    /// <summary>The committed hits as a <c>coverage-hits.json</c> document.</summary>
    /// <param name="mode">The run mode's name.</param>
    public CoverageHitsDocument ToDocument(string mode) => new(
        mode,
        Environment.GetEnvironmentVariable("GITHUB_RUN_ID"),
        Environment.GetEnvironmentVariable("GITHUB_SHA"),
        DateTimeOffset.UtcNow,
        [.. Hits.Select(pair => new CoverageHitEntry(pair.Key, pair.Value))]);

    /// <summary>Writes <see cref="ToDocument"/> to <paramref name="path"/>.</summary>
    /// <param name="path">The file.</param>
    /// <param name="mode">The run mode's name.</param>
    public void WriteTo(string path, string mode)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, JsonSerializer.Serialize(ToDocument(mode), CoverageHitsDocument.JsonOptions));
    }

    internal void Commit(string scope, IEnumerable<string> featureIds)
    {
        foreach (var featureId in featureIds)
        {
            _hits.GetOrAdd(featureId, _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal))[scope] = 0;
        }
    }

    internal void EndScope(CoverageScope scope)
    {
        if (ReferenceEquals(_scope.Value, scope))
        {
            _scope.Value = scope.Parent;
        }
    }

    private sealed class Restore(Action restore) : IDisposable
    {
        public void Dispose() => restore();
    }
}
