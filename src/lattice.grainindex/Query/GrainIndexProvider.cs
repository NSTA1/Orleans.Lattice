using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// The declaration-backed <see cref="IGrainIndexProvider"/>: it looks a
/// definition up in the silo's declared set and hands back a cached
/// <see cref="GrainIndex{TGrain, TState}"/> for it.
/// <para>
/// Caching matters because constructing an index builds its property table and
/// one value binder per property. That is cheap, but it is per-index setup work,
/// not per-query work, so it happens once.
/// </para>
/// </summary>
internal sealed class GrainIndexProvider : IGrainIndexProvider
{
    private readonly ConcurrentDictionary<string, object> _indexes = new(StringComparer.Ordinal);
    private readonly IReadOnlyList<IGrainIndexDefinition> _definitions;
    private readonly IGrainFactory _grainFactory;
    private readonly IOptionsMonitor<GrainIndexOptions> _options;
    private readonly string[] _names;

    public GrainIndexProvider(
        IOptions<GrainIndexDeclarationOptions> declarations,
        IGrainFactory grainFactory,
        IOptionsMonitor<GrainIndexOptions> options)
    {
        ArgumentNullException.ThrowIfNull(declarations);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(options);

        var declared = declarations.Value.Definitions;
        var snapshot = new IGrainIndexDefinition[declared.Count];
        var names = new string[declared.Count];
        for (var i = 0; i < declared.Count; i++)
        {
            snapshot[i] = declared[i];
            names[i] = declared[i].Name;
        }

        _definitions = snapshot;
        _names = names;
        _grainFactory = grainFactory;
        _options = options;
    }

    /// <inheritdoc />
    public IReadOnlyList<string> DeclaredIndexes => _names;

    /// <inheritdoc />
    public IGrainIndex<TGrain, TState> GetIndex<TGrain, TState>(string? name = null)
        where TGrain : IGrain
    {
        var definition = name is null ? FindByType<TGrain, TState>() : FindByName<TGrain, TState>(name);

        // The name uniquely identifies a definition, so a cached instance can
        // never be handed back under different type arguments.
        return (IGrainIndex<TGrain, TState>)_indexes.GetOrAdd(
            definition.Name,
            static (_, state) => new GrainIndex<TGrain, TState>(state.Definition, state.Factory, state.Options),
            (Definition: definition, Factory: _grainFactory, Options: _options));
    }

    private GrainIndexDefinition<TGrain, TState> FindByName<TGrain, TState>(string name)
        where TGrain : IGrain
    {
        for (var i = 0; i < _definitions.Count; i++)
        {
            var candidate = _definitions[i];
            if (!string.Equals(candidate.Name, name, StringComparison.Ordinal))
                continue;

            if (candidate is GrainIndexDefinition<TGrain, TState> typed)
                return typed;

            throw new InvalidOperationException(
                $"Grain index '{name}' is declared over grain type "
                + $"'{candidate.GrainInterfaceType.FullName}' and state type "
                + $"'{candidate.StateType.FullName}', but was requested as "
                + $"'{typeof(TGrain).FullName}' / '{typeof(TState).FullName}'.");
        }

        throw new InvalidOperationException(
            $"No grain index named '{name}' is declared. Declared indexes: {Describe()}.");
    }

    private GrainIndexDefinition<TGrain, TState> FindByType<TGrain, TState>()
        where TGrain : IGrain
    {
        GrainIndexDefinition<TGrain, TState>? found = null;
        for (var i = 0; i < _definitions.Count; i++)
        {
            if (_definitions[i] is not GrainIndexDefinition<TGrain, TState> typed)
                continue;

            if (found is not null)
            {
                throw new InvalidOperationException(
                    $"More than one grain index is declared over '{typeof(TGrain).FullName}' and "
                    + $"'{typeof(TState).FullName}' ('{found.Name}' and '{typed.Name}'). Name the "
                    + "index explicitly.");
            }

            found = typed;
        }

        return found ?? throw new InvalidOperationException(
            $"No grain index is declared over '{typeof(TGrain).FullName}' and "
            + $"'{typeof(TState).FullName}'. Declared indexes: {Describe()}.");
    }

    private string Describe() => _names.Length == 0 ? "(none)" : string.Join(", ", _names);
}
