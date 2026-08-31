using System.Linq.Expressions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.GrainIndex.Query;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The typed query surface over one declared grain index. See
/// <see cref="IGrainIndex{TGrain, TState}"/> for the supported predicate
/// dialect, the routing rules, the memory profile, and the consistency contract.
/// </summary>
/// <remarks>
/// An instance is stateless and concurrency-safe once constructed: the property
/// table and value binders are built here, in the constructor, and are then only
/// read. Build one per index and reuse it, exactly as with
/// <see cref="GrainIndexMaintainer{TGrain, TState}"/>.
/// </remarks>
/// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
/// <typeparam name="TState">The grain-state type the index projects from.</typeparam>
public sealed class GrainIndex<TGrain, TState> : IGrainIndex<TGrain, TState>
    where TGrain : IGrain
{
    private readonly GrainIndexDefinition<TGrain, TState> _definition;
    private readonly GrainIndexQueryProperty[] _properties;
    private readonly string[] _propertyNames;
    private readonly GrainIndexQueryExecutor _executor;
    private readonly IGrainFactory _grainFactory;

    /// <summary>
    /// Initialises an index over an explicit tree, which is the form to use when
    /// the caller already holds the index's <see cref="ILattice"/> reference.
    /// </summary>
    /// <param name="definition">The index definition. Must not be <c>null</c>.</param>
    /// <param name="tree">The index's backing lattice tree. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">The factory used to resolve matched grains. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">The definition projects no properties, so nothing can be queried.</exception>
    public GrainIndex(
        GrainIndexDefinition<TGrain, TState> definition,
        ILattice tree,
        IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(grainFactory);

        _definition = definition;
        (_properties, _propertyNames) = BuildPropertyTable(definition);
        _executor = new GrainIndexQueryExecutor(tree);
        _grainFactory = grainFactory;
    }

    /// <summary>
    /// Initialises an index that resolves its backing tree from the index's named
    /// options.
    /// </summary>
    /// <remarks>
    /// The tree name is read once, here: it is validated at startup and is not a
    /// runtime-tunable knob, so re-reading it per query would buy nothing.
    /// </remarks>
    /// <param name="definition">The index definition. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">The grain factory used to resolve the tree and matched grains. Must not be <c>null</c>.</param>
    /// <param name="options">The per-index options monitor, read by index name. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">The definition projects no properties, so nothing can be queried.</exception>
    public GrainIndex(
        GrainIndexDefinition<TGrain, TState> definition,
        IGrainFactory grainFactory,
        IOptionsMonitor<GrainIndexOptions> options)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(options);

        _definition = definition;
        (_properties, _propertyNames) = BuildPropertyTable(definition);
        _executor = new GrainIndexQueryExecutor(
            grainFactory.GetGrain<ILattice>(options.Get(definition.Name).TreeName));
        _grainFactory = grainFactory;
    }

    /// <inheritdoc />
    public string Name => _definition.Name;

    /// <inheritdoc />
    public IReadOnlyList<string> IndexedProperties => _propertyNames;

    /// <inheritdoc />
    public IGrainIndexQuery<TGrain> Where(Expression<Func<TState, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        var plan = GrainIndexQueryPlanner.Build(predicate, _definition.Name, _properties, _propertyNames);
        return new GrainIndexQuery<TGrain>(
            plan,
            _executor,
            _definition.KeyCodec,
            _grainFactory,
            GrainIndexQueryDefaults.PageSize,
            GrainIndexQueryDefaults.Execution);
    }

    private static (GrainIndexQueryProperty[] Properties, string[] Names) BuildPropertyTable(
        GrainIndexDefinition<TGrain, TState> definition)
    {
        var declared = definition.Properties;
        if (declared.Count == 0)
        {
            throw new ArgumentException(
                $"Grain index '{definition.Name}' projects no properties, so no predicate can be "
                + "routed against it. Declare at least one property with Include.",
                nameof(definition));
        }

        var properties = new GrainIndexQueryProperty[declared.Count];
        var names = new string[declared.Count];
        for (var i = 0; i < declared.Count; i++)
        {
            var property = declared[i];
            properties[i] = new GrainIndexQueryProperty(i, property.Name, property.PropertyType);
            names[i] = property.Name;
        }

        return (properties, names);
    }
}
