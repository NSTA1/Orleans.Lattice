using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// Every declared index that projects <typeparamref name="TState"/>, resolved
/// once per silo and then filtered per grain class.
/// </summary>
/// <remarks>
/// <para>
/// A state object knows its state type at compile time but only meets the grain
/// implementing it at activation, so the set is built eagerly per state type and
/// narrowed lazily per grain class. The narrowing is memoised, so a grain class
/// pays for the interface test once per silo rather than once per activation.
/// </para>
/// <para>
/// Closing <see cref="TypedGrainIndexEnroller{TGrain, TState}"/> over a
/// declaration's grain interface needs reflection, because that interface is a
/// run-time value carried on the declaration. It happens here, once per index,
/// at silo setup - the same place the declaration surface already resolves key
/// codecs reflectively - and never on an activation or write path.
/// </para>
/// </remarks>
/// <typeparam name="TState">The grain-state type.</typeparam>
internal sealed class GrainIndexEnrollmentSet<TState>
{
    private static readonly GrainIndexEnroller<TState>[] None = [];

    private readonly GrainIndexEnroller<TState>[] _declared;
    private readonly ConcurrentDictionary<Type, GrainIndexEnroller<TState>[]> _byGrainClass = new();

    /// <summary>Builds the set from the silo's declaration set.</summary>
    /// <param name="declarations">Every declared index. Must not be <c>null</c>.</param>
    /// <param name="options">The per-index options monitor. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">Resolves each index's backing tree. Must not be <c>null</c>.</param>
    /// <param name="store">The registry-backed enrolment bookkeeping. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexEnrollmentSet(
        IOptions<GrainIndexDeclarationOptions> declarations,
        IOptionsMonitor<GrainIndexOptions> options,
        IGrainFactory grainFactory,
        IGrainIndexEnrollmentStore store)
    {
        ArgumentNullException.ThrowIfNull(declarations);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(store);

        var definitions = declarations.Value.Definitions;
        List<GrainIndexEnroller<TState>>? matched = null;

        for (var i = 0; i < definitions.Count; i++)
        {
            var definition = definitions[i];
            if (definition.StateType != typeof(TState))
                continue;

            var indexOptions = options.Get(definition.Name);
            (matched ??= []).Add(Build(definition, indexOptions, grainFactory, store));
        }

        _declared = matched?.ToArray() ?? None;
    }

    /// <summary>
    /// Initialises a set from enrollers built by the caller, which is the form
    /// to use when the indexes under test are constructed directly rather than
    /// resolved from a silo's declaration set.
    /// </summary>
    /// <param name="declared">The enrollers this set offers. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="declared"/> is <c>null</c>.</exception>
    internal GrainIndexEnrollmentSet(GrainIndexEnroller<TState>[] declared)
    {
        ArgumentNullException.ThrowIfNull(declared);
        _declared = declared;
    }

    /// <summary>
    /// Whether any declared index projects <typeparamref name="TState"/> at all.
    /// When it does not, a state object of that type skips enrolment entirely.
    /// </summary>
    public bool IsEmpty => _declared.Length == 0;

    /// <summary>
    /// The declared indexes whose grain interface <paramref name="grainInstance"/>
    /// implements.
    /// </summary>
    /// <param name="grainInstance">The activating grain, which may be <c>null</c>.</param>
    /// <returns>The applicable enrollers, empty when none apply.</returns>
    public GrainIndexEnroller<TState>[] For(object? grainInstance)
    {
        if (_declared.Length == 0 || grainInstance is null)
            return None;

        return _byGrainClass.GetOrAdd(grainInstance.GetType(), static (_, arg) => Filter(arg.Declared, arg.Instance), (Declared: _declared, Instance: grainInstance));
    }

    private static GrainIndexEnroller<TState>[] Filter(
        GrainIndexEnroller<TState>[] declared,
        object grainInstance)
    {
        List<GrainIndexEnroller<TState>>? matched = null;
        for (var i = 0; i < declared.Length; i++)
        {
            if (declared[i].AppliesTo(grainInstance))
                (matched ??= []).Add(declared[i]);
        }

        return matched?.ToArray() ?? None;
    }

    private static GrainIndexEnroller<TState> Build(
        IGrainIndexDefinition definition,
        GrainIndexOptions options,
        IGrainFactory grainFactory,
        IGrainIndexEnrollmentStore store)
    {
        var tree = grainFactory.GetGrain<ILattice>(options.TreeName);
        var enrollerType = typeof(TypedGrainIndexEnroller<,>)
            .MakeGenericType(definition.GrainInterfaceType, typeof(TState));

        return (GrainIndexEnroller<TState>)Activator.CreateInstance(
            enrollerType,
            definition,
            tree,
            store,
            options.ProjectionMode)!;
    }
}
