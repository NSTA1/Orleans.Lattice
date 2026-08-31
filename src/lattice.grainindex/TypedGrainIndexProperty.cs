namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// A projected grain-state property whose accessor is strongly typed, so the
/// projection path can read it without boxing.
/// <para>
/// A declaration built with
/// <see cref="GrainIndexBuilder{TGrain, TState}.Include{TProperty}(System.Linq.Expressions.Expression{System.Func{TState, TProperty}})"/>
/// produces one of these per included property: the selector expression is
/// compiled exactly once, at silo-setup time, and the resulting delegate is what
/// <see cref="Accessor"/> holds. Nothing is compiled, cached, or reflected on the
/// projection path.
/// </para>
/// </summary>
/// <typeparam name="TState">The grain-state type the property is read from.</typeparam>
/// <typeparam name="TProperty">The property's declared CLR type.</typeparam>
public sealed class TypedGrainIndexProperty<TState, TProperty> : GrainIndexProperty<TState>
{
    /// <summary>
    /// Initialises a strongly typed projected property.
    /// </summary>
    /// <param name="name">The state property's name. Must not be <c>null</c>, empty, or white space.</param>
    /// <param name="accessor">
    /// The compiled accessor that reads the property. Must not be <c>null</c>,
    /// and must be a pure, side-effect-free read: it runs on the projection path.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TypedGrainIndexProperty(string name, Func<TState, TProperty> accessor)
        : base(name, typeof(TProperty))
    {
        ArgumentNullException.ThrowIfNull(accessor);
        Accessor = accessor;
    }

    /// <summary>
    /// The compiled accessor that reads the property from a state instance.
    /// Compiled once at declaration time and reused for every projection.
    /// </summary>
    public Func<TState, TProperty> Accessor { get; }

    /// <summary>
    /// Reads the property from <paramref name="state"/> without boxing.
    /// </summary>
    /// <param name="state">The grain state to read from.</param>
    /// <returns>The property's current value.</returns>
    public TProperty GetTypedValue(TState state) => Accessor(state);

    /// <inheritdoc />
    public override object? GetValue(TState state) => Accessor(state);
}
