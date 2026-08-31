namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// One property of a grain's state that an index projects: its name, its
/// declared CLR type, and the accessor that reads it. Instances are created once
/// per declaration at silo-setup time and then read on the projection path, once
/// per indexed grain per mutation.
/// </summary>
/// <remarks>
/// <para>
/// The declared type is kept alongside the accessor because the entry encoder
/// needs it to choose an order-preserving encoding for the value, and the query
/// router needs it to decide whether a predicate over the property can be turned
/// into a key range.
/// </para>
/// <para>
/// Reading through <see cref="GetValue(TState)"/> boxes a value-type property.
/// A caller on a hot path that knows the property type should down-cast to
/// <see cref="TypedGrainIndexProperty{TState, TProperty}"/> and call
/// <see cref="TypedGrainIndexProperty{TState, TProperty}.GetTypedValue(TState)"/>,
/// which reads through a strongly typed delegate and allocates nothing.
/// </para>
/// </remarks>
/// <typeparam name="TState">The grain-state type the property is read from.</typeparam>
public abstract class GrainIndexProperty<TState>
{
    /// <summary>
    /// Initialises the shared part of a projected property. The constructor is
    /// <c>private protected</c> so the only concrete form is
    /// <see cref="TypedGrainIndexProperty{TState, TProperty}"/>, which keeps the
    /// accessor strongly typed.
    /// </summary>
    /// <param name="name">The state property's name. Must not be <c>null</c>, empty, or white space.</param>
    /// <param name="propertyType">The property's declared CLR type. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    private protected GrainIndexProperty(string name, Type propertyType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(propertyType);
        Name = name;
        PropertyType = propertyType;
        EncodedName = System.Text.Json.JsonEncodedText.Encode(name);
        Descriptor = new GrainIndexPropertyDescriptor(
            name,
            propertyType.FullName ?? propertyType.Name);
    }

    /// <summary>The projected state property's name.</summary>
    public string Name { get; }

    /// <summary>
    /// The property's declared CLR type - the type as written on the state
    /// class, not the runtime type of any particular value.
    /// </summary>
    public Type PropertyType { get; }

    /// <summary>
    /// The persisted, serializable form of this property, computed once at
    /// construction so describing a declaration allocates nothing.
    /// </summary>
    public GrainIndexPropertyDescriptor Descriptor { get; }

    /// <summary>
    /// Reads the property from <paramref name="state"/>. Boxes the value when the
    /// property type is a value type; see the remarks on this class for the
    /// allocation-free alternative.
    /// </summary>
    /// <param name="state">The grain state to read from.</param>
    /// <returns>The property's current value.</returns>
    public abstract object? GetValue(TState state);

    /// <summary>
    /// The property's name, pre-escaped for JSON at declaration time so the
    /// projection path never re-escapes it per entry.
    /// </summary>
    internal System.Text.Json.JsonEncodedText EncodedName { get; }

    /// <summary>
    /// Appends this property's index entry for <paramref name="state"/> to
    /// <paramref name="writer"/>.
    /// <para>
    /// This is the projection path's only route into a property's value, and it
    /// exists so that route stays strongly typed: the override lives on the
    /// typed subclass, which knows the property's CLR type statically and can
    /// hand it to the writer without boxing. Reading through
    /// <see cref="GetValue(TState)"/> instead would box every value-type
    /// property once per grain mutation.
    /// </para>
    /// </summary>
    /// <param name="writer">The entry writer to append to.</param>
    /// <param name="state">The grain state to project.</param>
    internal abstract void AppendEntry(GrainIndexEntryWriter writer, TState state);
}
