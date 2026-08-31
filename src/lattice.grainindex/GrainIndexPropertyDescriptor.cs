using Orleans.Concurrency;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The persisted, serializable shape of one projected property of a grain
/// index: the state property's name and its declared CLR type. This is the form
/// stored in the index registry, so a later activation can compare a live
/// declaration against the stored one and detect drift, and so the entry
/// encoder knows which CLR type the value it is encoding has.
/// </summary>
/// <remarks>
/// The descriptor deliberately carries the declared type <i>name</i> rather
/// than a <see cref="Type"/>: the stored form must survive a process that has
/// not loaded the state assembly, and a name compares for drift without
/// resolving anything.
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexPropertyDescriptor)]
public readonly record struct GrainIndexPropertyDescriptor
{
    /// <summary>
    /// Initialises a new descriptor.
    /// </summary>
    /// <param name="name">The projected state property's name. Must not be <c>null</c>.</param>
    /// <param name="propertyTypeName">The property's declared CLR type name. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexPropertyDescriptor(string name, string propertyTypeName)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(propertyTypeName);
        Name = name;
        PropertyTypeName = propertyTypeName;
    }

    /// <summary>The projected state property's name.</summary>
    [Id(0)]
    public string Name { get; init; }

    /// <summary>
    /// The property's declared CLR type name, as
    /// <see cref="Type.FullName"/> (falling back to the simple type name for a
    /// type with no full name).
    /// </summary>
    [Id(1)]
    public string PropertyTypeName { get; init; }
}
