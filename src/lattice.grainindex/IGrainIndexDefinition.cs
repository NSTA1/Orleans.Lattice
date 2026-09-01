namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The grain-type-agnostic view of one declared grain index, so a host can hold
/// every declaration in a single list and describe, validate, or persist it
/// without knowing the grain and state types at compile time.
/// </summary>
/// <remarks>
/// The strongly typed form is
/// <see cref="GrainIndexDefinition{TGrain, TState}"/>, which additionally
/// exposes the typed key codec and the typed projected properties.
/// </remarks>
public interface IGrainIndexDefinition
{
    /// <summary>The logical index name, unique within the silo.</summary>
    string Name { get; }

    /// <summary>The indexed grain interface type.</summary>
    Type GrainInterfaceType { get; }

    /// <summary>The grain-state type the index projects from.</summary>
    Type StateType { get; }

    /// <summary>
    /// The codec that turns an indexed grain's identity into the string an index
    /// entry stores, and back again.
    /// </summary>
    IGrainKeyCodec KeyCodec { get; }

    /// <summary>
    /// The persisted form of each projected property, in declaration order.
    /// Never empty for a valid declaration.
    /// </summary>
    IReadOnlyList<GrainIndexPropertyDescriptor> PropertyDescriptors { get; }

    /// <summary>
    /// Combines this declaration's shape with the resolved options for the same
    /// index to produce the persistable descriptor.
    /// </summary>
    /// <param name="options">The resolved options for this index. Must not be <c>null</c>.</param>
    /// <returns>The descriptor to persist in the index registry.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <c>null</c>.</exception>
    GrainIndexDescriptor Describe(GrainIndexOptions options);
}
