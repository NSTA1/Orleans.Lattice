using Orleans.Concurrency;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The persisted, serializable shape of a whole grain-index declaration: which
/// grain type is indexed, which state type it projects from, which tree backs
/// it, which properties are projected, and whether its tree may be replicated
/// across clusters.
/// <para>
/// This is the form the index registry stores. Comparing a live declaration's
/// descriptor against the stored one is how a later activation detects that a
/// declaration has drifted from the data already written under it.
/// </para>
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexDescriptor)]
public sealed class GrainIndexDescriptor
{
    /// <summary>
    /// Initialises a new descriptor.
    /// </summary>
    /// <param name="name">The logical index name. Must not be <c>null</c>.</param>
    /// <param name="treeName">The lattice tree backing the index. Must not be <c>null</c>.</param>
    /// <param name="grainInterfaceTypeName">The indexed grain interface's CLR type name. Must not be <c>null</c>.</param>
    /// <param name="stateTypeName">The projected grain-state CLR type name. Must not be <c>null</c>.</param>
    /// <param name="properties">The projected properties, in declaration order. Must not be <c>null</c>.</param>
    /// <param name="allowReplication">Whether the index's tree may be replicated across clusters.</param>
    /// <exception cref="ArgumentNullException">Any reference argument is <c>null</c>.</exception>
    public GrainIndexDescriptor(
        string name,
        string treeName,
        string grainInterfaceTypeName,
        string stateTypeName,
        IReadOnlyList<GrainIndexPropertyDescriptor> properties,
        bool allowReplication)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(grainInterfaceTypeName);
        ArgumentNullException.ThrowIfNull(stateTypeName);
        ArgumentNullException.ThrowIfNull(properties);
        Name = name;
        TreeName = treeName;
        GrainInterfaceTypeName = grainInterfaceTypeName;
        StateTypeName = stateTypeName;
        Properties = properties;
        AllowReplication = allowReplication;
    }

    /// <summary>The logical index name, unique within the silo.</summary>
    [Id(0)]
    public string Name { get; }

    /// <summary>
    /// The lattice tree backing the index. Always inside the
    /// <see cref="GrainIndexTreeNames.ReservedPrefix"/> namespace.
    /// </summary>
    [Id(1)]
    public string TreeName { get; }

    /// <summary>The indexed grain interface's CLR type name.</summary>
    [Id(2)]
    public string GrainInterfaceTypeName { get; }

    /// <summary>The projected grain-state CLR type name.</summary>
    [Id(3)]
    public string StateTypeName { get; }

    /// <summary>
    /// The projected properties, in the order they were declared with
    /// <c>Include</c>. Never empty for a valid declaration.
    /// </summary>
    [Id(4)]
    public IReadOnlyList<GrainIndexPropertyDescriptor> Properties { get; }

    /// <summary>
    /// Whether the index's tree may be replicated across clusters. A grain index
    /// points at grain activations in one cluster, so this defaults to
    /// <c>false</c> and is an explicit, deliberate opt-in.
    /// </summary>
    [Id(5)]
    public bool AllowReplication { get; }
}
