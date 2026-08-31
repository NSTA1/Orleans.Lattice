namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// A declared grain index in its strongly typed form: the grain type indexed,
/// the state type projected from, the codec that encodes the grain's identity,
/// and the projected properties.
/// <para>
/// A definition describes only the <i>shape</i> of an index. The tunable
/// settings that a host may override per index - the backing tree name, the
/// cross-cluster replication opt-in, and the backfill knobs - live in
/// <see cref="GrainIndexOptions"/>, resolved by index name.
/// </para>
/// </summary>
/// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
/// <typeparam name="TState">The grain-state type the index projects from.</typeparam>
public sealed class GrainIndexDefinition<TGrain, TState> : IGrainIndexDefinition
    where TGrain : IGrain
{
    /// <summary>
    /// Initialises a definition. Built by
    /// <see cref="GrainIndexBuilder{TGrain, TState}"/>; the constructor is public
    /// so a host can compose a definition directly when it does not want the
    /// fluent surface.
    /// </summary>
    /// <param name="name">The logical index name. Must not be <c>null</c>, empty, or white space.</param>
    /// <param name="keyCodec">The grain-key codec. Must not be <c>null</c>.</param>
    /// <param name="properties">
    /// The projected properties, in declaration order. Must not be <c>null</c>
    /// and must not contain a <c>null</c> element. May be empty here; an empty
    /// projection set is rejected by the options validator at startup, with the
    /// index name in the message.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space, or <paramref name="properties"/> contains a <c>null</c>.</exception>
    /// <exception cref="ArgumentNullException">Any reference argument is <c>null</c>.</exception>
    public GrainIndexDefinition(
        string name,
        IGrainKeyCodec<TGrain> keyCodec,
        IReadOnlyList<GrainIndexProperty<TState>> properties)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(keyCodec);
        ArgumentNullException.ThrowIfNull(properties);

        var descriptors = new GrainIndexPropertyDescriptor[properties.Count];
        for (var i = 0; i < properties.Count; i++)
        {
            var property = properties[i];
            if (property is null)
            {
                throw new ArgumentException(
                    $"Grain index '{name}' declares a null projected property at position {i}.",
                    nameof(properties));
            }

            descriptors[i] = property.Descriptor;
        }

        Name = name;
        KeyCodec = keyCodec;
        Properties = properties;
        PropertyDescriptors = descriptors;
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public Type GrainInterfaceType => typeof(TGrain);

    /// <inheritdoc />
    public Type StateType => typeof(TState);

    /// <summary>
    /// The codec that turns an indexed grain's identity into the string an index
    /// entry stores, and back into a strongly typed grain reference.
    /// </summary>
    public IGrainKeyCodec<TGrain> KeyCodec { get; }

    /// <summary>
    /// The projected properties, in the order they were declared. Each carries
    /// the property name, its declared CLR type, and the accessor that reads it.
    /// </summary>
    public IReadOnlyList<GrainIndexProperty<TState>> Properties { get; }

    /// <inheritdoc />
    public IReadOnlyList<GrainIndexPropertyDescriptor> PropertyDescriptors { get; }

    /// <inheritdoc />
    IGrainKeyCodec IGrainIndexDefinition.KeyCodec => KeyCodec;

    /// <inheritdoc />
    public GrainIndexDescriptor Describe(GrainIndexOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new GrainIndexDescriptor(
            Name,
            options.TreeName,
            typeof(TGrain).FullName ?? typeof(TGrain).Name,
            typeof(TState).FullName ?? typeof(TState).Name,
            PropertyDescriptors,
            options.AllowReplication);
    }
}
