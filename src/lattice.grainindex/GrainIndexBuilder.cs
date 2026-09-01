using System.Linq.Expressions;
using System.Reflection;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The fluent surface a host uses to declare a grain index inside
/// <see cref="GrainIndexServiceCollectionExtensions.AddGrainIndex{TGrain, TState}(Hosting.ISiloBuilder, Action{GrainIndexBuilder{TGrain, TState}})"/>.
/// <para>
/// There is no "index everything" mode: each projected property is opted in
/// deliberately with
/// <see cref="Include{TProperty}(Expression{Func{TState, TProperty}})"/>, which
/// keeps the write amplification of an index visible in the declaration - every
/// mutation of an indexed grain touches one entry per included property.
/// </para>
/// </summary>
/// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
/// <typeparam name="TState">The grain-state type the index projects from.</typeparam>
public sealed class GrainIndexBuilder<TGrain, TState>
    where TGrain : IGrain
{
    private readonly List<GrainIndexProperty<TState>> _properties = [];
    private string? _name;
    private IGrainKeyCodec<TGrain>? _keyCodec;

    /// <summary>The tree name the host set explicitly, or <c>null</c> to use the reserved default.</summary>
    internal string? TreeNameOverride { get; private set; }

    /// <summary>Whether the host opted this index's tree into cross-cluster replication.</summary>
    internal bool AllowReplicationValue { get; private set; }

    /// <summary>The backfill batch size the host set explicitly, or <c>null</c> to keep the option default.</summary>
    internal int? BackfillBatchSizeOverride { get; private set; }

    /// <summary>The backfill interval the host set explicitly, or <c>null</c> to keep the option default.</summary>
    internal TimeSpan? BackfillIntervalOverride { get; private set; }

    /// <summary>
    /// Sets the logical index name, which must be unique within the silo and is
    /// the key the index's options are resolved by. Defaults to the grain
    /// interface's simple type name, so a silo declaring two indexes over the
    /// same grain type must name at least one of them.
    /// </summary>
    /// <param name="name">The index name. Must not be <c>null</c>, empty, or white space.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c>.</exception>
    public GrainIndexBuilder<TGrain, TState> WithName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        _name = name;
        return this;
    }

    /// <summary>
    /// Overrides the lattice tree backing this index. The name must stay inside
    /// the <see cref="GrainIndexTreeNames.ReservedPrefix"/> namespace so the tree
    /// remains identifiable as index-owned; a name outside it is rejected by the
    /// options validator at startup.
    /// </summary>
    /// <param name="treeName">The backing tree name. Must not be <c>null</c>, empty, or white space.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeName"/> is empty or white space.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="treeName"/> is <c>null</c>.</exception>
    public GrainIndexBuilder<TGrain, TState> WithTreeName(string treeName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        TreeNameOverride = treeName;
        return this;
    }

    /// <summary>
    /// Supplies the codec that encodes an indexed grain's identity into an index
    /// entry. When this is not called, the built-in codec matching the grain's
    /// key interface is used, and a grain whose key no built-in codec can encode
    /// is rejected at declaration time with a
    /// <see cref="GrainIndexKeyEncodingException"/>.
    /// </summary>
    /// <param name="keyCodec">The codec to use. Must not be <c>null</c>.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="keyCodec"/> is <c>null</c>.</exception>
    public GrainIndexBuilder<TGrain, TState> WithKeyCodec(IGrainKeyCodec<TGrain> keyCodec)
    {
        ArgumentNullException.ThrowIfNull(keyCodec);
        _keyCodec = keyCodec;
        return this;
    }

    /// <summary>
    /// Opts this index's tree into cross-cluster replication. Grain indexes are
    /// cluster-local by default because they point at grain activations in one
    /// cluster, so this is an explicit, deliberate opt-in.
    /// </summary>
    /// <param name="allow">Whether replication of the index's tree is permitted. Defaults to <c>true</c>.</param>
    /// <returns>This builder, for chaining.</returns>
    public GrainIndexBuilder<TGrain, TState> AllowReplication(bool allow = true)
    {
        AllowReplicationValue = allow;
        return this;
    }

    /// <summary>
    /// Sets the number of grains a single backfill pass visits.
    /// </summary>
    /// <param name="batchSize">The batch size. Must be at least 1.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="batchSize"/> is less than 1.</exception>
    public GrainIndexBuilder<TGrain, TState> WithBackfillBatchSize(int batchSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        BackfillBatchSizeOverride = batchSize;
        return this;
    }

    /// <summary>
    /// Sets the pause between backfill passes, which paces the backfill against
    /// foreground traffic.
    /// </summary>
    /// <param name="interval">The pause between passes. Must be greater than zero.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="interval"/> is not greater than zero.</exception>
    public GrainIndexBuilder<TGrain, TState> WithBackfillInterval(TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        BackfillIntervalOverride = interval;
        return this;
    }

    /// <summary>
    /// Opts one top-level state property into the index.
    /// <para>
    /// The selector must be a direct member access on the state parameter, such
    /// as <c>x =&gt; x.Age</c>. It is compiled exactly once, here at declaration
    /// time, and the resulting delegate is what the projection path invokes; no
    /// expression is compiled, interpreted, or reflected over per projection.
    /// </para>
    /// </summary>
    /// <typeparam name="TProperty">The selected property's declared CLR type.</typeparam>
    /// <param name="selector">The property selector. Must not be <c>null</c>.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="selector"/> is not a direct property access on the state
    /// parameter, or the same property has already been included.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is <c>null</c>.</exception>
    public GrainIndexBuilder<TGrain, TState> Include<TProperty>(Expression<Func<TState, TProperty>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        var propertyName = ResolvePropertyName(selector);
        for (var i = 0; i < _properties.Count; i++)
        {
            if (string.Equals(_properties[i].Name, propertyName, StringComparison.Ordinal))
            {
                throw new ArgumentException(
                    $"Property '{propertyName}' of '{typeof(TState).Name}' is already included in this "
                    + "grain index. Each property is indexed once; a repeated Include is a declaration bug.",
                    nameof(selector));
            }
        }

        _properties.Add(new TypedGrainIndexProperty<TState, TProperty>(propertyName, selector.Compile()));
        return this;
    }

    /// <summary>
    /// Materialises the declaration. Resolves the index name and the key codec,
    /// failing here when the grain's key cannot be encoded, and leaves
    /// set-level checks (empty projection set, duplicate names) to the options
    /// validators so they report every offender at startup with the index name.
    /// </summary>
    /// <returns>The declared definition.</returns>
    /// <exception cref="GrainIndexKeyEncodingException">
    /// No key codec was supplied and no built-in codec matches
    /// <typeparamref name="TGrain"/>.
    /// </exception>
    internal GrainIndexDefinition<TGrain, TState> Build() =>
        new(_name ?? typeof(TGrain).Name,
            _keyCodec ?? GrainKeyCodec.CreateDefault<TGrain>(),
            _properties);

    /// <summary>
    /// Extracts the property name from a selector, requiring a direct property
    /// access on the lambda's own parameter so the projected name is
    /// unambiguous and matches the state class.
    /// </summary>
    private static string ResolvePropertyName<TProperty>(Expression<Func<TState, TProperty>> selector)
    {
        var body = selector.Body;
        while (body is UnaryExpression
               {
                   NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked
               } conversion)
        {
            body = conversion.Operand;
        }

        if (body is MemberExpression { Member: PropertyInfo property } member
            && ReferenceEquals(member.Expression, selector.Parameters[0]))
        {
            return property.Name;
        }

        throw new ArgumentException(
            $"A grain index Include selector must be a direct property access on the state "
            + $"parameter, for example 'x => x.Age'. '{selector}' is not, so there is no single "
            + $"top-level property of '{typeof(TState).Name}' to index.",
            nameof(selector));
    }
}
