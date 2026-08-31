using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Turns one grain's typed state into the set of index entries it contributes,
/// and reconciles that set against what the grain contributed last time.
/// </summary>
/// <remarks>
/// <para>
/// A projector is built once per index, at silo-setup time, and then invoked on
/// the projection path: once per indexed grain per mutation. It holds no
/// per-grain state, so one instance serves every grain in the index and is safe
/// to call concurrently.
/// </para>
/// <para>
/// Projection reads each property through the compiled accessor the declaration
/// already captured - nothing is compiled, reflected, or boxed here - and hands
/// the value straight to the entry writer, which builds the key with
/// <see cref="GrainIndexKeyEncoder"/> and the payload with the field-name
/// contract in <see cref="GrainIndexEntryValue"/>.
/// </para>
/// <para>
/// Writing the entries is deliberately not this type's job: a projector is pure
/// and synchronous, and <see cref="GrainIndexMaintainer{TGrain, TState}"/>
/// applies what it produces. That split is what lets an enrolment hook project
/// and diff without an <c>await</c>, and lets a test assert the exact entry set
/// without a tree.
/// </para>
/// </remarks>
/// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
/// <typeparam name="TState">The grain-state type the index projects from.</typeparam>
public sealed class GrainIndexProjector<TGrain, TState>
    where TGrain : IGrain
{
    /// <summary>
    /// The index's pre-built telemetry tag, resolved once at construction so the
    /// projection path never builds one.
    /// </summary>
    private readonly KeyValuePair<string, object?> _indexTag;

    /// <summary>Initialises a projector over <paramref name="definition"/>.</summary>
    /// <param name="definition">The index definition to project. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="definition"/> is <c>null</c>.</exception>
    public GrainIndexProjector(GrainIndexDefinition<TGrain, TState> definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        Definition = definition;
        _indexTag = GrainIndexMetrics.IndexTag(definition.Name);
    }

    /// <summary>The index definition this projector projects.</summary>
    public GrainIndexDefinition<TGrain, TState> Definition { get; }

    /// <summary>
    /// Projects <paramref name="state"/> into one index entry per declared
    /// property, each pointing back at <paramref name="grainKey"/>.
    /// </summary>
    /// <param name="grainKey">The encoded grain key, as produced by the definition's key codec. Must not be <c>null</c>.</param>
    /// <param name="state">The grain state to project. Must not be <c>null</c>.</param>
    /// <returns>The grain's complete current projection.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexProjection Project(string grainKey, TState state)
    {
        ArgumentNullException.ThrowIfNull(grainKey);
        ThrowIfStateNull(state);

        var properties = Definition.Properties;
        if (properties.Count == 0)
            return GrainIndexProjection.Empty(grainKey);

        var writer = GrainIndexEntryWriter.Rent();
        try
        {
            writer.Begin(grainKey, properties.Count);
            for (var i = 0; i < properties.Count; i++)
                properties[i].AppendEntry(writer, state);

            return new GrainIndexProjection(grainKey, writer.Complete());
        }
        finally
        {
            GrainIndexEntryWriter.Return(writer);
        }
    }

    /// <summary>
    /// Projects <paramref name="state"/> for the grain identified by
    /// <paramref name="grainId"/>, encoding the identity with the definition's
    /// key codec.
    /// </summary>
    /// <param name="grainId">The indexed grain's identity.</param>
    /// <param name="state">The grain state to project. Must not be <c>null</c>.</param>
    /// <returns>The grain's complete current projection.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <c>null</c>.</exception>
    /// <exception cref="GrainIndexKeyEncodingException">The grain's key cannot be encoded by the definition's codec.</exception>
    public GrainIndexProjection Project(GrainId grainId, TState state) =>
        Project(Definition.KeyCodec.Encode(grainId), state);

    /// <summary>
    /// Projects <paramref name="state"/> and reconciles it against
    /// <paramref name="previous"/>, yielding the entries to write and the stale
    /// keys to tombstone.
    /// </summary>
    /// <param name="previous">
    /// The projection last written for this grain. Pass
    /// <see cref="GrainIndexProjection.Empty(string)"/> when the grain has
    /// never been indexed. Must not be <c>null</c>.
    /// </param>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="state">The grain state to project. Must not be <c>null</c>.</param>
    /// <returns>
    /// The update plan. It is <see cref="GrainIndexUpdatePlan.IsEmpty"/> when
    /// the state produces exactly the entries it produced last time, so
    /// re-projecting unchanged state costs nothing downstream.
    /// </returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexUpdatePlan Plan(GrainIndexProjection previous, string grainKey, TState state)
    {
        ArgumentNullException.ThrowIfNull(previous);

        // The timestamp is only taken when something is listening, so an
        // unsubscribed process pays one predictable branch per projection and
        // nothing else.
        if (!GrainIndexMetrics.ProjectionDuration.Enabled)
            return GrainIndexUpdatePlan.Between(previous, Project(grainKey, state));

        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        var plan = GrainIndexUpdatePlan.Between(previous, Project(grainKey, state));
        GrainIndexMetrics.RecordProjectionDuration(_indexTag, started);
        return plan;
    }

    /// <summary>
    /// Projects <paramref name="state"/> for <paramref name="grainId"/> and
    /// reconciles it against <paramref name="previous"/>.
    /// </summary>
    /// <param name="previous">The projection last written for this grain. Must not be <c>null</c>.</param>
    /// <param name="grainId">The indexed grain's identity.</param>
    /// <param name="state">The grain state to project. Must not be <c>null</c>.</param>
    /// <returns>The update plan.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    /// <exception cref="GrainIndexKeyEncodingException">The grain's key cannot be encoded by the definition's codec.</exception>
    public GrainIndexUpdatePlan Plan(GrainIndexProjection previous, GrainId grainId, TState state) =>
        Plan(previous, Definition.KeyCodec.Encode(grainId), state);

    /// <summary>
    /// Rejects a null state without boxing a value-type one.
    /// <c>ArgumentNullException.ThrowIfNull</c> takes <c>object?</c>, so passing
    /// an unconstrained generic to it would box the state on every projection.
    /// The <c>default(TState) is null</c> guard is a JIT-time constant, so the
    /// whole check disappears for a value-type state.
    /// </summary>
    private static void ThrowIfStateNull(TState state)
    {
        if (default(TState) is null && state is null)
            throw new ArgumentNullException(nameof(state));
    }
}
