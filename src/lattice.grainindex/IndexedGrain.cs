using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// An optional convenience base class shaped like <c>Grain&lt;TState&gt;</c>,
/// for a grain that would otherwise have used it and wants the same
/// <c>State</c> / <c>WriteStateAsync</c> ergonomics while being indexed.
/// </summary>
/// <remarks>
/// <para>
/// It is a convenience, not the mechanism. <see cref="IndexedAttribute"/> is
/// what enrols a grain, and it works on any grain that receives its state
/// through a constructor parameter - including one that already derives from
/// something else. This class contains no enrolment logic whatsoever: it holds
/// the state object the attribute produced and forwards to it.
/// </para>
/// <para>
/// It exists because <c>Grain&lt;TState&gt;</c> resolves its storage internally
/// rather than through a constructor parameter, so there is nothing on such a
/// grain to annotate. Moving to this base class is the smallest change that
/// makes it annotatable, and it leaves every <c>State</c> and
/// <c>WriteStateAsync()</c> call site in the grain unchanged:
/// </para>
/// <code>
/// public sealed class UserGrain(
///     [Indexed("user")] IPersistentState&lt;UserState&gt; state)
///     : IndexedGrain&lt;UserState&gt;(state), IUserGrain
/// {
///     public async Task SetAgeAsync(int age)
///     {
///         State.Age = age;
///         await WriteStateAsync();
///     }
/// }
/// </code>
/// <para>
/// Prefer using <see cref="IndexedAttribute"/> on its own and keeping the state
/// object in hand: it is one less base class in the way, and it makes the grain's
/// persistence explicit at the point of use.
/// </para>
/// </remarks>
/// <typeparam name="TState">The grain-state type.</typeparam>
public abstract class IndexedGrain<TState> : Grain
{
    private readonly IPersistentState<TState> _state;

    /// <summary>
    /// Initialises the grain over the state object its constructor received.
    /// </summary>
    /// <param name="state">
    /// The grain's persistent state, which must be the parameter annotated with
    /// <see cref="IndexedAttribute"/> for the grain to be indexed. Must not be
    /// <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <c>null</c>.</exception>
    protected IndexedGrain(IPersistentState<TState> state)
    {
        ArgumentNullException.ThrowIfNull(state);
        _state = state;
    }

    /// <summary>The grain's state, as <c>Grain&lt;TState&gt;.State</c> exposes it.</summary>
    protected TState State
    {
        get => _state.State;
        set => _state.State = value;
    }

    /// <summary>
    /// Whether the storage provider holds a record for this grain. It is
    /// <c>false</c> for a grain that has never been written, which
    /// <c>Grain&lt;TState&gt;</c> gives no way to tell.
    /// </summary>
    protected bool RecordExists => _state.RecordExists;

    /// <summary>
    /// The storage provider's concurrency tag for the current state, or
    /// <c>null</c> when the provider does not supply one.
    /// </summary>
    protected string? Etag => _state.Etag;

    /// <summary>
    /// The underlying state object, for a grain that needs to hand it to
    /// something else.
    /// </summary>
    protected IPersistentState<TState> PersistentState => _state;

    /// <summary>
    /// Commits the grain's state and, when the state parameter carries
    /// <see cref="IndexedAttribute"/>, publishes the resulting index entries.
    /// </summary>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the state, and any index entries, are durable.</returns>
    protected Task WriteStateAsync(CancellationToken cancellationToken = default) =>
        _state.WriteStateAsync(cancellationToken);

    /// <summary>
    /// Re-reads the grain's state from storage, reconciling its index entries
    /// against what it finds.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>A task that completes when the state has been re-read.</returns>
    protected Task ReadStateAsync(CancellationToken cancellationToken = default) =>
        _state.ReadStateAsync(cancellationToken);

    /// <summary>
    /// Deletes the grain's stored state and withdraws its entries from every
    /// index it was enrolled in.
    /// </summary>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the state and the entries are gone.</returns>
    protected Task ClearStateAsync(CancellationToken cancellationToken = default) =>
        _state.ClearStateAsync(cancellationToken);
}
