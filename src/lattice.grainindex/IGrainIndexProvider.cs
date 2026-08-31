namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Resolves the typed query surface for a declared grain index. Registered by
/// <c>AddGrainIndex</c> and resolvable anywhere the silo's services are - a
/// grain, a hosted service, or a client configured with the same declarations.
/// <para>Example:</para>
/// <code>
/// var index = provider.GetIndex&lt;IUserGrain, UserState&gt;("users");
/// var adults = await index.Where(u =&gt; u.Age &gt;= 18).ToGrainListAsync();
/// </code>
/// </summary>
public interface IGrainIndexProvider
{
    /// <summary>
    /// The names of the declared indexes, in declaration order.
    /// </summary>
    IReadOnlyList<string> DeclaredIndexes { get; }

    /// <summary>
    /// Returns the query surface for a declared index.
    /// </summary>
    /// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
    /// <typeparam name="TState">The grain-state type the index projects from.</typeparam>
    /// <param name="name">
    /// The index's logical name, or <c>null</c> to select the single index
    /// declared over <typeparamref name="TGrain"/> and
    /// <typeparamref name="TState"/>.
    /// </param>
    /// <returns>The index's query surface. The same instance is returned for repeat calls.</returns>
    /// <exception cref="InvalidOperationException">
    /// No index matches, more than one index matches an omitted
    /// <paramref name="name"/>, or the named index was declared over different
    /// type arguments.
    /// </exception>
    IGrainIndex<TGrain, TState> GetIndex<TGrain, TState>(string? name = null)
        where TGrain : IGrain;
}
