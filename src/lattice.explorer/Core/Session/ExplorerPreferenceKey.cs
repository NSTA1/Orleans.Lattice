using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// One declared entry in the Explorer's preference contract: a canonical
/// lower-case <see cref="Name"/>, the <see cref="Scope"/> it is remembered at,
/// and a short human description used when the shell has to explain that a
/// remembered value no longer resolves.
/// </summary>
/// <remarks>
/// <para>
/// Declaring a key is what distinguishes a preference from a stray write. Before
/// this contract the Explorer persisted state through ad hoc
/// <c>SetAsync("detail-plugin", ...)</c> calls scattered across components, so
/// nobody could answer "what does the Explorer remember about me?" without
/// grepping, and nothing could reliably clear it. A key declared here is
/// enumerable, scoped, resettable and documented by construction.
/// </para>
/// <para>
/// Keys are compared by reference: declare each one exactly once as a
/// <c>static readonly</c> field (see <see cref="ExplorerPreferenceKeys"/> for the
/// shell's own) and pass that instance around. Two different instances with the
/// same name are a declaration bug, and
/// <see cref="IExplorerPreferenceCatalog.Register"/> rejects the second.
/// </para>
/// <para>
/// The name shares the route grammar's canonical-lower-case rule
/// (<see cref="ExplorerRouteSlug"/>), so the shell has exactly one spelling
/// convention across URLs and stored state, and one hygiene assertion guards
/// both.
/// </para>
/// </remarks>
public sealed class ExplorerPreferenceKey
{
    /// <summary>
    /// Declares a preference key.
    /// </summary>
    /// <param name="name">
    /// The canonical lower-case key name, conventionally dotted and prefixed by
    /// the owning feature (for example <c>shell.area</c>).
    /// </param>
    /// <param name="description">
    /// A short noun phrase naming what is remembered, from the user's point of
    /// view - "the area you were last in". Used verbatim in the sentence the
    /// shell shows when the remembered value no longer resolves, so write it to
    /// read well mid-sentence.
    /// </param>
    /// <param name="scope">
    /// How widely the value applies. Defaults to
    /// <see cref="ExplorerPreferenceScope.UserAndCluster"/>, which is correct for
    /// anything naming something inside a cluster.
    /// </param>
    /// <exception cref="ArgumentException">
    /// <paramref name="name"/> is not canonical lower case, or
    /// <paramref name="description"/> is <see langword="null"/> or empty.
    /// </exception>
    public ExplorerPreferenceKey(
        string name,
        string description,
        ExplorerPreferenceScope scope = ExplorerPreferenceScope.UserAndCluster)
    {
        ExplorerRouteSlug.EnsureCanonical(name);
        ArgumentException.ThrowIfNullOrEmpty(description);

        Name = name;
        Description = description;
        Scope = scope;
    }

    /// <summary>The canonical lower-case key name, unique across the contract.</summary>
    public string Name { get; }

    /// <summary>
    /// A short noun phrase naming what is remembered, used to explain a fallback
    /// to the user.
    /// </summary>
    public string Description { get; }

    /// <summary>How widely the value applies, and therefore how the stored key is scoped.</summary>
    public ExplorerPreferenceScope Scope { get; }

    /// <inheritdoc />
    public override string ToString() => Name;
}
