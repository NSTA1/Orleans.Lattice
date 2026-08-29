using Orleans.Lattice.Explorer.Schema;

namespace Orleans.Lattice.Explorer.Schema.Domain;

/// <summary>
/// The state one operator's Schema area shares across its concern-scoped
/// components: the domain model they drive, the plugin-level gate, the selected
/// tree and its scoped grants, whether a request is in flight, and the last
/// operation's outcome.
/// <para>
/// Splitting the area by concern needs exactly one shared object rather than a
/// cascade of parameters: the panel owns a session, each tab takes it as its
/// single parameter, and a tab that mutates it raises
/// <see cref="Changed"/> so the panel re-renders the whole area once. One
/// allocation per panel instance, none per render.
/// </para>
/// </summary>
/// <param name="domain">The controlled domain model the area operates against.</param>
public sealed class SchemaSession(ISchemaPluginDomain domain)
{
    private readonly ISchemaPluginDomain _domain = domain ?? throw new ArgumentNullException(nameof(domain));

    /// <summary>Raised after any shared state changes, so the panel can re-render.</summary>
    public event Action? Changed;

    /// <summary>The controlled domain model. Never <see langword="null"/>.</summary>
    public ISchemaPluginDomain Domain => _domain;

    /// <summary>
    /// The plugin-level decision: whether the schema control endpoint answered
    /// the coarse reachability probe. Fail-closed until the host's refresher
    /// files a decision.
    /// </summary>
    public bool IsAllowed { get; set; }

    /// <summary>
    /// The tree currently selected in the area, or <see langword="null"/> when
    /// none is. Empty and whitespace ids are treated as no selection.
    /// </summary>
    public string? TreeId { get; set; }

    /// <summary>
    /// The scoped per-action decisions for <see cref="TreeId"/>. Starts at
    /// <see cref="SchemaTreeGrants.None"/>, which denies everything, so no
    /// control is interactive before a tree has been probed.
    /// </summary>
    public SchemaTreeGrants Grants { get; set; } = SchemaTreeGrants.None;

    /// <summary>Whether a schema request is currently in flight.</summary>
    public bool IsBusy { get; set; }

    /// <summary>The last mutation's outcome, or <see langword="null"/> when none has run since the last reset.</summary>
    public SchemaOperationResult? LastResult { get; set; }

    /// <summary>
    /// The dead-letter page most recently loaded, or <see langword="null"/> when
    /// none has been.
    /// <para>
    /// It lives on the session rather than inside the dead-letter component
    /// because that component unmounts whenever the operator visits another
    /// concern, and the queue is loaded on an explicit action: discarding it on a
    /// tab round trip would make the operator re-run a read nothing had
    /// invalidated. The page carries the tree it was read for, so it is shown
    /// only while that tree is still selected.
    /// </para>
    /// </summary>
    public SchemaDeadLetterPage? DeadLetters { get; set; }

    /// <summary>
    /// <see langword="true"/> when a tree is selected and its scoped grants have
    /// been probed, which is the point at which the area has something to show.
    /// </summary>
    public bool HasProbedTree => Grants.TreeId is not null;

    /// <summary>
    /// Whether an action gated on <paramref name="capability"/> should render
    /// enabled: nothing in flight, the plugin-level gate open, a tree selected,
    /// and the tree's scoped decision permitting it. Reads the keyed store, so
    /// it always reflects the newest probe rather than a cached snapshot.
    /// </summary>
    /// <param name="capability">The action to test.</param>
    public bool Can(SchemaCapability capability) =>
        !IsBusy && IsAllowed && !string.IsNullOrWhiteSpace(TreeId) && Grants.IsAllowed(capability);

    /// <summary>Raises <see cref="Changed"/>.</summary>
    public void NotifyChanged() => Changed?.Invoke();

    /// <summary>
    /// Runs <paramref name="operation"/> as the area's single in-flight request:
    /// marks the area busy, notifies so every control greys out, then clears the
    /// flag and notifies again whatever the outcome.
    /// </summary>
    /// <param name="operation">The request to run. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="operation"/> is <see langword="null"/>.</exception>
    public async Task RunAsync(Func<Task> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);

        IsBusy = true;
        NotifyChanged();
        try
        {
            await operation().ConfigureAwait(false);
        }
        finally
        {
            IsBusy = false;
            NotifyChanged();
        }
    }
}
