namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// One shell-owned native area: a stop on the directory spine, a root segment of
/// the address grammar, and optionally a source of address completions and
/// palette commands (epic decision E2).
/// </summary>
/// <remarks>
/// <para>
/// The contract is <see langword="internal"/> on purpose, and nothing public lets
/// another assembly register an area or a completion source: an area
/// registration API would be an external extension model by another name. An
/// area registers itself
/// from its own <c>ShellServiceCollectionExtensions.&lt;Area&gt;.cs</c> partial
/// with <see cref="ExplorerAreaServiceCollectionExtensions.AddExplorerArea{TArea}"/>.
/// </para>
/// <para>
/// Areas are resolved per circuit. <see cref="ExplorerAreaDirectory"/> asks each
/// one for its availability, time-boxed, and treats a fault, a cancellation or a
/// timeout as <see cref="AreaAvailability.Hidden"/>: an area that cannot say it
/// may be seen is not shown.
/// </para>
/// </remarks>
internal interface IExplorerArea
{
    /// <summary>
    /// The area's lower-case route segment and stable identity, such as
    /// <c>data</c>: a lower-case letter followed by lower-case letters, digits and
    /// hyphens.
    /// </summary>
    string Key { get; }

    /// <summary>The area's name on the spine and on Home, such as "Data".</summary>
    string DisplayName { get; }

    /// <summary>The area's position on the directory spine; lower comes first, ties by <see cref="Key"/>.</summary>
    int DirectoryOrder { get; }

    /// <summary>
    /// Whether the area's content follows the active tenant, so its address is
    /// rooted at <c>/t/{tenant}</c> when tenancy is on. Cluster-wide areas return
    /// <see langword="false"/> and are never tenant-rooted.
    /// </summary>
    bool IsTenantScoped => true;

    /// <summary>
    /// Whether <paramref name="address"/>, an address in this area, follows the
    /// active tenant. Defaults to <see cref="IsTenantScoped"/>; an area whose
    /// pages are rooted differently - a cluster-wide directory beside a
    /// tenant-rooted workspace - answers per address. The navigator asks this,
    /// not <see cref="IsTenantScoped"/>, when it canonicalizes, resolves or
    /// re-roots an address.
    /// </summary>
    /// <param name="address">An address whose area is this one.</param>
    bool IsTenantScopedAt(Address.ExplorerAddress address) => IsTenantScoped;

    /// <summary>
    /// How the address line groups the path of <paramref name="address"/>, an
    /// address in this area, into chain nodes: the number of consecutive path
    /// segments each node spans, outermost first. <see langword="null"/>, the
    /// default, is one node per segment. An area whose path holds a logical tree
    /// id answers so the whole id - <c>t/acme/a/crm/orders</c> - is one node.
    /// </summary>
    /// <remarks>
    /// The spans must be positive and sum to the path's length; an answer that
    /// does not is ignored and the path falls back to one node per segment.
    /// </remarks>
    /// <param name="address">An address whose area is this one.</param>
    IReadOnlyList<int>? GetChainSpans(Address.ExplorerAddress address) => null;

    /// <summary>
    /// The area's completion source for the address line, or <see langword="null"/>
    /// for none. Only a <see cref="AreaAvailabilityKind.Visible"/> area is asked.
    /// </summary>
    IAddressCompletionSource? Completions => null;

    /// <summary>
    /// The commands the area contributes to the palette. Every one must also be a
    /// visible control in the area, carrying <c>data-lt-command="{Id}"</c>.
    /// </summary>
    IReadOnlyList<ExplorerCommand> Commands => [];

    /// <summary>
    /// Whether this caller may see the area now. Answer from the area's facade
    /// capability probe; do not turn a probe failure into
    /// <see cref="AreaAvailability.Visible"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancelled when the directory stops waiting.</param>
    ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken);

    /// <summary>
    /// One plain sentence for the Home estate overview, such as "12 trees, 2 with
    /// dead letters", or <see langword="null"/> for none.
    /// </summary>
    /// <param name="cancellationToken">Cancelled when Home stops waiting.</param>
    ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken) => ValueTask.FromResult<string?>(null);

    /// <summary>
    /// A very short figure shown beside the area's stop on the spine, such as
    /// <c>1,204</c> trees or <c>2 lag</c>, or <see langword="null"/> for none. Keep
    /// it to a few characters and cheap: the directory asks for it, time-boxed, on
    /// every navigation, and only of visible areas.
    /// </summary>
    /// <param name="cancellationToken">Cancelled when the directory stops waiting.</param>
    ValueTask<string?> GetDirectoryBadgeAsync(CancellationToken cancellationToken) => ValueTask.FromResult<string?>(null);
}
