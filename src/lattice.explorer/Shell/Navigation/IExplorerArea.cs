namespace Orleans.Lattice.Explorer.Shell.Navigation;

/// <summary>
/// One shell-owned native area: a stop on the directory spine, a root segment of
/// the address grammar, and optionally a source of address completions and
/// palette commands (epic decision E2).
/// </summary>
/// <remarks>
/// <para>
/// The contract is <see langword="internal"/> on purpose, and nothing public lets
/// another assembly register an area or a completion source: an area
/// registration API is a plugin API by another name. An area registers itself
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
