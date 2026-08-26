namespace Orleans.Lattice;

/// <summary>
/// Payload delivered to every registered <see cref="ITreeAliasObserver"/>
/// when the tree registry repoints a logical tree's physical-identity alias
/// to a different physical tree - the event a shadow-cutover restore, a
/// resize, or a reshard produces when it swaps the logical tree onto a
/// freshly minted physical WAL. Carries the logical tree id and both the
/// old and new <b>effective</b> physical ids (an unaliased tree resolves to
/// its own logical id, so a removed alias reports the logical id as the new
/// physical), so a consumer can rebind directly without re-reading the
/// registry.
/// <para>
/// The core library raises this only when the effective physical id
/// actually changed; a no-op re-set of the same alias does not fire it. It
/// is the core-to-replication inversion that lets the cross-cluster shipper
/// rebind reactively instead of polling the registry on every pump tick.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeAliasChange)]
[Immutable]
public readonly record struct TreeAliasChange
{
    /// <summary>The logical tree id whose physical-identity alias changed.</summary>
    [Id(0)]
    public string TreeId { get; init; }

    /// <summary>
    /// The effective physical tree id before the change - the value
    /// <c>ILatticeRegistry.ResolveAsync</c> would have returned for
    /// <see cref="TreeId"/> immediately before the swap. Equal to
    /// <see cref="TreeId"/> when the tree was previously unaliased.
    /// </summary>
    [Id(1)]
    public string OldPhysicalTreeId { get; init; }

    /// <summary>
    /// The effective physical tree id after the change - the value
    /// <c>ILatticeRegistry.ResolveAsync</c> now returns for
    /// <see cref="TreeId"/>. Equal to <see cref="TreeId"/> when the alias
    /// was removed (the tree resolves to itself).
    /// </summary>
    [Id(2)]
    public string NewPhysicalTreeId { get; init; }
}
