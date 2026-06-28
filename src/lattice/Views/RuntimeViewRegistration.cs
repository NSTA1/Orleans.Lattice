namespace Orleans.Lattice.Views;

/// <summary>
/// Durable record of a materialised view created at runtime through
/// <see cref="ILatticeViewFactory.Create"/>. Persisted by the
/// <see cref="IViewRegistryGrain"/> so a runtime view can be re-registered into
/// the in-memory <see cref="IViewCatalog"/> and have its maintainer re-activated
/// after a silo restart, giving runtime views the same restart-durability that
/// startup-declared views get from <c>AddLatticeViews</c>.
/// <para>
/// A projection instance cannot be serialized, so only its <em>identity</em> is
/// recorded: the concrete CLR type (<see cref="ProjectionTypeName"/>) and the
/// stable <see cref="ProjectionVersion"/>. On re-hydration the projection is
/// resolved from the silo service provider by that type; a runtime view therefore
/// survives a restart only when its projection type is resolvable from DI (either
/// registered there or constructable with DI-satisfiable constructor arguments).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RuntimeViewRegistration)]
[Immutable]
internal sealed record RuntimeViewRegistration
{
    /// <summary>The logical view name; the view tree is <c>view-{ViewName}</c>.</summary>
    [Id(0)]
    public required string ViewName { get; init; }

    /// <summary>The source tree id whose WAL the view tails.</summary>
    [Id(1)]
    public required string SourceTreeId { get; init; }

    /// <summary>
    /// The projection's concrete CLR type, captured as an
    /// <see cref="System.Type.AssemblyQualifiedName"/> so it can be re-resolved
    /// from the silo service provider on re-hydration.
    /// </summary>
    [Id(2)]
    public required string ProjectionTypeName { get; init; }

    /// <summary>The projection's stable version at the time the view was created.</summary>
    [Id(3)]
    public required string ProjectionVersion { get; init; }

    /// <summary>Whether this view is an aggregation (grouped reduce) view.</summary>
    [Id(4)]
    public bool IsAggregation { get; init; }

    /// <summary>
    /// Whether this view is append-only (a durable history substrate). Restored
    /// onto the re-hydrated <see cref="ViewRegistration"/> so the maintainer keeps
    /// its non-destructive guard behaviour across a silo restart.
    /// </summary>
    [Id(5)]
    public bool Accumulative { get; init; }
}
