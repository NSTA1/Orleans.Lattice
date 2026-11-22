namespace Orleans.Lattice.Views;

/// <summary>
/// A view maintainer's apply lag, active generation tree id and lag age in one
/// value, returned by <see cref="IViewMaintainerGrain.GetStatusAsync"/> so a status
/// read costs one maintainer call instead of two.
/// </summary>
/// <param name="Lag">What <see cref="IViewMaintainerGrain.GetLagAsync"/> reports.</param>
/// <param name="ActiveTreeId">What <see cref="IViewMaintainerGrain.GetActiveTreeIdAsync"/> reports.</param>
/// <param name="LagAge">
/// How long the oldest unapplied source entry has been unapplied, as the maintainer
/// observed it: zero when caught up, a lower bound otherwise. <see langword="null"/>
/// from a maintainer that predates the field.
/// </param>
[GenerateSerializer]
[Alias(TypeAliases.ViewMaintainerStatus)]
[Immutable]
internal readonly record struct ViewMaintainerStatus(
    [property: Id(0)] long Lag,
    [property: Id(1)] string ActiveTreeId,
    [property: Id(2)] TimeSpan? LagAge = null);
