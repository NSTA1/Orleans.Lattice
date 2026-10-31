namespace Orleans.Lattice.Views;

/// <summary>
/// A view maintainer's apply lag and active generation tree id in one value,
/// returned by <see cref="IViewMaintainerGrain.GetStatusAsync"/> so a status
/// read costs one maintainer call instead of two.
/// </summary>
/// <param name="Lag">What <see cref="IViewMaintainerGrain.GetLagAsync"/> reports.</param>
/// <param name="ActiveTreeId">What <see cref="IViewMaintainerGrain.GetActiveTreeIdAsync"/> reports.</param>
[GenerateSerializer]
[Alias(TypeAliases.ViewMaintainerStatus)]
[Immutable]
internal readonly record struct ViewMaintainerStatus(
    [property: Id(0)] long Lag,
    [property: Id(1)] string ActiveTreeId);
