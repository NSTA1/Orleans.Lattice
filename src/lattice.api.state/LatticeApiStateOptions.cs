namespace Orleans.Lattice.Api.State;

/// <summary>
/// Options for the optional <c>Orleans.Lattice.Api.State</c> add-on, the
/// read-only cluster state API. Bound by
/// <see cref="LatticeApiStateServiceCollectionExtensions.AddLatticeStateApi"/>
/// and resolvable via <c>IOptions&lt;LatticeApiStateOptions&gt;</c>.
/// </summary>
/// <remarks>
/// The type is intentionally empty at the scaffolding stage; later issues in
/// the cluster-state-API epic add knobs (paging budgets, value-preview caps,
/// sampling cadences, the authorization posture, and so on) without changing
/// the registration front door.
/// </remarks>
public sealed class LatticeApiStateOptions
{
}
