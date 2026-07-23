namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Options for the optional <c>Orleans.Lattice.Api.Replication</c> add-on, the
/// transport-agnostic replication control facade. Bound by
/// <see cref="LatticeApiReplicationServiceCollectionExtensions.AddLatticeReplicationApi"/>
/// and resolvable via <c>IOptions&lt;LatticeApiReplicationOptions&gt;</c>.
/// </summary>
/// <remarks>
/// The facade currently exposes no tunable knobs; the type is the stable
/// registration front door so later issues in the replication control-API epic
/// can add configuration without changing the <c>AddLatticeReplicationApi</c>
/// signature.
/// </remarks>
public sealed class LatticeApiReplicationOptions
{
}
