namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// Options for the optional <c>Orleans.Lattice.Api.Schema</c> add-on, the
/// transport-agnostic schema-management control facade. Bound by
/// <see cref="LatticeApiSchemaServiceCollectionExtensions.AddLatticeSchemaApi"/>
/// and resolvable via <c>IOptions&lt;LatticeApiSchemaOptions&gt;</c>.
/// </summary>
/// <remarks>
/// The facade currently has no tunable knobs; the type is reserved so later
/// releases can add read-bounding or audit-tuning options without changing the
/// registration front door, mirroring the sibling control-API facades.
/// </remarks>
public sealed class LatticeApiSchemaOptions
{
}
