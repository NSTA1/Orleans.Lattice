namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Options for the optional <c>Orleans.Lattice.Api.TreeAdmin</c> add-on, the
/// transport-agnostic tree-administration control facade. Bound by
/// <see cref="LatticeApiTreeAdminServiceCollectionExtensions.AddLatticeTreeAdminApi"/>
/// and resolvable via <c>IOptions&lt;LatticeApiTreeAdminOptions&gt;</c>.
/// </summary>
/// <remarks>
/// The facade currently has no tunable knobs; the type is reserved so future
/// bounding or audit-tuning options can be added without changing the registration
/// front door, mirroring the sibling control-API facades.
/// </remarks>
public sealed class LatticeApiTreeAdminOptions
{
}
