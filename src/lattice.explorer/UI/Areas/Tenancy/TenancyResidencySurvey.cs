namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>How many reachable tenants have no residency set, as the Tenancy Home status reports it.</summary>
/// <param name="Unset">The tenants read with no resident region.</param>
/// <param name="IsPartial">Whether only the first tenants were read, so the count is a lower bound.</param>
internal readonly record struct TenancyResidencySurvey(int Unset, bool IsPartial);
