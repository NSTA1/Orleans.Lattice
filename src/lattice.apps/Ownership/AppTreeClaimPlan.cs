namespace Orleans.Lattice.Apps;

/// <summary>One tree an install claims: the ledger key, the manifest's app-local name, and the claim kind.</summary>
/// <param name="Key">The tree's effective (tenant-composed) id, the ledger key.</param>
/// <param name="TreeName">The app-local tree name the manifest declares.</param>
/// <param name="Kind">Structural or adopted.</param>
internal readonly record struct AppTreeClaimPlan(string Key, string TreeName, AppTreeClaimKind Kind);
