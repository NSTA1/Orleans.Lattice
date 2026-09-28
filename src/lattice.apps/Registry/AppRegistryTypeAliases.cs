namespace Orleans.Lattice.Apps;

/// <summary>
/// Stable Orleans serialization aliases for the app-registry types. Kept apart from
/// the manifest aliases so each surface owns its own table; every value shares the
/// package's <c>oap.</c> prefix and must stay unique across both tables. Never rename
/// or remove an alias: it is part of the durable registry wire format.
/// </summary>
internal static class AppRegistryTypeAliases
{
    internal const string AppRegistryLifecycleState = "oap.ls";
    internal const string AppIsolationContext = "oap.ic";
    internal const string AppRegistryRecord = "oap.rg";
    internal const string AppRegistryInstallRequest = "oap.iq";
    internal const string AppRegistryTransitionError = "oap.te";
    internal const string AppRegistryTransitionResult = "oap.tx";
    internal const string AppTreeClaim = "oap.oc";
    internal const string AppTreeClaimKind = "oap.ok";
}
