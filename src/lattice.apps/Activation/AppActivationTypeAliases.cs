namespace Orleans.Lattice.Apps;

/// <summary>
/// Stable Orleans serialization aliases for the app activation pipeline types. The
/// values are wire format: never rename or remove one.
/// </summary>
internal static class AppActivationTypeAliases
{
    internal const string AppActivationOperation = "oap.aq";
    internal const string AppActivationFailure = "oap.af";
    internal const string AppActivationOutcome = "oap.ao";
    internal const string AppActivationStatus = "oap.as";
    internal const string IAppActivationGrain = "oap.ag";
}
