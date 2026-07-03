namespace Orleans.Lattice.Auth;

/// <summary>
/// Convenience aggregates over <see cref="LatticeOperation"/> for authoring
/// authorization rules. The core <see cref="LatticeOperation"/> flags enum is
/// per-request vocabulary and deliberately carries no "all" aggregate, so this
/// helper supplies the grant-mask convenience the policy layer needs without
/// widening the core surface.
/// </summary>
public static class LatticeAuthOperations
{
    /// <summary>
    /// Every enforceable data-plane operation. A rule whose
    /// <see cref="LatticeAuthorizationRule.Operations"/> carries this mask covers
    /// the complete set of operations an access gate can authorize.
    /// </summary>
    public const LatticeOperation All =
        LatticeOperation.Read
        | LatticeOperation.Write
        | LatticeOperation.Delete
        | LatticeOperation.RangeRead
        | LatticeOperation.RangeDelete
        | LatticeOperation.CrdtApply
        | LatticeOperation.AtomicWrite
        | LatticeOperation.BulkLoad
        | LatticeOperation.Admin;
}
