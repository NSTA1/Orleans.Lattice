namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The outcome of validating a <b>new</b> principal id before it is created in the
/// Access area. A three-way split so the create form can fail closed against a
/// searchable directory, yet stay honest when no directory can be queried.
/// </summary>
public enum CreatePrincipalOutcome
{
    /// <summary>
    /// The id resolves to a real principal of the expected kind in the directory,
    /// so the create may proceed.
    /// </summary>
    Allow,

    /// <summary>
    /// A directory is available but the id does not resolve to a real principal of
    /// the expected kind, so the create is blocked with a reason.
    /// </summary>
    Block,

    /// <summary>
    /// No directory is available to validate against, so the create proceeds on
    /// the free-text id as an explicitly unvalidated entry (existence cannot be
    /// enforced where it cannot be queried).
    /// </summary>
    AllowUnvalidated,
}
