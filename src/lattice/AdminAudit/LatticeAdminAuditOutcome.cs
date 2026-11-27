namespace Orleans.Lattice;

/// <summary>
/// The outcome a <see cref="LatticeAdminAuditRecord"/> reports. Only
/// <see cref="Committed"/> claims that a change happened; every other value is
/// either a refusal, a failure, or an honest statement that the outcome could not
/// be proven. Values are explicit and persisted, so they are only ever appended.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeAdminAuditOutcome)]
public enum LatticeAdminAuditOutcome
{
    /// <summary>
    /// The intent was written and the call has not finished, or it finished
    /// without finalizing (for example the silo stopped between commit and
    /// finalize). The reconciler resolves it to one of the final outcomes.
    /// </summary>
    Pending = 0,

    /// <summary>The change was committed, proven by the caller's own successful commit.</summary>
    Committed = 1,

    /// <summary>
    /// Authorization passed but the change did not happen: validation, a
    /// precondition or conflict, a no-op, or a commit the reconciler proved never
    /// landed.
    /// </summary>
    Failed = 2,

    /// <summary>The caller was refused by the facade's authorization; nothing was attempted.</summary>
    Denied = 3,

    /// <summary>
    /// The call may or may not have committed and no evidence could prove either.
    /// The record still attributes the actor, but asserts no change.
    /// </summary>
    Indeterminate = 4,
}
