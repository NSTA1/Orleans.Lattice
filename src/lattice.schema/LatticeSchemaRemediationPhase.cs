namespace Orleans.Lattice.Schema;

/// <summary>
/// The phase of a background schema-remediation shadow build. The coordinator
/// advances strictly forward through these phases and persists each transition
/// before performing the phase's external side effects, so a reactivation after a
/// silo restart resumes at the last durably-recorded phase.
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaRemediationPhase)]
public enum LatticeSchemaRemediationPhase
{
    /// <summary>No remediation has run, or the coordinator is quiescent between runs.</summary>
    Idle = 0,

    /// <summary>
    /// The read-only dry-run gate is in progress: every existing value is rewritten
    /// by the remediation transform and validated against the target policy. No
    /// destination tree exists and the original tree is untouched. A failure here
    /// aborts with no cutover.
    /// </summary>
    DryRun = 1,

    /// <summary>
    /// The dry-run passed and the destination physical tree is being populated with
    /// the transformed, revalidated values. A failure here discards the partial
    /// destination and aborts with no cutover.
    /// </summary>
    Build = 2,

    /// <summary>
    /// The destination is fully built. The logical tree is repointed to the
    /// destination physical tree and the target policy is installed so subsequent
    /// writes are enforced against the shape the data now satisfies.
    /// </summary>
    Cutover = 3,

    /// <summary>The remediation completed successfully and the logical tree serves the remediated data.</summary>
    Completed = 4,

    /// <summary>
    /// The remediation aborted on the first offending value. The original tree was
    /// left untouched (no alias change, no policy change) and any partial
    /// destination was discarded.
    /// </summary>
    Aborted = 5,
}
