namespace Orleans.Lattice.Schema;

/// <summary>
/// The phases a schema operation reports, in order. The dry run, build and cutover
/// carry the names of the matching <see cref="LatticeSchemaRemediationPhase"/>
/// members.
/// </summary>
public static class SchemaOperationPhases
{
    /// <summary>The tree's target schema version is being advanced (advance-and-migrate only).</summary>
    public const string Advance = "Advance";

    /// <summary>Every value is rewritten and checked, with nothing written. Units: values; the total is not known until it ends.</summary>
    public const string DryRun = nameof(LatticeSchemaRemediationPhase.DryRun);

    /// <summary>Every value is rewritten into the destination copy. Units: values, out of the dry run's count.</summary>
    public const string Build = nameof(LatticeSchemaRemediationPhase.Build);

    /// <summary>The tree is pointed at the destination copy. Reports no units.</summary>
    public const string Cutover = nameof(LatticeSchemaRemediationPhase.Cutover);

    /// <summary>What the dry run's and the build's units count.</summary>
    public const string ValuesUnit = "values";
}
