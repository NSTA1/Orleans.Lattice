namespace Orleans.Lattice.Schema;

/// <summary>
/// The operation kinds the schema engine runs as tracked long-running operations.
/// Every kind starts with <see cref="Prefix"/>, which is how the schema facade
/// scopes the shared status, list and cancel verbs to schema operations.
/// </summary>
public static class SchemaOperationKinds
{
    /// <summary>The prefix every schema operation kind starts with.</summary>
    public const string Prefix = "schema.";

    /// <summary>A remediation: every value rewritten through a transform to satisfy a target policy, then cut over.</summary>
    public const string Remediation = "schema.remediation";

    /// <summary>An eager migration: every value re-stamped to the tree's current target schema version, then cut over.</summary>
    public const string Migration = "schema.migration";

    /// <summary>The tree's target schema version advanced, then an eager migration to it.</summary>
    public const string AdvanceAndMigrate = "schema.advance-and-migrate";
}
