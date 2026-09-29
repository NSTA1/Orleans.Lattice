namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>The long-running schema operations the area starts and follows.</summary>
internal enum SchemaOperationKind
{
    /// <summary>Re-stamp every stored value to the tree's current target version.</summary>
    Migrate = 0,

    /// <summary>Advance the target version, then re-stamp every stored value to it.</summary>
    AdvanceAndMigrate = 1,

    /// <summary>Rewrite every value through a transform so it satisfies a policy, then cut over.</summary>
    Remediate = 2,
}
