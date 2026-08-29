namespace Orleans.Lattice.Explorer.Plugins.Schema.Domain;

/// <summary>
/// One schema-management action the per-tree capability probe reports on, and
/// which the plugin files an independent scoped access decision for.
/// <para>
/// This enum is the plugin's own vocabulary. It is deliberately narrower than
/// "the plugin is reachable": the plugin-level gate answers whether the schema
/// control endpoint responds at all, while each member here answers whether the
/// caller may perform one action <em>on one tree</em>. The two live under
/// different keys in the same keyed store - a plugin-level key and a scoped key
/// - and a scoped decision never inherits the plugin-level one.
/// </para>
/// </summary>
public enum SchemaCapability
{
    /// <summary>Read the tree's enforcement policy.</summary>
    ViewPolicy,

    /// <summary>Set or clear the tree's enforcement policy.</summary>
    ManagePolicy,

    /// <summary>Read the tree's envelope-version config.</summary>
    ViewVersionConfig,

    /// <summary>Set, advance, migrate, or clear the tree's version config.</summary>
    ManageVersion,

    /// <summary>Read the tree's remediation (re-stamp) status.</summary>
    ViewRemediationStatus,

    /// <summary>Start a background remediation of the tree.</summary>
    Remediate,

    /// <summary>Run a read-only compliance audit of the tree.</summary>
    ScanCompliance,

    /// <summary>List and count the tree's strict-mode dead letters.</summary>
    ViewDeadLetters,
}
