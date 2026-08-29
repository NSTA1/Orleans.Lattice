namespace Orleans.Lattice.Explorer.Schema.Domain;

/// <summary>
/// The Schema plugin's scope vocabulary: how a per-tree, per-action decision is
/// named inside the plugin's own slice of the keyed access store.
/// <para>
/// A scope is <c>{treeId}/{action}</c>. Nothing outside this plugin needs to
/// know the strings - that is exactly what the keyed store buys over the fat
/// capability record the Explorer used to carry: the Schema area's per-tree
/// grey-out is expressed in the plugin's own vocabulary, under
/// <see cref="Orleans.Lattice.Explorer.Schema.SchemaPluginKeys.PluginId"/>, with
/// no shared type to edit.
/// </para>
/// </summary>
public static class SchemaPluginScopes
{
    /// <summary>
    /// The stable, ordinal suffix a capability contributes to a scope. Kept as
    /// explicit constants rather than <see cref="Enum.ToString()"/> so a rename
    /// of the enum member cannot silently change a persisted key shape.
    /// </summary>
    /// <param name="capability">The capability to name.</param>
    /// <returns>The action segment of the scope.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capability"/> is not a declared member.</exception>
    public static string Action(SchemaCapability capability) => capability switch
    {
        SchemaCapability.ViewPolicy => "policy.view",
        SchemaCapability.ManagePolicy => "policy.manage",
        SchemaCapability.ViewVersionConfig => "version.view",
        SchemaCapability.ManageVersion => "version.manage",
        SchemaCapability.ViewRemediationStatus => "remediation.view",
        SchemaCapability.Remediate => "remediation.run",
        SchemaCapability.ScanCompliance => "compliance.scan",
        SchemaCapability.ViewDeadLetters => "deadletters.view",
        _ => throw new ArgumentOutOfRangeException(nameof(capability), capability, "Unknown schema capability."),
    };

    /// <summary>
    /// Builds the scope one action on one tree is filed under.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/>.</param>
    /// <param name="capability">The action the scope names.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capability"/> is not a declared member.</exception>
    public static string For(string treeId, SchemaCapability capability)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return string.Concat(treeId, "/", Action(capability));
    }
}
