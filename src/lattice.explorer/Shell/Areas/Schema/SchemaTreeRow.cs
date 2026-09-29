using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>One tree as the Schema directory lists it: its policy, its version config and any app declaration.</summary>
/// <param name="TreeId">The logical tree id.</param>
/// <param name="Policy">The enforcement policy, or <see langword="null"/> when none is set or it could not be read.</param>
/// <param name="PolicyState">How the policy read ended.</param>
/// <param name="Version">The version config, or <see langword="null"/> when the tree is unversioned or it could not be read.</param>
/// <param name="VersionState">How the version read ended.</param>
/// <param name="Declaration">The installed app's manifest declaration for this tree, if any.</param>
internal sealed record SchemaTreeRow(
    string TreeId,
    LatticeSchemaPolicy? Policy,
    SchemaReadState PolicyState,
    LatticeSchemaVersionConfig? Version,
    SchemaReadState VersionState,
    SchemaAppDeclaration? Declaration)
{
    /// <summary>Whether the tree is under schema: a policy, a version config, or an app declaration.</summary>
    public bool IsGoverned => Policy is not null || Version is not null || Declaration is not null;
}
