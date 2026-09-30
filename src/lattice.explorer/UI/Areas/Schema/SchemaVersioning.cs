using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// What a version-config read means for the area. A config always describes a
/// versioned tree, so its target version is at least 1; version 0 is the reserved
/// "unversioned" sentinel. A cluster that reads an absent config as the value-type
/// default answers "family 0 at version 0" for every unversioned tree, and the
/// area must not count or show that as versioning.
/// </summary>
internal static class SchemaVersioning
{
    /// <summary>The config a tree is actually versioned under, or <see langword="null"/> when it is unversioned.</summary>
    /// <param name="config">The config as read.</param>
    /// <returns>The config when its target version is at least 1; otherwise <see langword="null"/>.</returns>
    public static LatticeSchemaVersionConfig? Effective(LatticeSchemaVersionConfig? config) =>
        config is { TargetVersion: > 0 } ? config : null;
}
