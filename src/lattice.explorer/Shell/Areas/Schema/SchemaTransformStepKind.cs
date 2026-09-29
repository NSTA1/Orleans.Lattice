namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// The transform steps the remediation editor can write. Conditional and
/// computed transforms are registered in host code; this editor does not guess
/// them (#1257).
/// </summary>
internal enum SchemaTransformStepKind
{
    /// <summary>Set (or overwrite) a top-level member to a constant.</summary>
    Set = 0,

    /// <summary>Remove a top-level member.</summary>
    Remove = 1,

    /// <summary>Move a top-level member to a new name.</summary>
    Rename = 2,
}
