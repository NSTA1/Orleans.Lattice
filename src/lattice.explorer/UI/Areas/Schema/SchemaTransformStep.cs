namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>One validated step of a remediation transform, as the editor lists it.</summary>
/// <param name="Kind">What the step does.</param>
/// <param name="Path">The top-level member it acts on.</param>
/// <param name="ToPath">For a rename, the member's new name; otherwise <see langword="null"/>.</param>
/// <param name="ValueKind">For a set, the kind of constant written.</param>
/// <param name="Value">For a set, the constant as typed; otherwise <see langword="null"/>.</param>
internal sealed record SchemaTransformStep(
    SchemaTransformStepKind Kind,
    string Path,
    string? ToPath,
    SchemaConstantKind ValueKind,
    string? Value)
{
    /// <summary>The step as a plain sentence, such as "Set region to "eu"".</summary>
    public string Describe() => Kind switch
    {
        SchemaTransformStepKind.Remove => $"Remove {Path}",
        SchemaTransformStepKind.Rename => $"Rename {Path} to {ToPath}",
        _ => ValueKind switch
        {
            SchemaConstantKind.Text => $"Set {Path} to \"{Value}\"",
            SchemaConstantKind.Null => $"Set {Path} to null",
            _ => $"Set {Path} to {Value}",
        },
    };
}
