namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>One validated step of a remediation transform, as the editor lists it.</summary>
/// <param name="Kind">What the step does.</param>
/// <param name="Path">The top-level member it acts on.</param>
/// <param name="ToPath">For a rename, the member's new name; otherwise <see langword="null"/>.</param>
/// <param name="ValueKind">For a set, the kind of constant written.</param>
/// <param name="Value">For a set, the constant as typed; otherwise <see langword="null"/>.</param>
/// <param name="Where">
/// For a conditional set (#5476), the condition the member must meet for the constant to be
/// written; <see langword="null"/> for a step every value takes.
/// </param>
internal sealed record SchemaTransformStep(
    SchemaTransformStepKind Kind,
    string Path,
    string? ToPath,
    SchemaConstantKind ValueKind,
    string? Value,
    SchemaTransformCondition? Where = null)
{
    /// <summary>The step as a plain sentence, such as "Set region to "eu"" or "Set total to 0 where total is less than 0".</summary>
    public string Describe() => Kind switch
    {
        SchemaTransformStepKind.Remove => $"Remove {Path}",
        SchemaTransformStepKind.Rename => $"Rename {Path} to {ToPath}",
        _ when Where is { } where => $"Set {Path} to {Constant(ValueKind, Value)} where {Path} {where.Describe()}",
        _ => $"Set {Path} to {Constant(ValueKind, Value)}",
    };

    /// <summary>A constant as a step reads it: text in quotes, null as <c>null</c>, anything else as typed.</summary>
    /// <param name="kind">The constant's kind.</param>
    /// <param name="value">The constant as typed.</param>
    internal static string Constant(SchemaConstantKind kind, string? value) => kind switch
    {
        SchemaConstantKind.Text => $"\"{value}\"",
        SchemaConstantKind.Null => "null",
        _ => value ?? string.Empty,
    };
}
