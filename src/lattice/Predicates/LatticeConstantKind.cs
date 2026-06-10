namespace Orleans.Lattice;

/// <summary>
/// Discriminates the runtime type of a <see cref="LatticeConstant"/> captured
/// during predicate translation, so the server-side evaluator can compare it
/// against the matching JSON value kind.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeConstantKind)]
public enum LatticeConstantKind : byte
{
    /// <summary>A null literal.</summary>
    Null = 0,

    /// <summary>A boolean literal.</summary>
    Boolean = 1,

    /// <summary>A string literal (also used for char, Guid, DateTime, and other stringly-compared values).</summary>
    String = 2,

    /// <summary>An integral literal, widened to <see cref="long"/>.</summary>
    Int64 = 3,

    /// <summary>A floating-point literal, widened to <see cref="double"/>.</summary>
    Double = 4,
}
