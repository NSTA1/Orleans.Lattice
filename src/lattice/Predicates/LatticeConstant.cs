namespace Orleans.Lattice;

/// <summary>
/// An immutable literal value captured during predicate translation. The
/// captured constant is normalized into one of a small set of wire-stable
/// shapes (<see cref="LatticeConstantKind"/>) so the server-side evaluator can
/// compare it against a JSON document value deterministically, independent of
/// the original CLR type.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeConstant)]
[Immutable]
public readonly record struct LatticeConstant
{
    /// <summary>The runtime kind of this constant.</summary>
    [Id(0)] public LatticeConstantKind Kind { get; init; }

    /// <summary>The boolean value when <see cref="Kind"/> is <see cref="LatticeConstantKind.Boolean"/>.</summary>
    [Id(1)] public bool BooleanValue { get; init; }

    /// <summary>The string value when <see cref="Kind"/> is <see cref="LatticeConstantKind.String"/>.</summary>
    [Id(2)] public string? StringValue { get; init; }

    /// <summary>The integral value when <see cref="Kind"/> is <see cref="LatticeConstantKind.Int64"/>.</summary>
    [Id(3)] public long Int64Value { get; init; }

    /// <summary>The floating-point value when <see cref="Kind"/> is <see cref="LatticeConstantKind.Double"/>.</summary>
    [Id(4)] public double DoubleValue { get; init; }

    /// <summary>Creates a null constant.</summary>
    public static LatticeConstant Null() => new() { Kind = LatticeConstantKind.Null };

    /// <summary>Creates a boolean constant.</summary>
    public static LatticeConstant Bool(bool value) => new() { Kind = LatticeConstantKind.Boolean, BooleanValue = value };

    /// <summary>Creates a string constant.</summary>
    public static LatticeConstant Text(string value) => new() { Kind = LatticeConstantKind.String, StringValue = value };

    /// <summary>Creates an integral constant.</summary>
    public static LatticeConstant Integer(long value) => new() { Kind = LatticeConstantKind.Int64, Int64Value = value };

    /// <summary>Creates a floating-point constant.</summary>
    public static LatticeConstant Real(double value) => new() { Kind = LatticeConstantKind.Double, DoubleValue = value };
}
