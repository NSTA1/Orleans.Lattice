namespace Orleans.Lattice;

/// <summary>
/// The string instance method carried by a
/// <see cref="LatticePredicateNodeKind.StringMethod"/> node in the predicate IR.
/// All comparisons are ordinal (culture-invariant).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeStringMethod)]
public enum LatticeStringMethod : byte
{
    /// <summary><see cref="string.StartsWith(string)"/> (ordinal).</summary>
    StartsWith = 0,

    /// <summary><see cref="string.EndsWith(string)"/> (ordinal).</summary>
    EndsWith = 1,

    /// <summary><see cref="string.Contains(string)"/> (ordinal).</summary>
    Contains = 2,

    /// <summary><see cref="string.Equals(string)"/> (ordinal).</summary>
    Equals = 3,
}
