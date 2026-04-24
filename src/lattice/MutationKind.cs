namespace Orleans.Lattice;

/// <summary>
/// Identifies the kind of mutation reported to an <see cref="IMutationObserver"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.MutationKind)]
public enum MutationKind
{
    /// <summary>A single key was written.</summary>
    Set = 0,

    /// <summary>A single key was deleted (tombstoned).</summary>
    Delete = 1,

    /// <summary>A key range was deleted — matching live keys in <c>[StartKey, EndExclusiveKey)</c> were tombstoned in bulk.</summary>
    DeleteRange = 2,
}
