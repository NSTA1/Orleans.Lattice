namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// How a tracked file is treated by the content hygiene gates.
/// </summary>
public enum HygieneFileKind
{
    /// <summary>
    /// The file is classified neither as text nor as binary. The gates treat
    /// this as an error rather than a skip, because a file that quietly
    /// matches no rule would drop out of every content scan without trace.
    /// </summary>
    Unclassified = 0,

    /// <summary>The file holds text and is scanned.</summary>
    Text = 1,

    /// <summary>
    /// The file holds a binary payload, where a matched byte sequence would be
    /// meaningless, so it is deliberately not scanned.
    /// </summary>
    Binary = 2,
}
