namespace Orleans.Lattice;

/// <summary>
/// The JSON value type a <see cref="LatticeRuntimeViewProviderField"/> carries in a
/// described provider's payload object.
/// </summary>
public enum LatticeRuntimeViewProviderFieldKind
{
    /// <summary>A JSON string.</summary>
    Text = 0,

    /// <summary>A JSON number.</summary>
    Number = 1,

    /// <summary>A JSON <c>true</c> or <c>false</c>.</summary>
    Boolean = 2,
}
