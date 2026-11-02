namespace Orleans.Lattice;

/// <summary>
/// One property of a described runtime-view provider's JSON payload object, so an
/// operator console can offer the payload as a form instead of raw JSON.
/// </summary>
public sealed record LatticeRuntimeViewProviderField
{
    /// <summary>Describes one payload property.</summary>
    /// <param name="name">The JSON property name. Must not be <c>null</c> or empty.</param>
    /// <param name="label">The label a form shows for the property. Must not be <c>null</c> or empty.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> or <paramref name="label"/> is <c>null</c> or empty.</exception>
    public LatticeRuntimeViewProviderField(string name, string label)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentException.ThrowIfNullOrEmpty(label);
        Name = name;
        Label = label;
    }

    /// <summary>The JSON property name.</summary>
    public string Name { get; }

    /// <summary>The label a form shows for the property.</summary>
    public string Label { get; }

    /// <summary>An optional one-line explanation a form shows beside the property.</summary>
    public string? Description { get; init; }

    /// <summary>Whether the property must be supplied.</summary>
    public bool Required { get; init; }

    /// <summary>The JSON value type of the property. Defaults to <see cref="LatticeRuntimeViewProviderFieldKind.Text"/>.</summary>
    public LatticeRuntimeViewProviderFieldKind Kind { get; init; }
}
