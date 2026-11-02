namespace Orleans.Lattice;

/// <summary>
/// What an operator is told about a host-registered runtime-view projection
/// provider: what it builds, and the shape of its payload. A described provider
/// accepts its payload as a UTF-8 JSON object; when <see cref="PayloadFields"/> is
/// non-empty a console offers the payload as a form with one input per field,
/// otherwise as JSON the provider validates when the view is created.
/// </summary>
public sealed class LatticeRuntimeViewProviderDescription
{
    /// <summary>Describes a provider.</summary>
    /// <param name="description">A one-line description of the views the provider builds. Must not be <c>null</c> or empty.</param>
    /// <param name="payloadFields">The payload object's properties, or <c>null</c> when the payload is free-form JSON.</param>
    /// <exception cref="ArgumentException"><paramref name="description"/> is <c>null</c> or empty, or two fields share a name.</exception>
    public LatticeRuntimeViewProviderDescription(
        string description,
        IEnumerable<LatticeRuntimeViewProviderField>? payloadFields = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(description);
        var fields = payloadFields?.ToArray() ?? [];
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var field in fields)
        {
            ArgumentNullException.ThrowIfNull(field, nameof(payloadFields));
            if (!names.Add(field.Name))
            {
                throw new ArgumentException($"Payload field '{field.Name}' is described more than once.", nameof(payloadFields));
            }
        }

        Description = description;
        PayloadFields = fields;
    }

    /// <summary>A one-line description of the views the provider builds.</summary>
    public string Description { get; }

    /// <summary>The payload object's properties, in display order; empty when the payload is free-form JSON.</summary>
    public IReadOnlyList<LatticeRuntimeViewProviderField> PayloadFields { get; }
}
