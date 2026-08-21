namespace Orleans.Lattice;

/// <summary>
/// Immutable context supplied to a runtime-view projection provider when it
/// reconstructs a persisted view definition.
/// </summary>
public sealed class LatticeRuntimeViewProjectionContext
{
    private readonly byte[] _payload;

    internal LatticeRuntimeViewProjectionContext(
        string viewName,
        string sourceTreeId,
        ReadOnlySpan<byte> payload)
    {
        ViewName = viewName;
        SourceTreeId = sourceTreeId;
        _payload = payload.ToArray();
    }

    /// <summary>The logical view name being reconstructed.</summary>
    public string ViewName { get; }

    /// <summary>The source tree id whose WAL the view tails.</summary>
    public string SourceTreeId { get; }

    /// <summary>A defensive copy of the provider's persisted opaque payload.</summary>
    public byte[] Payload => _payload.ToArray();

    internal ReadOnlySpan<byte> PayloadSpan => _payload;
}
