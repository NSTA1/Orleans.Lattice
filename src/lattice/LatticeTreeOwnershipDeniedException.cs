namespace Orleans.Lattice;

/// <summary>
/// An alias assignment refused by <see cref="ITreeOwnershipGuard"/> before
/// the registry writes or publishes an alias change. Unlike ordinary caller
/// authorization, ownership is enforced for system-origin maintenance too.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTreeOwnershipDenied)]
public sealed class LatticeTreeOwnershipDeniedException : Exception
{
    /// <summary>Creates an ownership denial with a caller-safe reason.</summary>
    /// <param name="reason">The non-blank reason the alias was refused.</param>
    /// <exception cref="ArgumentException">The reason is null, empty, or whitespace.</exception>
    public LatticeTreeOwnershipDeniedException(string reason)
        : base($"Tree alias denied by ownership: {reason}")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        Reason = reason;
    }

    /// <summary>The caller-safe reason supplied by the ownership guard.</summary>
    [Id(0)]
    public string Reason { get; }
}
