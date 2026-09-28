namespace Orleans.Lattice;

/// <summary>
/// An in-process ownership decision returned by <see cref="ITreeOwnershipGuard"/>.
/// The default value denies. This value never crosses a grain boundary.
/// </summary>
public readonly struct TreeOwnershipDecision
{
    private TreeOwnershipDecision(bool allowed, string? reason)
    {
        Allowed = allowed;
        Reason = reason;
    }

    /// <summary>Whether the alias assignment is allowed.</summary>
    public bool Allowed { get; }

    /// <summary>
    /// The caller-safe denial reason, or <see langword="null"/> for an allow
    /// or a default denial. The registry supplies a generic reason for a default denial.
    /// </summary>
    public string? Reason { get; }

    /// <summary>Returns an explicit, allocation-free allow decision.</summary>
    public static TreeOwnershipDecision Allow() => new(true, null);

    /// <summary>Returns a denial with a caller-safe, non-blank reason.</summary>
    /// <param name="reason">The reason the ownership boundary refuses the alias.</param>
    /// <exception cref="ArgumentException">The reason is null, empty, or whitespace.</exception>
    public static TreeOwnershipDecision Deny(string reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return new(false, reason);
    }
}
