namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The verdict of validating a new principal id before creating it: the
/// <see cref="Outcome"/> plus, when the create is blocked, a human-readable
/// <see cref="Reason"/> the form renders inline. Produced by
/// <see cref="AccessCreateModel.ValidateAsync"/>.
/// </summary>
public readonly record struct CreatePrincipalDecision
{
    private CreatePrincipalDecision(CreatePrincipalOutcome outcome, string reason)
    {
        Outcome = outcome;
        Reason = reason;
    }

    /// <summary>The validation outcome.</summary>
    public CreatePrincipalOutcome Outcome { get; }

    /// <summary>The inline reason to show when the create is blocked; empty otherwise.</summary>
    public string Reason { get; }

    /// <summary><see langword="true"/> when the create is blocked and must not proceed.</summary>
    public bool IsBlocked => Outcome == CreatePrincipalOutcome.Block;

    /// <summary>
    /// <see langword="true"/> when the create may proceed - either the id resolved
    /// to a real principal (<see cref="CreatePrincipalOutcome.Allow"/>) or no
    /// directory was available to validate against
    /// (<see cref="CreatePrincipalOutcome.AllowUnvalidated"/>).
    /// </summary>
    public bool CanSave => Outcome != CreatePrincipalOutcome.Block;

    /// <summary>
    /// <see langword="true"/> when the create proceeds on an unvalidated free-text
    /// id because no directory could be queried.
    /// </summary>
    public bool IsUnvalidated => Outcome == CreatePrincipalOutcome.AllowUnvalidated;

    /// <summary>A verdict allowing the create against a resolved directory principal.</summary>
    public static CreatePrincipalDecision Allow() => new(CreatePrincipalOutcome.Allow, string.Empty);

    /// <summary>A verdict allowing the create on an unvalidated free-text id (no directory).</summary>
    public static CreatePrincipalDecision AllowUnvalidated() =>
        new(CreatePrincipalOutcome.AllowUnvalidated, string.Empty);

    /// <summary>A verdict blocking the create with an inline <paramref name="reason"/>.</summary>
    /// <param name="reason">The reason to show. Must not be <see langword="null"/>.</param>
    public static CreatePrincipalDecision Block(string reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        return new CreatePrincipalDecision(CreatePrincipalOutcome.Block, reason);
    }
}
