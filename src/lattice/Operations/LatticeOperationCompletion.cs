namespace Orleans.Lattice.Operations;

/// <summary>The terminal outcome a runner records on an operation's tracking grain.</summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeOperationCompletion)]
[Immutable]
internal sealed record LatticeOperationCompletion
{
    /// <summary>The terminal state: succeeded, failed or cancelled.</summary>
    [Id(0)] public required LatticeOperationState State { get; init; }

    /// <summary>The failure or cancellation reason.</summary>
    [Id(1)] public string? FailureReason { get; init; }

    /// <summary>An opaque reference to the result.</summary>
    [Id(2)] public string? ResultReference { get; init; }

    /// <summary>A small string map describing the result.</summary>
    [Id(3)] public IReadOnlyDictionary<string, string> Result { get; init; } = LatticeOperationRecord.EmptyResult;

    /// <summary>Builds a succeeded completion.</summary>
    /// <param name="resultReference">The opaque result reference.</param>
    /// <param name="result">The result map, or <see langword="null"/> for none.</param>
    /// <returns>The completion.</returns>
    public static LatticeOperationCompletion Succeeded(
        string? resultReference = null,
        IReadOnlyDictionary<string, string>? result = null) =>
        new()
        {
            State = LatticeOperationState.Succeeded,
            ResultReference = resultReference,
            Result = result ?? LatticeOperationRecord.EmptyResult,
        };

    /// <summary>Builds a failed completion.</summary>
    /// <param name="reason">The failure reason.</param>
    /// <returns>The completion.</returns>
    public static LatticeOperationCompletion Failed(string reason) =>
        new() { State = LatticeOperationState.Failed, FailureReason = reason };

    /// <summary>Builds a cancelled completion.</summary>
    /// <param name="reason">The cancellation reason.</param>
    /// <returns>The completion.</returns>
    public static LatticeOperationCompletion Cancelled(string reason) =>
        new() { State = LatticeOperationState.Cancelled, FailureReason = reason };
}
