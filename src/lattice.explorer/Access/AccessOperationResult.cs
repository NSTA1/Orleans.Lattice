namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The result of an Access-area mutation (a user / group / membership / rule
/// write): a <see cref="Status"/> and a human-readable <see cref="Message"/>.
/// The membership and policy services fold a server denial or a transport
/// failure into a non-success result rather than throwing, so the UI degrades
/// cleanly and always has a message to show.
/// </summary>
public sealed record AccessOperationResult
{
    /// <summary>The outcome category of the operation.</summary>
    public required AccessOperationStatus Status { get; init; }

    /// <summary>A human-readable description of the outcome.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary><see langword="true"/> when the operation succeeded.</summary>
    public bool IsSuccess => Status == AccessOperationStatus.Succeeded;

    /// <summary>Creates a success result with <paramref name="message"/>.</summary>
    /// <param name="message">The success message. Must not be <see langword="null"/>.</param>
    public static AccessOperationResult Success(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new AccessOperationResult { Status = AccessOperationStatus.Succeeded, Message = message };
    }

    /// <summary>Creates a denial result with <paramref name="message"/>.</summary>
    /// <param name="message">The denial message. Must not be <see langword="null"/>.</param>
    public static AccessOperationResult Denied(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new AccessOperationResult { Status = AccessOperationStatus.Denied, Message = message };
    }

    /// <summary>Creates a failure result with <paramref name="message"/>.</summary>
    /// <param name="message">The failure message. Must not be <see langword="null"/>.</param>
    public static AccessOperationResult Failure(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new AccessOperationResult { Status = AccessOperationStatus.Failed, Message = message };
    }
}
