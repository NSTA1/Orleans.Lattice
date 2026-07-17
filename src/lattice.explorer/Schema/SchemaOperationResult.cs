namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The result of a Schema-area mutation (a policy / version-config write, a version
/// advance, or a migration): a <see cref="Status"/> and a human-readable
/// <see cref="Message"/>. The policy and versioning services fold a server denial
/// or a transport failure into a non-success result rather than throwing, so the UI
/// degrades cleanly and always has a message to show.
/// </summary>
public sealed record SchemaOperationResult
{
    /// <summary>The outcome category of the operation.</summary>
    public required SchemaOperationStatus Status { get; init; }

    /// <summary>A human-readable description of the outcome.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary><see langword="true"/> when the operation succeeded.</summary>
    public bool IsSuccess => Status == SchemaOperationStatus.Succeeded;

    /// <summary>Creates a success result with <paramref name="message"/>.</summary>
    /// <param name="message">The success message. Must not be <see langword="null"/>.</param>
    public static SchemaOperationResult Success(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new SchemaOperationResult { Status = SchemaOperationStatus.Succeeded, Message = message };
    }

    /// <summary>Creates a denial result with <paramref name="message"/>.</summary>
    /// <param name="message">The denial message. Must not be <see langword="null"/>.</param>
    public static SchemaOperationResult Denied(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new SchemaOperationResult { Status = SchemaOperationStatus.Denied, Message = message };
    }

    /// <summary>Creates a failure result with <paramref name="message"/>.</summary>
    /// <param name="message">The failure message. Must not be <see langword="null"/>.</param>
    public static SchemaOperationResult Failure(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new SchemaOperationResult { Status = SchemaOperationStatus.Failed, Message = message };
    }
}
