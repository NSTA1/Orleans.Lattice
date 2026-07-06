namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The result of a backup management action: a <see cref="BackupOperationStatus"/>
/// plus a human-readable message for the UI. A denial or a failure is returned as
/// data rather than thrown, so a Razor handler never has to wrap calls in a
/// try / catch.
/// </summary>
public sealed record BackupOperationResult
{
    /// <summary>The outcome classification.</summary>
    public required BackupOperationStatus Status { get; init; }

    /// <summary>A short, user-facing description of the outcome.</summary>
    public required string Message { get; init; }

    /// <summary><see langword="true"/> when <see cref="Status"/> is <see cref="BackupOperationStatus.Succeeded"/>.</summary>
    public bool IsSuccess => Status == BackupOperationStatus.Succeeded;

    /// <summary>Builds a success result with <paramref name="message"/>.</summary>
    /// <param name="message">The success message. Must not be <see langword="null"/>.</param>
    public static BackupOperationResult Success(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new BackupOperationResult { Status = BackupOperationStatus.Succeeded, Message = message };
    }

    /// <summary>Builds a denied result with <paramref name="message"/>.</summary>
    /// <param name="message">The denial message. Must not be <see langword="null"/>.</param>
    public static BackupOperationResult Denied(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new BackupOperationResult { Status = BackupOperationStatus.Denied, Message = message };
    }

    /// <summary>Builds a failed result with <paramref name="message"/>.</summary>
    /// <param name="message">The failure message. Must not be <see langword="null"/>.</param>
    public static BackupOperationResult Failure(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new BackupOperationResult { Status = BackupOperationStatus.Failed, Message = message };
    }
}
