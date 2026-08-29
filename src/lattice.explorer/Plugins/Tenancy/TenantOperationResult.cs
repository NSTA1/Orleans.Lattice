namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// The result of a tenancy operation that returns no value: a
/// <see cref="Status"/> and a human-readable <see cref="Message"/>. The seam
/// folds a server refusal or a transport failure into a non-success result
/// rather than throwing, so a tenancy panel degrades cleanly and always has
/// something to show.
/// </summary>
/// <seealso cref="TenantOperationResult{TValue}"/>
public record TenantOperationResult
{
    /// <summary>The outcome category of the operation.</summary>
    public required TenantOperationStatus Status { get; init; }

    /// <summary>A human-readable description of the outcome. Never <see langword="null"/>.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary><see langword="true"/> when the operation succeeded.</summary>
    public bool IsSuccess => Status == TenantOperationStatus.Succeeded;

    /// <summary>
    /// <see langword="true"/> when the cluster does not serve this surface at
    /// all, so the caller should render nothing rather than an error.
    /// </summary>
    public bool IsUnavailable => Status == TenantOperationStatus.Unavailable;

    /// <summary>Creates a success result carrying <paramref name="message"/>.</summary>
    /// <param name="message">The success message. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static TenantOperationResult Success(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new TenantOperationResult { Status = TenantOperationStatus.Succeeded, Message = message };
    }

    /// <summary>
    /// Creates a non-success result with <paramref name="status"/> and
    /// <paramref name="message"/>.
    /// </summary>
    /// <param name="status">The outcome category.</param>
    /// <param name="message">The description of the outcome. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static TenantOperationResult Failure(TenantOperationStatus status, string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new TenantOperationResult { Status = status, Message = message };
    }
}
