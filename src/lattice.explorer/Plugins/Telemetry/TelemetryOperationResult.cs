namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>
/// The outcome of a telemetry operation that carries no value: a status, a
/// message a panel can render, and - for a bounds refusal the seam detected
/// itself - the specific limit that was violated.
/// </summary>
public record TelemetryOperationResult
{
    /// <summary>The classified outcome.</summary>
    public required TelemetryQueryStatus Status { get; init; }

    /// <summary>A message a panel can render. Never <see langword="null"/>.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>
    /// The specific window limit that was violated, when
    /// <see cref="Status"/> is <see cref="TelemetryQueryStatus.OutOfBounds"/> and
    /// the seam recognised it. A refusal the facade raised arrives as
    /// <see cref="ExplorerTelemetryBoundsViolation.Unspecified"/>, because the
    /// transport carries a status and a message rather than a value.
    /// </summary>
    public ExplorerTelemetryBoundsViolation Violation { get; init; }

    /// <summary><see langword="true"/> when the operation succeeded.</summary>
    public bool IsSuccess => Status == TelemetryQueryStatus.Succeeded;

    /// <summary>
    /// <see langword="true"/> when the cluster serves no telemetry facade, so a
    /// telemetry surface should render nothing rather than an error.
    /// </summary>
    public bool IsUnavailable => Status == TelemetryQueryStatus.Unavailable;

    /// <summary>
    /// <see langword="true"/> when retrying could plausibly succeed: the failure
    /// was the backend's, not the request's.
    /// </summary>
    public bool IsRetryable => Status == TelemetryQueryStatus.BackendUnavailable;

    /// <summary>Creates a success result.</summary>
    /// <param name="message">The message to render.</param>
    /// <returns>The success result.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static TelemetryOperationResult Success(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new TelemetryOperationResult { Status = TelemetryQueryStatus.Succeeded, Message = message };
    }

    /// <summary>Creates a failure result.</summary>
    /// <param name="status">The classified outcome.</param>
    /// <param name="message">The message to render.</param>
    /// <param name="violation">The specific window limit violated, when known.</param>
    /// <returns>The failure result.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static TelemetryOperationResult Failure(
        TelemetryQueryStatus status,
        string message,
        ExplorerTelemetryBoundsViolation violation = ExplorerTelemetryBoundsViolation.None)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new TelemetryOperationResult { Status = status, Message = message, Violation = violation };
    }
}
