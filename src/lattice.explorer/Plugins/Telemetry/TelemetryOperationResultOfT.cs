namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The outcome of a telemetry operation that returns a value - a catalogue or an
/// evaluated result - carrying the value only on success.
/// </summary>
/// <typeparam name="TValue">The value the operation returns.</typeparam>
public sealed record TelemetryOperationResult<TValue> : TelemetryOperationResult
{
    /// <summary>
    /// The value the operation produced, or <see langword="null"/> (or
    /// <see langword="default"/>) when it failed.
    /// </summary>
    public TValue? Value { get; init; }

    /// <summary>Creates a success result carrying <paramref name="value"/>.</summary>
    /// <param name="value">The value produced.</param>
    /// <param name="message">The message to render.</param>
    /// <returns>The success result.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static TelemetryOperationResult<TValue> Success(TValue value, string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new TelemetryOperationResult<TValue>
        {
            Status = TelemetryQueryStatus.Succeeded,
            Message = message,
            Value = value,
        };
    }

    /// <summary>Creates a failure result.</summary>
    /// <param name="status">The classified outcome.</param>
    /// <param name="message">The message to render.</param>
    /// <param name="violation">The specific window limit violated, when known.</param>
    /// <returns>The failure result.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static new TelemetryOperationResult<TValue> Failure(
        TelemetryQueryStatus status,
        string message,
        ExplorerTelemetryBoundsViolation violation = ExplorerTelemetryBoundsViolation.None)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new TelemetryOperationResult<TValue> { Status = status, Message = message, Violation = violation };
    }
}
