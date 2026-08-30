namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// How a panel renders one telemetry outcome: the message, whether it is worth
/// retrying, and the severity class that decides how it looks.
/// </summary>
/// <remarks>
/// <para>
/// <b>The distinction that matters is retryable versus not.</b> A backend that
/// could not answer is a transient fault a caller should retry; an unknown query
/// or an out-of-bounds window will never succeed as sent, so a retry button
/// beside it is an invitation to waste time. That is why the seam keeps the
/// three failure kinds apart, and this type carries the distinction through to
/// the affordance.
/// </para>
/// </remarks>
public sealed record TelemetryNotice
{
    private TelemetryNotice(string severity, string message, string? guidance, bool retryable)
    {
        Severity = severity;
        Message = message;
        Guidance = guidance;
        IsRetryable = retryable;
    }

    /// <summary>The CSS severity class the panel renders the notice with.</summary>
    public string Severity { get; }

    /// <summary>The message, taken from the seam's classified result.</summary>
    public string Message { get; }

    /// <summary>Additional guidance, when the outcome has a specific remedy.</summary>
    public string? Guidance { get; }

    /// <summary>
    /// Whether the panel should offer a retry. Only a transient backend fault
    /// is retryable; a refusal of the request itself is not.
    /// </summary>
    public bool IsRetryable { get; }

    /// <summary>
    /// The notice for <paramref name="result"/>, or <see langword="null"/> when
    /// it succeeded - a chart that rendered needs no banner saying so.
    /// </summary>
    /// <param name="result">The classified outcome.</param>
    /// <returns>The notice, or <see langword="null"/> on success.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is <see langword="null"/>.</exception>
    public static TelemetryNotice? For(TelemetryOperationResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        if (result.IsSuccess)
        {
            return null;
        }

        var message = string.IsNullOrWhiteSpace(result.Message)
            ? "The telemetry request failed."
            : result.Message;

        return result.Status switch
        {
            TelemetryQueryStatus.AuthenticationRequired => new TelemetryNotice(
                TelemetrySeverity.Denied,
                message,
                "The connection carries no accepted credential. Sign in to the cluster and try again.",
                retryable: false),

            TelemetryQueryStatus.Denied => new TelemetryNotice(
                TelemetrySeverity.Denied,
                message,
                guidance: null,
                retryable: false),

            TelemetryQueryStatus.Unavailable => new TelemetryNotice(
                TelemetrySeverity.Muted,
                message,
                "This cluster serves no telemetry facade, so there is nothing to chart here.",
                retryable: false),

            TelemetryQueryStatus.UnknownQuery => new TelemetryNotice(
                TelemetrySeverity.Refused,
                message,
                "Pick another entry from the catalogue. The cluster decides which queries it offers you.",
                retryable: false),

            TelemetryQueryStatus.OutOfBounds => new TelemetryNotice(
                TelemetrySeverity.Refused,
                message,
                BoundsGuidance(result.Violation),
                retryable: false),

            TelemetryQueryStatus.BackendUnavailable => new TelemetryNotice(
                TelemetrySeverity.Warn,
                message,
                "The metrics backend could not answer. The request itself was fine, so it is worth retrying.",
                retryable: true),

            TelemetryQueryStatus.InvalidRequest => new TelemetryNotice(
                TelemetrySeverity.Refused,
                message,
                guidance: null,
                retryable: false),

            _ => new TelemetryNotice(TelemetrySeverity.Refused, message, guidance: null, retryable: false),
        };
    }

    private static string? BoundsGuidance(ExplorerTelemetryBoundsViolation violation) => violation switch
    {
        ExplorerTelemetryBoundsViolation.RangeTooLong =>
            "Choose a shorter time range. The limit is the one this query publishes.",
        ExplorerTelemetryBoundsViolation.LookbackTooOld =>
            "Choose a more recent window. The metrics store does not retain data that far back.",
        ExplorerTelemetryBoundsViolation.TooManyPoints =>
            "Choose a coarser step or a shorter range. The two together exceed this query's point budget.",
        ExplorerTelemetryBoundsViolation.StepBelowMinimum =>
            "Choose a coarser step. This query publishes a finest resolution.",
        ExplorerTelemetryBoundsViolation.StepAboveMaximum =>
            "Choose a finer step. This query publishes a coarsest resolution.",
        ExplorerTelemetryBoundsViolation.RangeNotAscending =>
            "The window ends before it starts.",
        _ => null,
    };
}
