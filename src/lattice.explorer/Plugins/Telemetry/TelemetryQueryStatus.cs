namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>
/// The outcome of one telemetry operation, in the Explorer's own terms. The three
/// failure kinds the facade distinguishes stay distinguished here, because
/// collapsing them is what makes a panel misinform a user.
/// </summary>
/// <remarks>
/// <para>
/// <b>A backend outage is not an invalid query.</b>
/// <see cref="BackendUnavailable"/> means the request was fine and the metrics
/// store could not answer it, so a panel offers a retry;
/// <see cref="UnknownQuery"/> and <see cref="OutOfBounds"/> mean the request
/// itself will never succeed as sent, so a panel corrects it instead. Presenting
/// either as the other leaves a user retrying a query that can never work, or
/// abandoning a good query during a transient outage.
/// </para>
/// </remarks>
public enum TelemetryQueryStatus
{
    /// <summary>The operation succeeded.</summary>
    Succeeded = 0,

    /// <summary>
    /// The caller presented a credential and the facade refused it. Not
    /// recoverable by signing in again.
    /// </summary>
    Denied = 1,

    /// <summary>
    /// The caller presented no credential. Recoverable: the shell offers a
    /// sign-in.
    /// </summary>
    AuthenticationRequired = 2,

    /// <summary>
    /// The cluster serves no telemetry facade at all. Not a denial and not a
    /// transport fault - there is simply nothing here to render.
    /// </summary>
    Unavailable = 3,

    /// <summary>
    /// The query id is not one the caller may run. The facade deliberately makes
    /// "no such query" and "not offered to you" indistinguishable, so a caller
    /// cannot probe for queries outside its entitlement, and this status keeps
    /// them so.
    /// </summary>
    UnknownQuery = 4,

    /// <summary>
    /// The requested window violates the bounds the catalogue entry declares. The
    /// specific limit is on <see cref="TelemetryOperationResult.Violation"/> when
    /// the seam detected it before the wire.
    /// </summary>
    OutOfBounds = 5,

    /// <summary>
    /// The metrics backend could not answer, or the facade could not be reached.
    /// Neither is the caller's fault and both are worth retrying with backoff,
    /// which is exactly what separates this from the two caller errors above.
    /// </summary>
    BackendUnavailable = 6,

    /// <summary>The request was malformed and was refused before evaluation.</summary>
    InvalidRequest = 7,

    /// <summary>The operation failed for a reason the seam does not classify.</summary>
    Failed = 8,
}
