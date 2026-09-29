using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// The link between the two regions, and the switch that pauses it so the
/// Replication area can show a link going <c>Lagging</c> and then <c>Stalled</c>.
/// </summary>
/// <remarks>
/// Each region's replication receiver runs behind <see cref="InvokeAsync"/>.
/// While the link is paused every cross-region replication call - live push,
/// snapshot bootstrap and saga control, in both directions - is refused with
/// <c>503 Service Unavailable</c>, which the sender's gRPC client reads as the
/// peer being unreachable. Nothing else on the endpoint is touched: the
/// Explorer's own facade calls keep working, so the console can watch the
/// backlog grow. Resuming lets the senders catch up from where they stopped.
/// </remarks>
internal sealed class PeerLink
{
    /// <summary>
    /// The path prefix every cross-region replication gRPC service shares
    /// (<c>LatticeReplication</c>, <c>LatticeRemoteSnapshot</c> and
    /// <c>LatticeSaga</c>). The replication <em>control and status</em> facades
    /// the Explorer calls live under <c>/orleans.lattice.api.replication</c> and
    /// are never matched.
    /// </summary>
    public const string ReplicationPathPrefix = "/orleans.lattice.replication.";

    private readonly Lock _gate = new();
    private volatile bool _paused;

    /// <summary>Raised after the link is paused or resumed, with the new state.</summary>
    public event Action<bool>? Changed;

    /// <summary>Whether cross-region replication is currently refused.</summary>
    public bool IsPaused => _paused;

    /// <summary>Pauses the link, or resumes it when it is paused.</summary>
    /// <returns>Whether the link is paused afterwards.</returns>
    public bool Toggle()
    {
        bool paused;
        lock (_gate)
        {
            paused = !_paused;
            _paused = paused;
        }

        Changed?.Invoke(paused);
        return paused;
    }

    /// <summary>Pauses the link; does nothing when it is already paused.</summary>
    /// <returns>Whether this call paused it.</returns>
    public bool Pause() => Set(paused: true);

    /// <summary>Resumes the link; does nothing when it is not paused.</summary>
    /// <returns>Whether this call resumed it.</returns>
    public bool Resume() => Set(paused: false);

    /// <summary>Whether <paramref name="path"/> is a cross-region replication call.</summary>
    /// <param name="path">The request path.</param>
    public static bool IsReplicationPath(PathString path) =>
        path.HasValue && path.Value!.StartsWith(ReplicationPathPrefix, StringComparison.Ordinal);

    /// <summary>
    /// Refuses a cross-region replication call while the link is paused, and
    /// passes every other request to <paramref name="next"/>.
    /// </summary>
    /// <param name="context">The request.</param>
    /// <param name="next">The rest of the pipeline.</param>
    public Task InvokeAsync(HttpContext context, RequestDelegate next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);

        if (_paused && IsReplicationPath(context.Request.Path))
        {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            return Task.CompletedTask;
        }

        return next(context);
    }

    private bool Set(bool paused)
    {
        lock (_gate)
        {
            if (_paused == paused)
            {
                return false;
            }

            _paused = paused;
        }

        Changed?.Invoke(paused);
        return true;
    }
}
