namespace Orleans.Lattice.Auth;

/// <summary>
/// A host seam that receives a structured <see cref="LatticeAuthDecisionEvent"/>
/// for every gated authorization decision, at the verbosity and sampling the
/// host configures on <see cref="LatticeAuthOptions"/>. Register one or more
/// implementations to fan a decision out to logs, a durable trail, an external
/// SIEM, or any other audit destination.
/// </summary>
/// <remarks>
/// <para>
/// A sink is invoked <b>after</b> the enforcement gate has computed and returned
/// its decision, so a sink can never change, delay, or block the decision: the
/// gate observes the returned task but does not await it on the request path. A
/// sink implementation must therefore be resilient to being called on a
/// fire-and-forget basis and must not throw synchronously.
/// </para>
/// <para>
/// Sinks run only when auditing is enabled
/// (<see cref="LatticeAuthOptions.EnableAuditSink"/>); when it is disabled no
/// event is ever built or dispatched, so the seam is strictly zero-cost on the
/// hot path by default.
/// </para>
/// </remarks>
public interface ILatticeAuthAuditSink
{
    /// <summary>
    /// Records a single authorization decision event.
    /// </summary>
    /// <param name="decisionEvent">The decision event to record.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the sink has accepted the event.</returns>
    ValueTask WriteAsync(LatticeAuthDecisionEvent decisionEvent, CancellationToken cancellationToken = default);
}
