using Grpc.Core;

namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// Identifies which telemetry operation an inbound gRPC call invokes. Supplied to
/// <see cref="ILatticeTelemetryApiAuthorizer.IsAuthorizedAsync"/> so a host can
/// make per-operation decisions (for example allow catalogue discovery but deny
/// query evaluation on an edge-facing endpoint).
/// </summary>
public enum LatticeTelemetryApiOperation
{
    /// <summary>The read-only <c>GetCatalog</c> curated-catalogue discovery RPC.</summary>
    GetCatalog = 0,

    /// <summary>The read-only <c>Query</c> curated-query evaluation RPC.</summary>
    Query = 1,

    /// <summary>
    /// A telemetry method the interceptor does not recognise (for example a future
    /// RPC added without updating the operation map). Presented to the authorizer
    /// so a deny-by-default policy can refuse an unmapped call rather than have it
    /// silently masquerade as a benign one.
    /// </summary>
    Unknown = 2,
}

/// <summary>
/// Describes an inbound telemetry gRPC call to
/// <see cref="ILatticeTelemetryApiAuthorizer.IsAuthorizedAsync"/>. Carries the
/// <see cref="Operation"/> being invoked, an optional <see cref="TargetId"/>, and
/// the underlying gRPC <see cref="ServerCallContext"/> for header / identity /
/// peer inspection.
/// </summary>
public readonly struct LatticeTelemetryApiAuthorizationContext
{
    /// <summary>Initialises the authorization context.</summary>
    /// <param name="call">The underlying gRPC server call context.</param>
    /// <param name="operation">The telemetry operation being invoked.</param>
    /// <param name="targetId">
    /// The curated query id the call selects, or <see langword="null"/> for an
    /// operation that selects no single query.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="call"/> is <see langword="null"/>.</exception>
    public LatticeTelemetryApiAuthorizationContext(
        ServerCallContext call,
        LatticeTelemetryApiOperation operation,
        string? targetId)
    {
        ArgumentNullException.ThrowIfNull(call);
        Call = call;
        Operation = operation;
        TargetId = targetId;
    }

    /// <summary>The underlying gRPC server call context (headers, deadline, peer).</summary>
    public ServerCallContext Call { get; }

    /// <summary>The telemetry operation being invoked.</summary>
    public LatticeTelemetryApiOperation Operation { get; }

    /// <summary>
    /// The <b>curated query id</b> the call selects, or <see langword="null"/> for
    /// an operation that selects no single query (catalogue discovery).
    /// </summary>
    /// <remarks>
    /// This is deliberately the query id and never a tenant id. The effective
    /// tenant is derived server-side by the facade from the authenticated caller;
    /// surfacing a request-supplied tenant here would invite a host policy to make
    /// a decision on a value the wire controls, which is exactly the bypassable
    /// path a routable facade exists to prevent.
    /// </remarks>
    public string? TargetId { get; }
}
