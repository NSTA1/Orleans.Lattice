using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// Translates a tenancy fault into the Explorer's
/// <see cref="TenantOperationStatus"/>, and decides which exceptions are faults
/// the seam folds into a result rather than letting escape.
/// <para>
/// The mapping is defined over the <em>facade's own exception contract</em> -
/// the exceptions <see cref="ITenantAdminClient"/> documents - so every
/// documented refusal has its own distinct status. A caller reaching the facade
/// over gRPC sees a narrower picture, because the binding collapses all five
/// precondition refusals onto a single <see cref="StatusCode.FailedPrecondition"/>
/// and keeps the specific reason only in the message; those land on
/// <see cref="TenantOperationStatus.PreconditionFailed"/> with that reason
/// carried verbatim.
/// </para>
/// </summary>
internal static class TenantFaultMapper
{
    /// <summary>
    /// Decides whether <paramref name="exception"/> is a fault the seam owns and
    /// should fold into a non-success result.
    /// <para>
    /// Returns <see langword="false"/> for a cancellation the caller asked for,
    /// so it propagates as an <see cref="OperationCanceledException"/> the way
    /// every other cancellable call in the Explorer behaves, and for anything
    /// outside the known fault families, so a genuine defect surfaces instead of
    /// being disguised as a server refusal.
    /// </para>
    /// </summary>
    /// <param name="exception">The exception observed while calling the client.</param>
    /// <param name="cancellationToken">The token the caller supplied.</param>
    /// <returns><see langword="true"/> when the seam should fold it into a result.</returns>
    public static bool IsFault(Exception exception, CancellationToken cancellationToken)
    {
        if (exception is OperationCanceledException)
        {
            return false;
        }

        if (exception is RpcException rpc)
        {
            // A cancellation the caller asked for is not a server fault: let it
            // surface as cancellation rather than as a rendered failure.
            return rpc.StatusCode != StatusCode.Cancelled || !cancellationToken.IsCancellationRequested;
        }

        return exception
            is TenancyUnavailableException
            or LatticeAuthorizationDeniedException
            or TenantNotFoundException
            or TenantAlreadyExistsException
            or ReservedTenantOperationException
            or TenantRegionNotAllowedException
            or TenantLastRegionException
            or TenantLastAdminSubjectException
            or TenantGrantNotFoundException
            or TenantGrantTransitionException
            or ArgumentException
            or InvalidOperationException;
    }

    /// <summary>
    /// Classifies <paramref name="exception"/> into the status that describes
    /// what the cluster actually refused.
    /// </summary>
    /// <param name="exception">The fault to classify. Must not be <see langword="null"/>.</param>
    /// <returns>The matching status.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    public static TenantOperationStatus Classify(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return exception switch
        {
            // The tenancy add-on, or the optional facade behind this RPC, is not
            // registered on the cluster. Not a denial and not a transport fault.
            TenancyUnavailableException => TenantOperationStatus.Unavailable,

            LatticeAuthorizationDeniedException => TenantOperationStatus.Denied,

            // The five documented precondition refusals, each kept distinct.
            ReservedTenantOperationException => TenantOperationStatus.ReservedTenant,
            TenantRegionNotAllowedException => TenantOperationStatus.RegionNotAllowed,
            TenantLastRegionException => TenantOperationStatus.LastRegion,
            TenantLastAdminSubjectException => TenantOperationStatus.LastAdminSubject,
            TenantGrantTransitionException => TenantOperationStatus.GrantTransitionRejected,

            // The two absences. The grant arm precedes the tenant arm only for
            // readability; the types are unrelated, so order carries no meaning.
            TenantGrantNotFoundException => TenantOperationStatus.GrantNotFound,
            TenantNotFoundException => TenantOperationStatus.NotFound,

            TenantAlreadyExistsException => TenantOperationStatus.AlreadyExists,

            RpcException rpc => ClassifyRpc(rpc),

            // A malformed request the seam rejected before the wire.
            ArgumentException => TenantOperationStatus.InvalidRequest,

            // The Explorer holds no endpoint yet, so there is nothing to call.
            InvalidOperationException => TenantOperationStatus.Failed,

            _ => TenantOperationStatus.Failed,
        };
    }

    /// <summary>
    /// Produces the message a panel renders for <paramref name="exception"/>,
    /// classified as <paramref name="status"/>.
    /// </summary>
    /// <param name="exception">The fault to describe. Must not be <see langword="null"/>.</param>
    /// <param name="status">The status <paramref name="exception"/> classified to.</param>
    /// <returns>A non-empty human-readable description.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    public static string Describe(Exception exception, TenantOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(exception);

        // An RpcException's own message prefixes the gRPC status line, which is
        // noise in a panel; its detail is the server's actual explanation.
        var message = exception is RpcException rpc ? rpc.Status.Detail : exception.Message;
        if (!string.IsNullOrWhiteSpace(message))
        {
            return message;
        }

        return status == TenantOperationStatus.Unavailable
            ? "This cluster does not serve tenant administration."
            : "The tenancy request failed.";
    }

    /// <summary>
    /// Builds the valueless failure result for <paramref name="exception"/>.
    /// </summary>
    /// <param name="exception">The fault to fold. Must not be <see langword="null"/>.</param>
    /// <returns>The classified, described failure result.</returns>
    public static TenantOperationResult Fail(Exception exception)
    {
        var status = Classify(exception);
        return TenantOperationResult.Failure(status, Describe(exception, status));
    }

    /// <summary>
    /// Builds the failure result for <paramref name="exception"/> on an
    /// operation that would otherwise have produced a
    /// <typeparamref name="TValue"/>.
    /// </summary>
    /// <typeparam name="TValue">The value the operation would have produced.</typeparam>
    /// <param name="exception">The fault to fold. Must not be <see langword="null"/>.</param>
    /// <returns>The classified, described failure result, carrying no value.</returns>
    public static TenantOperationResult<TValue> Fail<TValue>(Exception exception)
    {
        var status = Classify(exception);
        return TenantOperationResult<TValue>.Failure(status, Describe(exception, status));
    }

    /// <summary>
    /// Classifies a residual transport fault the client did not already
    /// translate into a typed exception.
    /// </summary>
    /// <remarks>
    /// <see cref="StatusCode.Unavailable"/> is deliberately <em>not</em>
    /// <see cref="TenantOperationStatus.Unavailable"/>: on the wire it means the
    /// server could not be reached, which is a retryable transport failure, not
    /// the permanent "this cluster has no such capability" that
    /// <see cref="StatusCode.Unimplemented"/> reports.
    /// </remarks>
    private static TenantOperationStatus ClassifyRpc(RpcException exception) => exception.StatusCode switch
    {
        StatusCode.Unimplemented => TenantOperationStatus.Unavailable,
        StatusCode.PermissionDenied => TenantOperationStatus.Denied,
        StatusCode.Unauthenticated => TenantOperationStatus.AuthenticationRequired,
        StatusCode.NotFound => TenantOperationStatus.NotFound,
        StatusCode.AlreadyExists => TenantOperationStatus.AlreadyExists,
        StatusCode.InvalidArgument => TenantOperationStatus.InvalidRequest,
        StatusCode.FailedPrecondition => TenantOperationStatus.PreconditionFailed,
        _ => TenantOperationStatus.Failed,
    };
}
