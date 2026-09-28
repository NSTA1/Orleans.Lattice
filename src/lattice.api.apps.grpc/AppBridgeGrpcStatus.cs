using Grpc.Core;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>
/// The one-to-one mapping between <see cref="AppBridgeFailure"/> codes and gRPC status codes, used by the
/// bridge service to send a failure and by the bridge client to rebuild it. Only the code crosses the wire; the
/// message is always the fixed, sanitised one for the code.
/// </summary>
internal static class AppBridgeGrpcStatus
{
    /// <summary>Returns the status code a failure is sent as. An undefined code is sent as a denial.</summary>
    /// <param name="failure">The failure code.</param>
    /// <returns>The gRPC status code.</returns>
    public static StatusCode ToStatusCode(AppBridgeFailure failure) => failure switch
    {
        AppBridgeFailure.NotFound => StatusCode.NotFound,
        AppBridgeFailure.Invalid => StatusCode.InvalidArgument,
        AppBridgeFailure.TooLarge => StatusCode.ResourceExhausted,
        AppBridgeFailure.Conflict => StatusCode.Aborted,
        AppBridgeFailure.Unavailable => StatusCode.Unavailable,
        _ => StatusCode.PermissionDenied,
    };

    /// <summary>
    /// Returns the failure a status code received from the bridge service stands for. Authentication and
    /// authorization refusals are denials; any code the service never sends for a failure (including an internal
    /// error or an exceeded deadline) is <see cref="AppBridgeFailure.Unavailable"/>.
    /// </summary>
    /// <param name="code">The gRPC status code.</param>
    /// <returns>The failure code.</returns>
    public static AppBridgeFailure ToFailure(StatusCode code) => code switch
    {
        StatusCode.PermissionDenied or StatusCode.Unauthenticated => AppBridgeFailure.Denied,
        StatusCode.NotFound => AppBridgeFailure.NotFound,
        StatusCode.InvalidArgument => AppBridgeFailure.Invalid,
        StatusCode.ResourceExhausted or StatusCode.OutOfRange => AppBridgeFailure.TooLarge,
        StatusCode.Aborted or StatusCode.FailedPrecondition => AppBridgeFailure.Conflict,
        _ => AppBridgeFailure.Unavailable,
    };

    /// <summary>Builds the sanitised status a failure is sent as.</summary>
    /// <param name="failure">The failure code.</param>
    /// <returns>The status, carrying only the fixed message for the sent code.</returns>
    public static Status ToStatus(AppBridgeFailure failure)
    {
        var code = ToStatusCode(failure);
        return new Status(code, AppBridgeException.DefaultMessage(ToFailure(code)));
    }
}
