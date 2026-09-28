namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Thrown by <see cref="ILatticeAppBridge"/> when a request is refused or cannot be served,
/// carrying a closed <see cref="AppBridgeFailure"/> code a transport maps without parsing
/// the message.
/// </summary>
/// <remarks>
/// Derives directly from <see cref="Exception"/> so Orleans can deep-copy it across a
/// co-located grain-call boundary without a hand-written copier, and is serializable so
/// the code propagates intact across a silo boundary. The message reaches an untrusted
/// app UI, so it must be sanitised: it never names a physical tree id, a subject, or any
/// other rule or key the caller could not otherwise see.
/// </remarks>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppBridgeException)]
public sealed class AppBridgeException : Exception
{
    /// <summary>
    /// Initialises a <see cref="AppBridgeFailure.Denied"/> failure with its fixed message.
    /// Provided to satisfy the framework's exception-construction contract; throw sites
    /// use an overload that names the failure.
    /// </summary>
    public AppBridgeException()
        : this(AppBridgeFailure.Denied)
    {
    }

    /// <summary>Initialises a failure with the fixed, sanitised message for its code.</summary>
    /// <param name="failure">The failure code; must be a defined <see cref="AppBridgeFailure"/> member.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="failure"/> is not a defined code.</exception>
    public AppBridgeException(AppBridgeFailure failure)
        : base(DefaultMessage(failure))
        => Failure = failure;

    /// <summary>Initialises a failure with a caller-supplied, already sanitised message.</summary>
    /// <param name="failure">The failure code; must be a defined <see cref="AppBridgeFailure"/> member.</param>
    /// <param name="message">The non-null sanitised message.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="failure"/> is not a defined code.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
    public AppBridgeException(AppBridgeFailure failure, string message)
        : base(message ?? throw new ArgumentNullException(nameof(message)))
    {
        _ = DefaultMessage(failure);
        Failure = failure;
    }

    /// <summary>The reason the request failed.</summary>
    [Id(0)]
    public AppBridgeFailure Failure { get; }

    /// <summary>Returns the fixed, sanitised message for a failure code.</summary>
    /// <param name="failure">The failure code.</param>
    /// <returns>A message that discloses nothing beyond the code itself.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="failure"/> is not a defined code.</exception>
    public static string DefaultMessage(AppBridgeFailure failure) => failure switch
    {
        AppBridgeFailure.Denied => "The app bridge request was denied.",
        AppBridgeFailure.NotFound => "The app, install revision or tree was not found.",
        AppBridgeFailure.Invalid => "The app bridge request was invalid.",
        AppBridgeFailure.TooLarge => "The app bridge request or response was too large.",
        AppBridgeFailure.Conflict => "The app bridge request conflicted with the current state.",
        AppBridgeFailure.Unavailable => "The app bridge is unavailable.",
        _ => throw new ArgumentOutOfRangeException(nameof(failure), failure, "Unknown app bridge failure code."),
    };
}
