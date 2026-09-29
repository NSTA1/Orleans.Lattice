namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>
/// Turns a facade fault into one plain sentence for the page. The transport maps
/// every wire status onto a small set of BCL exceptions (T1's fault table), so the
/// area describes those and never shows a stack or a status code.
/// </summary>
internal static class ClusterFaults
{
    /// <summary>The sentence shown when the caller may not perform an operation.</summary>
    public const string Denied = "You do not have permission to do this.";

    /// <summary>Describes <paramref name="exception"/> in one sentence.</summary>
    /// <param name="exception">The fault.</param>
    /// <returns>The sentence.</returns>
    public static string Describe(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return exception switch
        {
            UnauthorizedAccessException => Denied,
            _ when !string.IsNullOrWhiteSpace(exception.Message) && exception.Message != DefaultMessage(exception) => exception.Message,
            KeyNotFoundException => "The cluster does not know that tree.",
            NotSupportedException => "This cluster does not serve that operation.",
            TimeoutException => "The cluster did not answer in time.",
            _ => "The cluster could not complete the request.",
        };
    }

    private static string? DefaultMessage(Exception exception) => exception switch
    {
        KeyNotFoundException => DefaultKeyNotFound,
        NotSupportedException => DefaultNotSupported,
        TimeoutException => DefaultTimeout,
        InvalidOperationException => DefaultInvalidOperation,
        _ => null,
    };

    private static readonly string DefaultKeyNotFound = new KeyNotFoundException().Message;
    private static readonly string DefaultNotSupported = new NotSupportedException().Message;
    private static readonly string DefaultTimeout = new TimeoutException().Message;
    private static readonly string DefaultInvalidOperation = new InvalidOperationException().Message;

    /// <summary>Whether <paramref name="exception"/> is an authorization denial.</summary>
    /// <param name="exception">The fault.</param>
    /// <returns><see langword="true"/> for a denial.</returns>
    public static bool IsDenied(Exception exception) => exception is UnauthorizedAccessException;
}
