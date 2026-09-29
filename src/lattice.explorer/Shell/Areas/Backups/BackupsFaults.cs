using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Shell.Transport;

namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>
/// Turns a backup facade fault into one plain sentence for the page. The server
/// is the fail-closed authorization point for every backup action, so a denial
/// is reported as "not permitted" rather than as an error, and nothing here ever
/// widens what the caller may do.
/// </summary>
internal static class BackupsFaults
{
    /// <summary>What a denial says.</summary>
    public const string NotPermitted = "You are not permitted to do this. Ask an administrator for the backup grant on this tree.";

    /// <summary>What an operation the connection does not serve says.</summary>
    public const string NotServed = "This connection does not serve this operation. Run it from a silo host, where the backup control API is in process.";

    /// <summary>What an unreachable cluster says.</summary>
    public const string Unreachable = "The cluster could not be reached. Check the connection and try again.";

    /// <summary>What a missing backup says.</summary>
    public const string NotFound = "That backup no longer exists.";

    /// <summary>What a restore through an unshared backup store says.</summary>
    public const string UnsharedStore =
        "This restore could not be completed because the backup is not reachable from every cluster. A multi-cluster "
        + "restore needs one backup store that every cluster shares; otherwise a backup captured on one cluster is "
        + "invisible to its peers and the coordinated restore is aborted. Configure every cluster with the same shared "
        + "backup sink, then try again.";

    /// <summary>One sentence for <paramref name="exception"/>.</summary>
    /// <param name="exception">The fault.</param>
    public static string Describe(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return exception switch
        {
            LatticeAuthorizationDeniedException => NotPermitted,
            UnauthorizedAccessException => NotPermitted,
            NotSupportedException => NotServed,
            KeyNotFoundException => NotFound,
            LatticeRestoreValidationException validation => "The backup failed validation: " + Detail(validation),
            ShellTransportException { IsTransient: true } => Unreachable,
            InvalidOperationException invalid when IndicatesUnsharedStore(invalid.Message) => UnsharedStore + " Server detail: " + invalid.Message,
            ArgumentException argument => "The request was not accepted: " + Detail(argument),
            InvalidOperationException invalid => "The operation could not be completed: " + Detail(invalid),
            ShellTransportException transport => "The operation failed: " + Detail(transport),
            _ => "The operation failed unexpectedly.",
        };
    }

    /// <summary>Whether <paramref name="exception"/> is a denial.</summary>
    /// <param name="exception">The fault.</param>
    public static bool IsDenied(Exception exception) =>
        exception is LatticeAuthorizationDeniedException or UnauthorizedAccessException;

    /// <summary>Whether <paramref name="exception"/> is the caller cancelling, which is never reported as a fault.</summary>
    /// <param name="exception">The fault.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    public static bool IsCancellation(Exception exception, CancellationToken cancellationToken) =>
        exception is OperationCanceledException && cancellationToken.IsCancellationRequested;

    private static bool IndicatesUnsharedStore(string? detail) =>
        detail is not null
        && (detail.Contains("could not prepare", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("absent from the sink", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("catalog or sink", StringComparison.OrdinalIgnoreCase));

    private static string Detail(Exception exception)
    {
        var message = exception.Message.Trim();
        return message.Length == 0 ? "no reason was given." : message;
    }
}
