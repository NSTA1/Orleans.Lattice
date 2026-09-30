using System.Collections.Frozen;
using Orleans.Lattice.Explorer.AppKit;

namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>
/// The host side of AppKit frame protocol v1 (F5, issue #3814): message and event type
/// names, the bridge operations, the closed error-code set, and the size limits the
/// broker enforces.
/// </summary>
/// <remarks>
/// Every name and limit is taken from AppKit's public <see cref="AppKitProtocol"/>, the
/// protocol's one owner, so the host cannot drift from the frame. The Shell does not
/// reference <c>lattice.apps</c>, so the operation set is additionally pinned to F1's
/// <c>AppUiBridgeOperations</c> by test. The protocol has no lifecycle, consent, auth,
/// cross-app, fetch or auto-height message, by design.
/// </remarks>
internal static class AppFrameProtocol
{
    /// <summary>The protocol version the host speaks.</summary>
    public const int Version = AppKitProtocol.Version;

    /// <summary>Frame to parent over <c>window</c>: the bootstrap is ready for its port.</summary>
    public const string Ready = AppKitProtocol.Messages.Ready;

    /// <summary>Parent to frame over <c>window</c>: carries the one transferred port.</summary>
    public const string Hello = AppKitProtocol.Messages.Hello;

    /// <summary>Host to frame over the port, exactly once: the verified bundle.</summary>
    public const string Bundle = AppKitProtocol.Messages.Bundle;

    /// <summary>Frame to host: the last bundle script loaded (informational).</summary>
    public const string Loaded = AppKitProtocol.Messages.Loaded;

    /// <summary>Frame to host: the bootstrap failed.</summary>
    public const string Failed = AppKitProtocol.Messages.Failed;

    /// <summary>Host event: the appearance changed.</summary>
    public const string ContextChanged = AppKitProtocol.Events.ContextChanged;

    /// <summary>Host event: the host navigated the frame's in-app path.</summary>
    public const string NavChanged = AppKitProtocol.Events.NavChanged;

    /// <summary>Host event: the frame's authority was revoked; the host then tears it down.</summary>
    public const string Revoked = AppKitProtocol.Events.Revoked;

    /// <summary>Reads the frame's launch context.</summary>
    public const string ContextRead = AppKitProtocol.Operations.ContextRead;

    /// <summary>Reads the signed-in user's display name; consented separately.</summary>
    public const string ContextUser = AppKitProtocol.Operations.ContextUser;

    /// <summary>Reads keys and ranges from the app's own trees.</summary>
    public const string DataRead = AppKitProtocol.Operations.DataRead;

    /// <summary>Writes a key to the app's own trees.</summary>
    public const string DataWrite = AppKitProtocol.Operations.DataWrite;

    /// <summary>Deletes a key from the app's own trees.</summary>
    public const string DataDelete = AppKitProtocol.Operations.DataDelete;

    /// <summary>Reports the frame's in-app path.</summary>
    public const string NavSync = AppKitProtocol.Operations.NavSync;

    /// <summary>Raises a host-rendered, text-only notification.</summary>
    public const string UiNotify = AppKitProtocol.Operations.UiNotify;

    /// <summary>The <c>data.read</c> action for one key.</summary>
    public const string ActionGet = AppKitProtocol.DataActions.Get;

    /// <summary>The <c>data.read</c> action for a page of keys.</summary>
    public const string ActionScan = AppKitProtocol.DataActions.Scan;

    /// <summary>The <c>data.write</c> action.</summary>
    public const string ActionSet = AppKitProtocol.DataActions.Set;

    /// <summary>The <c>data.delete</c> action.</summary>
    public const string ActionDelete = AppKitProtocol.DataActions.Delete;

    /// <summary>Error code: refused.</summary>
    public const string ErrorDenied = AppKitProtocol.ErrorCodes.Denied;

    /// <summary>Error code: the key, tree or app was not found.</summary>
    public const string ErrorNotFound = AppKitProtocol.ErrorCodes.NotFound;

    /// <summary>Error code: the request was malformed.</summary>
    public const string ErrorInvalid = AppKitProtocol.ErrorCodes.Invalid;

    /// <summary>Error code: the request or response exceeded a size limit.</summary>
    public const string ErrorTooLarge = AppKitProtocol.ErrorCodes.TooLarge;

    /// <summary>Error code: the frame exceeded its rate or concurrency limit.</summary>
    public const string ErrorRateLimited = AppKitProtocol.ErrorCodes.RateLimited;

    /// <summary>Error code: the cluster could not serve the request.</summary>
    public const string ErrorUnavailable = AppKitProtocol.ErrorCodes.Unavailable;

    /// <summary>Error code: the request conflicted with current state.</summary>
    public const string ErrorConflict = AppKitProtocol.ErrorCodes.Conflict;

    /// <summary>Revocation reason: the app was disabled.</summary>
    public const string RevokedDisabled = AppKitProtocol.RevokedReasons.Disabled;

    /// <summary>Revocation reason: the app was uninstalled.</summary>
    public const string RevokedUninstalled = AppKitProtocol.RevokedReasons.Uninstalled;

    /// <summary>Revocation reason: the app was upgraded.</summary>
    public const string RevokedUpgraded = AppKitProtocol.RevokedReasons.Upgraded;

    /// <summary>Revocation reason: the launch's install revision is no longer current.</summary>
    public const string RevokedRevision = AppKitProtocol.RevokedReasons.Revision;

    /// <summary>Revocation reason: the host closed the frame.</summary>
    public const string RevokedClosed = AppKitProtocol.RevokedReasons.Closed;

    /// <summary>The largest decoded value, in bytes.</summary>
    public const int MaxValueBytes = AppKitProtocol.Limits.MaxValueBytes;

    /// <summary>The largest response, in UTF-8 bytes of its JSON.</summary>
    public const int MaxResponseBytes = AppKitProtocol.Limits.MaxResponseBytes;

    /// <summary>The largest request envelope, in UTF-8 bytes of its JSON.</summary>
    public const int MaxRequestBytes = AppKitProtocol.Limits.MaxRequestBytes;

    /// <summary>The largest scan page.</summary>
    public const int MaxPageSize = AppKitProtocol.Limits.MaxPageSize;

    /// <summary>The scan page size when the frame does not ask for one.</summary>
    public const int DefaultPageSize = 50;

    /// <summary>The longest <c>ui.notify</c> text, in characters.</summary>
    public const int MaxNotifyLength = AppKitProtocol.Limits.MaxNotifyLength;

    /// <summary>The longest key, in characters.</summary>
    public const int MaxKeyLength = AppKitProtocol.Limits.MaxKeyLength;

    /// <summary>The longest logical tree name, in characters.</summary>
    public const int MaxTreeNameLength = AppKitProtocol.Limits.MaxTreeNameLength;

    /// <summary>The longest <c>nav.sync</c> path, in characters.</summary>
    public const int MaxPathLength = AppKitProtocol.Limits.MaxPathLength;

    /// <summary>The longest scan continuation, in characters.</summary>
    public const int MaxContinuationLength = AppKitProtocol.Limits.MaxContinuationLength;

    /// <summary>The most role names a <c>context.read</c> result carries.</summary>
    public const int MaxRoles = AppKitProtocol.Limits.MaxRoles;

    /// <summary>The longest role name a <c>context.read</c> result carries.</summary>
    public const int MaxRoleNameLength = AppKitProtocol.Limits.MaxRoleNameLength;

    /// <summary>The largest request id (2^53 - 1, the largest integer a JavaScript number holds exactly).</summary>
    public const long MaxRequestId = (1L << 53) - 1;

    /// <summary>Every bridge operation, compared ordinally.</summary>
    public static readonly FrozenSet<string> Operations = AppKitProtocol.Operations.All.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>Every error code a response may carry, compared ordinally.</summary>
    public static readonly FrozenSet<string> ErrorCodes = AppKitProtocol.ErrorCodes.All.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>Every <c>lattice.failed</c> code the frame may report; anything else is reported as <c>internal</c>.</summary>
    public static readonly FrozenSet<string> FailureCodes = AppKitProtocol.FailureCodes.All.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>Returns whether <paramref name="operation"/> is a tree-scoped <c>data.*</c> operation.</summary>
    /// <param name="operation">The operation name.</param>
    /// <returns><see langword="true"/> for <see cref="DataRead"/>, <see cref="DataWrite"/> and <see cref="DataDelete"/>.</returns>
    public static bool IsDataOperation(string? operation) => operation is DataRead or DataWrite or DataDelete;

    /// <summary>
    /// Returns whether <paramref name="tree"/> has the shape of a logical tree name:
    /// 1 to <see cref="MaxTreeNameLength"/> characters matching AppKit's
    /// <see cref="AppKitProtocol.TreeNamePattern"/>, <c>^[a-z][a-z0-9_-]*$</c> (the manifest's
    /// local-name rule). A physical tree id (which carries <c>/</c> or <c>:</c>) never fits.
    /// </summary>
    /// <param name="tree">The candidate name.</param>
    /// <returns><see langword="true"/> when the name is well formed.</returns>
    public static bool IsLogicalTreeName(ReadOnlySpan<char> tree) => IsManifestName(tree, MaxTreeNameLength);

    /// <summary>
    /// Returns whether <paramref name="role"/> is a well-formed app role name: the same
    /// manifest name rule as a tree (<see cref="AppKitProtocol.TreeNamePattern"/>), 1 to
    /// <see cref="MaxRoleNameLength"/> characters.
    /// </summary>
    /// <param name="role">The candidate name.</param>
    /// <returns><see langword="true"/> when the name is well formed.</returns>
    public static bool IsRoleName(ReadOnlySpan<char> role) => IsManifestName(role, MaxRoleNameLength);

    private static bool IsManifestName(ReadOnlySpan<char> name, int maxLength)
    {
        if (name.IsEmpty || name.Length > maxLength || name[0] is not (>= 'a' and <= 'z'))
        {
            return false;
        }

        foreach (var c in name)
        {
            if (!IsLowerAlphanumeric(c) && c is not '_' and not '-')
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsLowerAlphanumeric(char c) => c is (>= 'a' and <= 'z') or (>= '0' and <= '9');
}
