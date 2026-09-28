using System.Collections.Frozen;

namespace Orleans.Lattice.Explorer.Shell.Framing;

/// <summary>
/// The host side of AppKit frame protocol v1 (F5, issue #3814): message and event type
/// names, the bridge operations, the closed error-code set, and the size limits the
/// broker enforces.
/// </summary>
/// <remarks>
/// The Shell does not reference <c>lattice.apps</c>, so the operation names are
/// restated here and pinned to F1's <c>AppUiBridgeOperations</c> by test, and every
/// name and limit is pinned to AppKit's <c>AppKitProtocol</c> by test once that type
/// is present. The protocol has no lifecycle, consent, auth, cross-app, fetch or
/// auto-height message, by design.
/// </remarks>
internal static class AppFrameProtocol
{
    /// <summary>The protocol version the host speaks.</summary>
    public const int Version = 1;

    /// <summary>Frame to parent over <c>window</c>: the bootstrap is ready for its port.</summary>
    public const string Ready = "lattice.ready";

    /// <summary>Parent to frame over <c>window</c>: carries the one transferred port.</summary>
    public const string Hello = "lattice.hello";

    /// <summary>Host to frame over the port, exactly once: the verified bundle.</summary>
    public const string Bundle = "lattice.bundle";

    /// <summary>Frame to host: the last bundle script loaded (informational).</summary>
    public const string Loaded = "lattice.loaded";

    /// <summary>Frame to host: the bootstrap failed.</summary>
    public const string Failed = "lattice.failed";

    /// <summary>Host event: the appearance changed.</summary>
    public const string ContextChanged = "context.changed";

    /// <summary>Host event: the host navigated the frame's in-app path.</summary>
    public const string NavChanged = "nav.changed";

    /// <summary>Host event: the frame's authority was revoked; the host then tears it down.</summary>
    public const string Revoked = "lattice.revoked";

    /// <summary>Reads the frame's launch context.</summary>
    public const string ContextRead = "context.read";

    /// <summary>Reads the signed-in user's display name; consented separately.</summary>
    public const string ContextUser = "context.user";

    /// <summary>Reads keys and ranges from the app's own trees.</summary>
    public const string DataRead = "data.read";

    /// <summary>Writes a key to the app's own trees.</summary>
    public const string DataWrite = "data.write";

    /// <summary>Deletes a key from the app's own trees.</summary>
    public const string DataDelete = "data.delete";

    /// <summary>Reports the frame's in-app path.</summary>
    public const string NavSync = "nav.sync";

    /// <summary>Raises a host-rendered, text-only notification.</summary>
    public const string UiNotify = "ui.notify";

    /// <summary>The <c>data.read</c> action for one key.</summary>
    public const string ActionGet = "get";

    /// <summary>The <c>data.read</c> action for a page of keys.</summary>
    public const string ActionScan = "scan";

    /// <summary>The <c>data.write</c> action.</summary>
    public const string ActionSet = "set";

    /// <summary>The <c>data.delete</c> action.</summary>
    public const string ActionDelete = "delete";

    /// <summary>Error code: refused.</summary>
    public const string ErrorDenied = "denied";

    /// <summary>Error code: the key, tree or app was not found.</summary>
    public const string ErrorNotFound = "not_found";

    /// <summary>Error code: the request was malformed.</summary>
    public const string ErrorInvalid = "invalid";

    /// <summary>Error code: the request or response exceeded a size limit.</summary>
    public const string ErrorTooLarge = "too_large";

    /// <summary>Error code: the frame exceeded its rate or concurrency limit.</summary>
    public const string ErrorRateLimited = "rate_limited";

    /// <summary>Error code: the cluster could not serve the request.</summary>
    public const string ErrorUnavailable = "unavailable";

    /// <summary>Error code: the request conflicted with current state.</summary>
    public const string ErrorConflict = "conflict";

    /// <summary>Revocation reason: the app was disabled.</summary>
    public const string RevokedDisabled = "disabled";

    /// <summary>Revocation reason: the app was uninstalled.</summary>
    public const string RevokedUninstalled = "uninstalled";

    /// <summary>Revocation reason: the app was upgraded.</summary>
    public const string RevokedUpgraded = "upgraded";

    /// <summary>Revocation reason: the launch's install revision is no longer current.</summary>
    public const string RevokedRevision = "revision";

    /// <summary>Revocation reason: the host closed the frame.</summary>
    public const string RevokedClosed = "closed";

    /// <summary>The largest decoded value, in bytes.</summary>
    public const int MaxValueBytes = 64 * 1024;

    /// <summary>The largest response, in UTF-8 bytes of its JSON.</summary>
    public const int MaxResponseBytes = 1024 * 1024;

    /// <summary>The largest request envelope, in UTF-8 bytes of its JSON.</summary>
    public const int MaxRequestBytes = 128 * 1024;

    /// <summary>The largest scan page.</summary>
    public const int MaxPageSize = 200;

    /// <summary>The scan page size when the frame does not ask for one.</summary>
    public const int DefaultPageSize = 50;

    /// <summary>The longest <c>ui.notify</c> text, in characters.</summary>
    public const int MaxNotifyLength = 200;

    /// <summary>The longest key, in characters.</summary>
    public const int MaxKeyLength = 1024;

    /// <summary>The longest logical tree name, in characters.</summary>
    public const int MaxTreeNameLength = 128;

    /// <summary>The longest <c>nav.sync</c> path, in characters.</summary>
    public const int MaxPathLength = 1024;

    /// <summary>The longest scan continuation, in characters.</summary>
    public const int MaxContinuationLength = 4096;

    /// <summary>The largest request id (2^53 - 1, the largest integer a JavaScript number holds exactly).</summary>
    public const long MaxRequestId = (1L << 53) - 1;

    /// <summary>Every bridge operation, compared ordinally.</summary>
    public static readonly FrozenSet<string> Operations = new[]
    {
        ContextRead, ContextUser, DataRead, DataWrite, DataDelete, NavSync, UiNotify,
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>Every error code a response may carry, compared ordinally.</summary>
    public static readonly FrozenSet<string> ErrorCodes = new[]
    {
        ErrorDenied, ErrorNotFound, ErrorInvalid, ErrorTooLarge, ErrorRateLimited, ErrorUnavailable, ErrorConflict,
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>Every <c>lattice.failed</c> code the frame may report; anything else is reported as <c>internal</c>.</summary>
    public static readonly FrozenSet<string> FailureCodes = new[]
    {
        "protocol_unsupported", "bundle_malformed", "bundle_too_large", "asset_missing", "digest_mismatch",
        "bundle_digest_mismatch", "crypto_unavailable", "load_failed", "internal",
    }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>Returns whether <paramref name="operation"/> is a tree-scoped <c>data.*</c> operation.</summary>
    /// <param name="operation">The operation name.</param>
    /// <returns><see langword="true"/> for <see cref="DataRead"/>, <see cref="DataWrite"/> and <see cref="DataDelete"/>.</returns>
    public static bool IsDataOperation(string? operation) => operation is DataRead or DataWrite or DataDelete;

    /// <summary>
    /// Returns whether <paramref name="tree"/> has the shape of a logical tree name:
    /// 1 to <see cref="MaxTreeNameLength"/> characters of <c>[a-z0-9][a-z0-9._-]*</c>. A
    /// physical tree id (which carries <c>/</c> or <c>:</c>) never fits.
    /// </summary>
    /// <param name="tree">The candidate name.</param>
    /// <returns><see langword="true"/> when the name is well formed.</returns>
    public static bool IsLogicalTreeName(ReadOnlySpan<char> tree)
    {
        if (tree.IsEmpty || tree.Length > MaxTreeNameLength || !IsLowerAlphanumeric(tree[0]))
        {
            return false;
        }

        foreach (var c in tree)
        {
            if (!IsLowerAlphanumeric(c) && c is not '.' and not '_' and not '-')
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsLowerAlphanumeric(char c) => c is (>= 'a' and <= 'z') or (>= '0' and <= '9');
}
