namespace Orleans.Lattice.Explorer.AppKit;

/// <summary>
/// The names and bounds of the AppKit frame protocol, version 1: the messages a
/// sandboxed app frame and its Explorer host exchange, the bridge operations an
/// app's UI may request, and the closed error and failure code sets.
/// </summary>
/// <remarks>
/// <para>
/// The in-frame runtime (<c>appkit/v1/boot.js</c>) and the protocol schema
/// (<c>appkit/v1/protocol.schema.json</c>) spell these same literals, and the
/// host-side frame component and bridge broker compile against this class, so
/// the two ends of the port cannot drift. The operation names are the bridge
/// vocabulary that <c>Orleans.Lattice.Apps.AppUiBridgeOperations</c> owns; a
/// test pins them to it.
/// </para>
/// <para>
/// The protocol has no lifecycle, consent, authentication, cross-app, fetch or
/// frame-sizing messages. A frame reaches the cluster only through the
/// operations in <see cref="Operations"/>, and only within the scope its app
/// was consented for.
/// </para>
/// </remarks>
public static class AppKitProtocol
{
    /// <summary>The protocol version this kit speaks, carried as <c>protocol</c> on every control message.</summary>
    public const int Version = 1;

    /// <summary>The kit's static-asset directory, relative to the AppKit package's static-web-asset base path.</summary>
    public const string AssetDirectory = "appkit/v1";

    /// <summary>The file name of the app-agnostic bootstrap document inside <see cref="AssetDirectory"/>.</summary>
    public const string FrameDocument = "frame.html";

    /// <summary>
    /// The pattern a logical tree name matches: the app manifest's local-name rule.
    /// It admits no <c>/</c>, so no physical tree id can be spelled as one.
    /// </summary>
    public const string TreeNamePattern = "^[a-z][a-z0-9_-]*$";

    /// <summary>
    /// The window and port control messages. Every control message is an object
    /// whose <c>type</c> is one of these names.
    /// </summary>
    public static class Messages
    {
        /// <summary>
        /// Frame to parent window, posted once when the bootstrap starts:
        /// <c>{ type, protocol }</c>.
        /// </summary>
        public const string Ready = "lattice.ready";

        /// <summary>
        /// Parent window to frame, with exactly one transferred <c>MessagePort</c>:
        /// <c>{ type, protocol }</c>. The frame accepts only the first one.
        /// </summary>
        public const string Hello = "lattice.hello";

        /// <summary>
        /// Host to frame over the port, exactly once:
        /// <c>{ type, protocol, appearance, bundle }</c>, carrying every verified bundle asset.
        /// </summary>
        public const string Bundle = "lattice.bundle";

        /// <summary>Frame to host over the port, after the last bundle script has loaded: <c>{ type, protocol }</c>.</summary>
        public const string Loaded = "lattice.loaded";

        /// <summary>
        /// Frame to host, when the frame could not load its bundle:
        /// <c>{ type, protocol, code, message }</c>, with <c>code</c> in <see cref="FailureCodes"/>.
        /// </summary>
        public const string Failed = "lattice.failed";

        /// <summary>Every control message name, in protocol order.</summary>
        public static IReadOnlyList<string> All { get; } = [Ready, Hello, Bundle, Loaded, Failed];
    }

    /// <summary>
    /// The events the host sends the frame over the port, as
    /// <c>{ type, data }</c>. App code subscribes with <c>lattice.on(name, handler)</c>.
    /// </summary>
    public static class Events
    {
        /// <summary>The Explorer's appearance changed: <c>data</c> is <c>{ theme, contrast, density, reducedMotion }</c>.</summary>
        public const string ContextChanged = "context.changed";

        /// <summary>The host moved the frame's internal path (for example, history navigation): <c>data</c> is <c>{ path }</c>.</summary>
        public const string NavChanged = "nav.changed";

        /// <summary>The host withdrew the frame's authority: <c>data</c> is <c>{ reason }</c>, one of <see cref="RevokedReasons"/>.</summary>
        public const string Revoked = "lattice.revoked";

        /// <summary>Every host event name.</summary>
        public static IReadOnlyList<string> All { get; } = [ContextChanged, NavChanged, Revoked];
    }

    /// <summary>The reasons a <see cref="Events.Revoked"/> event may carry.</summary>
    public static class RevokedReasons
    {
        /// <summary>The app was disabled.</summary>
        public const string Disabled = "disabled";

        /// <summary>The app was uninstalled.</summary>
        public const string Uninstalled = "uninstalled";

        /// <summary>The app was upgraded, so the running bundle is no longer the installed one.</summary>
        public const string Upgraded = "upgraded";

        /// <summary>The cluster refused the frame's install revision.</summary>
        public const string Revision = "revision";

        /// <summary>The host closed the frame for any other reason.</summary>
        public const string Closed = "closed";

        /// <summary>Every revocation reason.</summary>
        public static IReadOnlyList<string> All { get; } = [Disabled, Uninstalled, Upgraded, Revision, Closed];
    }

    /// <summary>
    /// The bridge operations a request's <c>op</c> may name - exactly the bridge
    /// vocabulary of <c>Orleans.Lattice.Apps.AppUiBridgeOperations</c>.
    /// </summary>
    public static class Operations
    {
        /// <summary>Reads the frame's context: slug, version, protocol, appearance and tenant display name.</summary>
        public const string ContextRead = "context.read";

        /// <summary>Reads the signed-in user's display name, and nothing else about them.</summary>
        public const string ContextUser = "context.user";

        /// <summary>Reads app-owned data: <see cref="DataActions.Get"/> or <see cref="DataActions.Scan"/>.</summary>
        public const string DataRead = "data.read";

        /// <summary>Writes app-owned data: <see cref="DataActions.Set"/>.</summary>
        public const string DataWrite = "data.write";

        /// <summary>Deletes app-owned data: <see cref="DataActions.Delete"/>.</summary>
        public const string DataDelete = "data.delete";

        /// <summary>Reports the frame's internal path so the host can reflect it in the address line.</summary>
        public const string NavSync = "nav.sync";

        /// <summary>Asks the host to show a short, text-only notification.</summary>
        public const string UiNotify = "ui.notify";

        /// <summary>Every operation name.</summary>
        public static IReadOnlyList<string> All { get; } =
            [ContextRead, ContextUser, DataRead, DataWrite, DataDelete, NavSync, UiNotify];
    }

    /// <summary>The <c>action</c> argument of the data operations.</summary>
    public static class DataActions
    {
        /// <summary>Reads one key (<see cref="Operations.DataRead"/>).</summary>
        public const string Get = "get";

        /// <summary>Reads one page of keys under a prefix (<see cref="Operations.DataRead"/>).</summary>
        public const string Scan = "scan";

        /// <summary>Writes one key (<see cref="Operations.DataWrite"/>).</summary>
        public const string Set = "set";

        /// <summary>Deletes one key (<see cref="Operations.DataDelete"/>).</summary>
        public const string Delete = "delete";

        /// <summary>Every data action.</summary>
        public static IReadOnlyList<string> All { get; } = [Get, Scan, Set, Delete];
    }

    /// <summary>The closed set of <c>error.code</c> values a failed response carries.</summary>
    public static class ErrorCodes
    {
        /// <summary>The request is outside the frame's grants or the user's rights.</summary>
        public const string Denied = "denied";

        /// <summary>The app, revision, tree or key does not exist, or is not visible to the caller.</summary>
        public const string NotFound = "not_found";

        /// <summary>The request is malformed.</summary>
        public const string Invalid = "invalid";

        /// <summary>A key, value, request or response exceeds a bound in <see cref="Limits"/>.</summary>
        public const string TooLarge = "too_large";

        /// <summary>The frame exceeded its request rate or concurrency bound.</summary>
        public const string RateLimited = "rate_limited";

        /// <summary>The request could not be served now, including a timeout or a closed port.</summary>
        public const string Unavailable = "unavailable";

        /// <summary>The request conflicts with the tree's current state.</summary>
        public const string Conflict = "conflict";

        /// <summary>Every error code.</summary>
        public static IReadOnlyList<string> All { get; } =
            [Denied, NotFound, Invalid, TooLarge, RateLimited, Unavailable, Conflict];
    }

    /// <summary>The closed set of <c>code</c> values a <see cref="Messages.Failed"/> message carries.</summary>
    public static class FailureCodes
    {
        /// <summary>The hello or bundle named a protocol version this kit does not speak.</summary>
        public const string ProtocolUnsupported = "protocol_unsupported";

        /// <summary>The bundle message is not the shape the protocol defines.</summary>
        public const string BundleMalformed = "bundle_malformed";

        /// <summary>The bundle has too many assets, or an asset or the whole bundle is too large.</summary>
        public const string BundleTooLarge = "bundle_too_large";

        /// <summary>The entry, a stylesheet or a script names an asset the bundle does not carry.</summary>
        public const string AssetMissing = "asset_missing";

        /// <summary>An asset's bytes do not hash to its SHA-256 digest.</summary>
        public const string DigestMismatch = "digest_mismatch";

        /// <summary>The assets' digests do not recompute to the bundle digest.</summary>
        public const string BundleDigestMismatch = "bundle_digest_mismatch";

        /// <summary>The frame has no Web Crypto digest, so it cannot verify the bundle.</summary>
        public const string CryptoUnavailable = "crypto_unavailable";

        /// <summary>A stylesheet or script failed to load.</summary>
        public const string LoadFailed = "load_failed";

        /// <summary>Any other bootstrap fault.</summary>
        public const string Internal = "internal";

        /// <summary>Every failure code.</summary>
        public static IReadOnlyList<string> All { get; } =
        [
            ProtocolUnsupported, BundleMalformed, BundleTooLarge, AssetMissing, DigestMismatch,
            BundleDigestMismatch, CryptoUnavailable, LoadFailed, Internal,
        ];
    }

    /// <summary>The size and count bounds both ends of the port enforce.</summary>
    public static class Limits
    {
        /// <summary>The largest decoded value a data request or response may carry: 64 KiB.</summary>
        public const int MaxValueBytes = 64 * 1024;

        /// <summary>The largest base64 value string, which encodes <see cref="MaxValueBytes"/> bytes.</summary>
        public const int MaxValueBase64Length = (MaxValueBytes + 2) / 3 * 4;

        /// <summary>The largest request envelope, measured as the UTF-8 bytes of its JSON: 128 KiB.</summary>
        public const int MaxRequestBytes = 128 * 1024;

        /// <summary>The largest response, measured as the UTF-8 bytes of its JSON: 1 MiB.</summary>
        public const int MaxResponseBytes = 1024 * 1024;

        /// <summary>The largest page a scan may request.</summary>
        public const int MaxPageSize = 200;

        /// <summary>The longest notification text, in UTF-16 code units.</summary>
        public const int MaxNotifyLength = 200;

        /// <summary>The longest key or prefix, in UTF-16 code units.</summary>
        public const int MaxKeyLength = 1024;

        /// <summary>The longest logical tree name.</summary>
        public const int MaxTreeNameLength = 128;

        /// <summary>The longest internal path <see cref="Operations.NavSync"/> may report.</summary>
        public const int MaxPathLength = 1024;

        /// <summary>The longest scan continuation token.</summary>
        public const int MaxContinuationLength = 4096;

        /// <summary>The kit's default request timeout, in milliseconds.</summary>
        public const int DefaultTimeoutMilliseconds = 30_000;

        /// <summary>The longest request timeout app code may ask for, in milliseconds.</summary>
        public const int MaxTimeoutMilliseconds = 300_000;
    }
}
