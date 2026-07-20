using Microsoft.Extensions.Primitives;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// The baseline security-response-header policy emitted on every response from
/// the Orleans.Lattice Explorer web head. The policy is anti-clickjacking first
/// (CWE-1021): the authenticated admin console must not be frameable by a
/// foreign origin. Values are exposed as constants (and pre-wrapped
/// <see cref="StringValues"/>) so the per-response middleware never allocates a
/// string or <see cref="StringValues"/> per request.
/// </summary>
internal static class ExplorerSecurityHeaders
{
    /// <summary>
    /// The <c>Content-Security-Policy</c> value. <c>frame-ancestors 'none'</c> is
    /// the primary clickjacking control; the remaining directives are the
    /// tightest set compatible with the Blazor Web App (interactive server)
    /// asset model. <c>'unsafe-inline'</c> is retained for <c>script-src</c> and
    /// <c>style-src</c> because Blazor Web App streaming SSR injects inline
    /// <c>&lt;script&gt;</c> DOM-patch blocks and interactive components emit
    /// inline <c>style</c> attributes; a strict policy without it would break the
    /// running console. <c>connect-src 'self'</c> permits the same-origin SignalR
    /// WebSocket that carries the interactive circuit.
    /// </summary>
    internal const string ContentSecurityPolicyValue =
        "default-src 'self'; " +
        "script-src 'self' 'unsafe-inline'; " +
        "style-src 'self' 'unsafe-inline'; " +
        "img-src 'self' data:; " +
        "font-src 'self' data:; " +
        "connect-src 'self'; " +
        "frame-ancestors 'none'; " +
        "base-uri 'self'; " +
        "form-action 'self'";

    /// <summary>
    /// The <c>X-Frame-Options</c> value: <c>DENY</c>. Kept alongside the CSP
    /// <c>frame-ancestors</c> directive to also deny framing on older browsers
    /// that predate CSP frame-ancestors support.
    /// </summary>
    internal const string FrameOptionsValue = "DENY";

    /// <summary>
    /// The <c>X-Content-Type-Options</c> value: <c>nosniff</c>. Stops the browser
    /// from MIME-sniffing a response away from its declared content type.
    /// </summary>
    internal const string ContentTypeOptionsValue = "nosniff";

    /// <summary>The cached <c>Content-Security-Policy</c> header value.</summary>
    internal static readonly StringValues ContentSecurityPolicy = ContentSecurityPolicyValue;

    /// <summary>The cached <c>X-Frame-Options</c> header value.</summary>
    internal static readonly StringValues FrameOptions = FrameOptionsValue;

    /// <summary>The cached <c>X-Content-Type-Options</c> header value.</summary>
    internal static readonly StringValues ContentTypeOptions = ContentTypeOptionsValue;
}
