using System.Text;
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
    /// Builds the <c>Content-Security-Policy</c> header value, appending any
    /// <paramref name="additionalFormActionSources"/> to the <c>form-action</c>
    /// directive of the baseline <see cref="ContentSecurityPolicyValue"/>. With no
    /// (or only malformed) extra sources the baseline value is returned unchanged.
    /// <para>
    /// The baseline value ends with the <c>form-action</c> directive
    /// (<c>form-action 'self'</c>), so each source is appended, space-separated,
    /// to exactly that directive's allow-list and no other directive is touched.
    /// This lets a federated sign-out provider permit its identity provider's
    /// end-session origin (the cross-origin redirect target of the sign-out POST,
    /// which browsers check against <c>form-action</c>) without loosening the
    /// policy anywhere else. A source that is blank or contains whitespace, a
    /// <c>;</c>, or a <c>,</c> is dropped rather than emitted, so a malformed
    /// contribution cannot inject an unrelated directive or policy (fail closed).
    /// </para>
    /// </summary>
    /// <param name="additionalFormActionSources">
    /// Extra CSP source expressions for the <c>form-action</c> directive, or
    /// <see langword="null"/> / empty for the baseline policy.
    /// </param>
    /// <returns>The composed Content-Security-Policy header value.</returns>
    internal static string BuildContentSecurityPolicy(IEnumerable<string>? additionalFormActionSources)
    {
        if (additionalFormActionSources is null)
        {
            return ContentSecurityPolicyValue;
        }

        StringBuilder? builder = null;
        foreach (var source in additionalFormActionSources)
        {
            if (!IsValidFormActionSource(source))
            {
                continue;
            }

            builder ??= new StringBuilder(ContentSecurityPolicyValue);
            builder.Append(' ').Append(source);
        }

        return builder?.ToString() ?? ContentSecurityPolicyValue;
    }

    /// <summary>
    /// A CSP source expression is a single token: it must be non-empty and carry
    /// no whitespace and no <c>;</c> or <c>,</c> (which would start a new directive
    /// or policy). Anything else is rejected so it cannot corrupt the header.
    /// </summary>
    private static bool IsValidFormActionSource(string? source)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            return false;
        }

        foreach (var c in source)
        {
            if (char.IsWhiteSpace(c) || c == ';' || c == ',')
            {
                return false;
            }
        }

        return true;
    }

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

    /// <summary>
    /// The <c>Referrer-Policy</c> value: <c>no-referrer</c>. The authenticated
    /// admin console must never leak a request URL (which can carry tree, key, or
    /// subject context in its path or query) to a foreign origin via the
    /// <c>Referer</c> header on an outbound navigation, so no referrer is sent at
    /// all.
    /// </summary>
    internal const string ReferrerPolicyValue = "no-referrer";

    /// <summary>
    /// The <c>Permissions-Policy</c> value. The admin console uses none of the
    /// powerful browser features named here, so each is disabled for every
    /// origin (empty allow-list). This narrows what an injected or compromised
    /// script - already constrained by the CSP - could request. <c>interest-cohort</c>
    /// additionally opts the console out of Topics/FLoC cohort computation.
    /// </summary>
    internal const string PermissionsPolicyValue =
        "camera=(), microphone=(), geolocation=(), interest-cohort=()";

    /// <summary>The cached <c>Content-Security-Policy</c> header value.</summary>
    internal static readonly StringValues ContentSecurityPolicy = ContentSecurityPolicyValue;

    /// <summary>The cached <c>X-Frame-Options</c> header value.</summary>
    internal static readonly StringValues FrameOptions = FrameOptionsValue;

    /// <summary>The cached <c>X-Content-Type-Options</c> header value.</summary>
    internal static readonly StringValues ContentTypeOptions = ContentTypeOptionsValue;

    /// <summary>The cached <c>Referrer-Policy</c> header value.</summary>
    internal static readonly StringValues ReferrerPolicy = ReferrerPolicyValue;

    /// <summary>The cached <c>Permissions-Policy</c> header value.</summary>
    internal static readonly StringValues PermissionsPolicy = PermissionsPolicyValue;
}
