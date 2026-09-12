namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Detects a URL carrying a password-bearing userinfo component in a memory
/// <c>body</c>, so the write seam can refuse it rather than store a value that can
/// never be written again.
/// </summary>
/// <remarks>
/// <para>
/// A redaction upstream of this store rewrites this shape in transit. The span it
/// replaces runs from the scheme through the at-sign, and it extends
/// <em>backwards</em> past the scheme through any preceding non-delimiter
/// characters, so it can consume a structural character that belonged to the
/// serialised record rather than to the URL. The stored value is then malformed.
/// The damage is silent and permanent in the worst way available: the entry still
/// reads back, so nothing looks wrong, while every later read-modify-write against
/// it fails forever.
/// </para>
/// <para>
/// <strong>The trigger is the shape, not the secret.</strong> This is the single
/// most important property of this guard and the one that makes the obvious design
/// wrong. The rewrite never asks whether the value is a credential. A shell-variable
/// placeholder is rewritten exactly as a literal token is, so a guard that tried to
/// tell a real credential from a placeholder - by length, by character mix, by
/// entropy - would permit the precise bodies that corrupt. What is spared is
/// spared for a structural reason only: a placeholder using a character that cannot
/// appear in a userinfo component (an angle bracket, say) is not a userinfo at all,
/// so no URL of this shape is present to rewrite.
/// </para>
/// <para>
/// That is why this repository's own documented push convention is caught. The
/// convention embeds a shell variable in a remote URL for a single push; quoting it
/// in a memory entry reproduces the shape, placeholder and all. An agent following
/// the convention correctly and then capturing what it learned would mint a frozen
/// record as a matter of course, which is exactly how the entry written to warn
/// about this trap came to be frozen by it.
/// </para>
/// <para>
/// <strong>Precision.</strong> The predicate requires a scheme separator, a
/// userinfo containing a colon, and an at-sign that ends it, so ordinary content is
/// untouched: a plain URL, a <c>host:port</c> authority, a bare
/// <c>user@host</c> with no password segment, and an scp-style git remote (which
/// has no scheme separator) all pass. Requiring the colon is not a nicety - a
/// userinfo without one is not rewritten, so refusing it would block bodies that
/// were never at risk.
/// </para>
/// <para>
/// <strong>This type names the offending shape and never reproduces it.</strong> It
/// recognises the shape structurally, by walking characters, and contains no URL of
/// the form it detects. The same constraint binds its tests, which assemble their
/// fixture data from parts at runtime. A detector or a fixture spelling out the
/// sequence it detects would be an instance of the defect, corrupting any entry
/// that quoted it - the same trap <see cref="RepoContextBodyFraming"/> closes for
/// tool-call framing, and for the same reason.
/// </para>
/// <para>
/// This guard deliberately shares no code with
/// <see cref="RepoContextSecretRedactor"/>. That type answers a different question
/// - how to rewrite text that is about to be logged - and was exonerated as a cause
/// of this corruption by simulation. Reusing its internals here would re-entangle a
/// component that was deliberately cleared.
/// </para>
/// </remarks>
internal static class RepoContextBodyCredentials
{
    /// <summary>The scheme separator that introduces a URL authority.</summary>
    private const string SchemeSeparator = "://";

    /// <summary>
    /// Inspects <paramref name="body"/> for a URL carrying a password-bearing
    /// userinfo component.
    /// </summary>
    /// <param name="body">The candidate body. May be <see langword="null"/>.</param>
    /// <returns>
    /// The inspection outcome. A <see langword="null"/>, empty, or clean body is
    /// reported as carrying no such URL.
    /// </returns>
    internal static RepoContextBodyCredentialInspection Inspect(string? body)
    {
        if (string.IsNullOrEmpty(body))
        {
            return new RepoContextBodyCredentialInspection(false, -1, 0);
        }

        var schemeIndex = body.IndexOf(SchemeSeparator, StringComparison.Ordinal);
        while (schemeIndex >= 0)
        {
            var authorityStart = schemeIndex + SchemeSeparator.Length;

            // The userinfo runs to the first character that cannot appear in one.
            // Sub-delims are deliberately absent from that set: they are legal in a
            // userinfo, so stopping at one would hide the at-sign behind it and let
            // the offending shape through.
            var scanEnd = authorityStart;
            while (scanEnd < body.Length && !IsUserinfoTerminator(body[scanEnd]))
            {
                scanEnd++;
            }

            var at = body.AsSpan(authorityStart, scanEnd - authorityStart).LastIndexOf('@');
            if (at >= 0)
            {
                var userinfo = body.AsSpan(authorityStart, at);

                // No colon means no password segment, and a userinfo without one is
                // not rewritten, so it is not at risk and must not be refused.
                if (userinfo.Contains(':'))
                {
                    return new RepoContextBodyCredentialInspection(true, schemeIndex, userinfo.Length);
                }
            }

            schemeIndex = body.IndexOf(SchemeSeparator, authorityStart, StringComparison.Ordinal);
        }

        return new RepoContextBodyCredentialInspection(false, -1, 0);
    }

    /// <summary>
    /// Builds the rejection message for a body carrying such a URL, naming where it
    /// sits and how long its userinfo is, and never quoting any of it.
    /// </summary>
    /// <remarks>
    /// This message is deliberately long, and must stay that way. It is the only
    /// channel that survives the rewrite: it is what a caller sees at the moment the
    /// value is refused, before anything is stored. A message of the form "see the
    /// documentation" would fail exactly when it is needed, because the document
    /// describing the trap necessarily writes the shape out to describe it, and the
    /// rewrite treats that occurrence identically - a reader following the pointer
    /// arrives at a corrupted explanation of corruption. So the message carries the
    /// whole lesson rather than a reference to it, in the order a reader needs it:
    /// what was rejected and on what grounds, why a placeholder is not a safe form,
    /// the backwards-reaching span that is the actual mechanism, and where the
    /// symptom will appear.
    /// </remarks>
    /// <param name="location">
    /// How the offending body was supplied, so the caller can find it: the
    /// <c>'body'</c> argument of <c>remember</c>, or the <c>'body'</c> entry of the
    /// <c>'fields'</c> map of <c>update</c>.
    /// </param>
    /// <param name="inspection">The inspection that refused the body.</param>
    /// <returns>A self-contained, actionable message carrying no body content.</returns>
    internal static string DescribeRejection(string location, RepoContextBodyCredentialInspection inspection) =>
        location
        + " was refused: it contains a URL whose authority carries a userinfo component with a "
        + "password segment (at offset "
        + inspection.Offset.ToString(System.Globalization.CultureInfo.InvariantCulture)
        + ", userinfo length "
        + inspection.UserinfoLength.ToString(System.Globalization.CultureInfo.InvariantCulture)
        + "). It is refused for its SHAPE. No claim is made that it holds a real credential, and "
        + "none is needed, because the rewrite described below matches the shape and never inspects "
        + "the value. "
        + "A placeholder is NOT a safe form. A shell-variable placeholder is NOT sufficient: it is "
        + "rewritten exactly as a literal token is. An angle-bracketed name survives only because "
        + "angle brackets are illegal in an RFC 3986 userinfo component, so that text is not a "
        + "userinfo at all - it is spared for a structural reason, not because anything recognises "
        + "it as a placeholder. "
        + "Storing this body would corrupt the entry permanently. A redaction upstream of this store "
        + "rewrites that shape in transit, and the span it replaces reaches BACKWARDS past the "
        + "scheme through the characters preceding it, so it can swallow an adjacent structural "
        + "character - a quote, a backslash, a line ending - and leave the stored value undecodable. "
        + "The entry still reads back, so nothing looks wrong, while every later update to it fails, "
        + "forever. "
        + "Expect the visible symptom somewhere OTHER than the URL: a broken code fence, a file with "
        + "mixed line endings, a stray backslash. Suspect the string first. It is minutes to find if "
        + "you do, and an hour in the wrong file if you do not. "
        + "Re-issue the call without that shape: describe the URL instead of writing one, or use an "
        + "angle-bracketed placeholder.";

    /// <summary>
    /// Reports whether <paramref name="c"/> cannot appear unencoded in a URL
    /// userinfo component (RFC 3986 section 3.2.1) and therefore ends it: a path,
    /// query, or fragment separator, whitespace, a quote, or an angle bracket.
    /// Sub-delims are deliberately absent - they are legal in a userinfo, and
    /// treating one as a boundary would truncate the scan before the at-sign and let
    /// the offending shape through.
    /// </summary>
    private static bool IsUserinfoTerminator(char c) =>
        c is '/' or '\\' or '?' or '#' or ' ' or '\t' or '\r' or '\n' or '"' or '<' or '>';
}
