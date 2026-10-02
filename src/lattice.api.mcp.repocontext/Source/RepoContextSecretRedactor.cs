using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Scrubs secrets out of any text that is about to be logged or returned as a
/// failure reason. A transport exception can quote the URL it was given, and an
/// operator can legitimately configure a remote whose URL embeds userinfo, so every
/// message that leaves the git source passes through here first.
/// </summary>
internal static class RepoContextSecretRedactor
{
    /// <summary>The text substituted for any redacted span.</summary>
    internal const string Placeholder = "***";

    /// <summary>
    /// Removes <paramref name="credential"/>'s secret from <paramref name="text"/>
    /// and strips userinfo from any URL-like span it contains.
    /// </summary>
    /// <param name="text">The text to scrub. May be <see langword="null"/>.</param>
    /// <param name="credential">The credential whose secret must not survive, or
    /// <see langword="null"/> when none was resolved.</param>
    /// <returns>The scrubbed text; empty when <paramref name="text"/> was
    /// <see langword="null"/> or blank.</returns>
    internal static string Redact(string? text, RepoContextGitCredential? credential)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return string.Empty;
        }

        var scrubbed = text;
        if (credential is { IsAnonymous: false, Secret.Length: > 0 })
        {
            scrubbed = scrubbed.Replace(credential.Secret, Placeholder, StringComparison.Ordinal);
        }

        return RedactUrls(scrubbed);
    }

    /// <summary>
    /// Replaces the userinfo component of every URL-like span in
    /// <paramref name="text"/> - everything between the scheme separator and the
    /// last at-sign of the authority - with the placeholder, so a credential
    /// embedded in a configured remote URL never reaches a log.
    /// </summary>
    /// <param name="text">The text to scrub. Must not be <see langword="null"/>.</param>
    /// <returns>The text with every URL userinfo component redacted.</returns>
    internal static string RedactUrls(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        var schemeIndex = text.IndexOf("://", StringComparison.Ordinal);
        if (schemeIndex < 0)
        {
            return text;
        }

        var builder = new StringBuilder(text.Length);
        var cursor = 0;
        while (schemeIndex >= 0)
        {
            var authorityStart = schemeIndex + 3;

            // Locate the userinfo/host boundary '@' by scanning only up to a
            // character that cannot appear anywhere in the authority. The RFC 3986
            // sub-delims - ',', ';', '(', ')' and '\'' among them - are legal in
            // userinfo, so they must NOT bound this scan: stopping at one that sits
            // inside the userinfo (a password such as "p,ss") would hide the '@' and
            // leave the whole credential unredacted. The gen-delims '?' and '#' are
            // excluded on the same grounds; see IsUserinfoTerminator.
            var userinfoEnd = authorityStart;
            while (userinfoEnd < text.Length && !IsUserinfoTerminator(text[userinfoEnd]))
            {
                userinfoEnd++;
            }

            var at = text.AsSpan(authorityStart, userinfoEnd - authorityStart).LastIndexOf('@');
            var hostStart = at >= 0 ? authorityStart + at + 1 : authorityStart;

            // The host runs from just past the userinfo to the first character that
            // ends the authority for logging purposes - a path separator, whitespace,
            // a quote, or a sub-delim that bounds a URL embedded in prose - so a
            // trailing token or a following URL is never swallowed into the host.
            var hostEnd = hostStart;
            while (hostEnd < text.Length && !IsAuthorityTerminator(text[hostEnd]))
            {
                hostEnd++;
            }

            builder.Append(text, cursor, authorityStart - cursor);
            if (at >= 0)
            {
                builder.Append(Placeholder).Append('@');
            }

            builder.Append(text, hostStart, hostEnd - hostStart);
            cursor = hostEnd;
            schemeIndex = text.IndexOf("://", cursor, StringComparison.Ordinal);
        }

        builder.Append(text, cursor, text.Length - cursor);
        return builder.ToString();
    }

    private static bool IsAuthorityTerminator(char c) =>
        c is '/' or '\\' or ' ' or '\t' or '\r' or '\n' or '"' or '\'' or ')' or ',' or ';';

    // The characters that cannot appear anywhere in a URL authority and therefore
    // definitively end the userinfo scan: a path separator, whitespace, or a double
    // quote.
    //
    // Sub-delims (',', ';', '(', ')', '\'' and the rest) are deliberately absent
    // because they are legal in a userinfo - treating one as a boundary would
    // truncate the scan before the '@' and leak an embedded credential.
    //
    // '?' and '#' are absent for exactly the same reason, and their presence here
    // was a credential leak. RFC 3986 does forbid them in a conforming userinfo, so
    // treating them as terminators looked sound - but this redactor is handed
    // whatever a transport exception quoted, and git accepts an authority whose
    // password carries a raw gen-delim because it splits on the LAST '@' before the
    // path. For "https://alice:s3c?ret@host/r.git" the scan stopped at the '?',
    // found no '@' in the "alice:s3c" prefix it had seen, concluded there was no
    // userinfo at all, and copied the authority through verbatim - emitting the
    // whole credential into the log it was supposed to scrub. Bounding the scan at
    // the authority instead finds the last '@' and redacts everything before it.
    //
    // The residual cost is over-redaction, which is the safe direction for a
    // redactor: a pathless URL whose query carries an '@' ("https://host?x=a@b")
    // now has its prefix replaced. Under-redaction leaks a secret permanently into
    // a log; over-redaction costs a less precise diagnostic message.
    private static bool IsUserinfoTerminator(char c) =>
        c is '/' or '\\' or ' ' or '\t' or '\r' or '\n' or '"';
}
