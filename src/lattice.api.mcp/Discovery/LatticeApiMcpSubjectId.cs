using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Resolves the subject identifier the MCP discovery surface <b>reports and
/// logs</b> for a caller, without ever surfacing the caller's credential
/// material.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists.</b> A <see cref="LatticeCredential"/> carries two very
/// different things: a <c>PrincipalId</c>, which is an identifier and is safe to
/// log and echo, and a <c>Token</c>, which is the bearer secret itself. Discovery
/// previously fell back to the token when the principal id was absent, and that
/// value then reached two places it must never reach: server log lines, and
/// <see cref="LatticeApiMcpCapabilities.SubjectId"/>, which the <b>ungated</b>
/// <c>lattice_capabilities</c> meta-tool returns to the client - so a caller's
/// bearer token was written into the server's logs and into the MCP client's
/// transcript, where both rest far longer than the request that carried them. A
/// credential is not an identifier and must not be used as one.
/// </para>
/// <para>
/// <b>What replaces it.</b> When no principal id is present the subject is
/// reported as a <see cref="FingerprintPrefix"/>-prefixed fingerprint: the
/// leading hex characters of the SHA-256 of the token. That keeps the property
/// doing its job - it is stable for a given caller, so two sessions on one token
/// still correlate across logs and payloads - while being one-way, so possessing
/// it does not let anyone authenticate. It is deliberately truncated: nothing
/// authenticates or authorizes on this value, it is diagnostic only, and a short
/// form stays readable in a log line.
/// </para>
/// <para>
/// <b>Scope.</b> This governs what is <i>reported</i>. It deliberately does not
/// change which value a permission lookup is keyed on - see
/// <c>AuthAdminMcpPermissionResolver</c>, which still keys its lookup exactly as
/// before so no host's resolution behaviour changes, and uses this helper only
/// for what it writes to the log.
/// </para>
/// <para>
/// <b>Allocation.</b> The common path returns the existing principal id string
/// with no allocation. The digest is computed only on the cold path where a
/// credential carries no principal id.
/// </para>
/// </remarks>
internal static class LatticeApiMcpSubjectId
{
    /// <summary>The prefix that marks a subject id as a token fingerprint rather than a principal id.</summary>
    internal const string FingerprintPrefix = "token:";

    /// <summary>How many hex characters of the SHA-256 digest the fingerprint keeps.</summary>
    internal const int FingerprintHexLength = 16;

    /// <summary>
    /// Returns the subject identifier to report for <paramref name="credential"/>:
    /// its principal id when it has one, otherwise a one-way fingerprint of its
    /// token, otherwise <see langword="null"/> when it carries neither.
    /// </summary>
    /// <param name="credential">The resolved caller credential.</param>
    /// <returns>A safe-to-log, safe-to-echo subject id, or <see langword="null"/>.</returns>
    public static string? Resolve(LatticeCredential credential)
    {
        if (!string.IsNullOrEmpty(credential.PrincipalId))
        {
            return credential.PrincipalId;
        }

        return string.IsNullOrEmpty(credential.Token) ? null : Fingerprint(credential.Token);
    }

    /// <summary>
    /// Computes the prefixed, truncated one-way fingerprint of a bearer token. The
    /// token itself is never returned and its plaintext copy is cleared before the
    /// buffer is released.
    /// </summary>
    /// <param name="token">The bearer token to fingerprint.</param>
    /// <returns>The prefixed hex digest, for example <c>token:9f86d081884c7d65</c>.</returns>
    private static string Fingerprint(string token)
    {
        var plaintext = Encoding.UTF8.GetBytes(token);
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        try
        {
            SHA256.HashData(plaintext, digest);
        }
        finally
        {
            Array.Clear(plaintext);
        }

        return string.Concat(
            FingerprintPrefix,
            Convert.ToHexStringLower(digest[..(FingerprintHexLength / 2)]));
    }
}
