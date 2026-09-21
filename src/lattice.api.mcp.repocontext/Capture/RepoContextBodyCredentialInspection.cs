namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The outcome of inspecting a memory <c>body</c> for a URL carrying a
/// password-bearing userinfo component, as produced by
/// <see cref="RepoContextBodyCredentials.Inspect"/>.
/// </summary>
/// <param name="CarriesCredentialUrl">
/// <see langword="true"/> when the body contains a URL whose authority carries a
/// userinfo component with a password segment, which is the shape an upstream
/// redaction rewrites into a value the memory store can no longer decode.
/// </param>
/// <param name="Offset">
/// The index of the offending scheme separator within the body, or <c>-1</c> when
/// the body is clean. Reported so a caller can locate the URL in a long body
/// without this type having to quote any of it.
/// </param>
/// <param name="UserinfoLength">
/// The length of the offending userinfo component, or <c>0</c> when the body is
/// clean. Reported as a length rather than as text for the same reason: it is
/// enough to identify which URL is meant, and carries none of its content.
/// </param>
internal readonly record struct RepoContextBodyCredentialInspection(
    bool CarriesCredentialUrl,
    int Offset,
    int UserinfoLength);
