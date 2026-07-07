using System.Text;
using Grpc.Core;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Samples.PasswordProtection;

/// <summary>
/// A custom <see cref="ILatticeStateApiCredentialBridge"/> that lifts the
/// <b>username</b> out of an inbound <c>authorization: Basic base64(user:pass)</c>
/// header onto the ambient <see cref="LatticeCredential"/> so the data-plane
/// authorization gate can resolve the caller's subject. The password itself is
/// never carried past the transport: it has already been verified against the
/// salted PBKDF2 credential dictionary by <c>EnvVarCredentialAuthorizer</c>
/// (the transport authorizer) before this bridge runs.
/// </summary>
/// <remarks>
/// The default <see cref="ILatticeStateApiCredentialBridge"/> is bearer-oriented
/// (it strips a <c>Bearer</c> scheme prefix and forwards the remaining token
/// verbatim); this Basic-aware bridge decodes the header and forwards only the
/// username, stamped with <see cref="PasswordAuthenticator.Scheme"/>. An absent,
/// malformed, or empty header yields <see langword="null"/> (an anonymous
/// caller), which auth-backed visibility fails closed.
/// </remarks>
internal sealed class PasswordCredentialBridge : ILatticeStateApiCredentialBridge
{
    private const string BasicScheme = "Basic ";

    /// <inheritdoc />
    public LatticeCredential? Resolve(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var raw = context.RequestHeaders?.GetValue("authorization");
        if (string.IsNullOrWhiteSpace(raw) ||
            !raw.StartsWith(BasicScheme, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        string decoded;
        try
        {
            decoded = Encoding.UTF8.GetString(Convert.FromBase64String(raw[BasicScheme.Length..].Trim()));
        }
        catch (FormatException)
        {
            return null;
        }

        var separator = decoded.IndexOf(':', StringComparison.Ordinal);
        var username = separator < 0 ? decoded : decoded[..separator];
        return string.IsNullOrEmpty(username)
            ? null
            : new LatticeCredential(username, PasswordAuthenticator.Scheme);
    }
}
