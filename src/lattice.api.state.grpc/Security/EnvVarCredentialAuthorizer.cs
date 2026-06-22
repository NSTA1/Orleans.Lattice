using System.Collections.Concurrent;
using System.Text;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// A turnkey reference <see cref="ILatticeStateApiAuthorizer"/> that validates the
/// inbound <c>authorization: Basic base64(user:pass)</c> header against an
/// environment-variable-backed credential dictionary of salted PBKDF2-SHA256
/// password hashes (never plaintext), replacing the default
/// <see cref="DenyAllStateApiAuthorizer"/>.
/// </summary>
/// <remarks>
/// <para>
/// Each credential lives in an environment variable named
/// <c>&lt;prefix&gt;&lt;username&gt;</c> (the prefix defaults to
/// <c>LATTICE_STATE_USER_</c>) whose value is an encoded
/// <c>pbkdf2-sha256$&lt;iterations&gt;$&lt;salt&gt;$&lt;key&gt;</c> hash produced by
/// the credential-generation helper scripts under <c>tools/</c>. The authorizer
/// re-derives the presented password with the embedded salt and iteration count
/// and compares in constant time via <see cref="LatticePasswordHash.Verify"/>.
/// </para>
/// <para>
/// Because username/password credentials are low-entropy and guessable, the
/// authorizer applies a per-username failed-attempt lockout
/// (<see cref="EnvVarCredentialAuthorizerOptions.MaxFailedAttempts"/> /
/// <see cref="EnvVarCredentialAuthorizerOptions.LockoutDuration"/>) to blunt
/// brute-force attempts. Authorization is flat: the call's
/// <see cref="LatticeStateApiOperation"/> and target tree are not consulted (the
/// seam keeps them available for future role / per-tree scoping).
/// </para>
/// </remarks>
public sealed class EnvVarCredentialAuthorizer : ILatticeStateApiAuthorizer
{
    private const string AuthorizationHeaderName = "authorization";
    private const string BasicScheme = "Basic ";

    // A well-formed dummy hash used to equalise the verification cost for an
    // unknown username, so a caller cannot distinguish "no such user" from
    // "wrong password" by timing the response.
    private static readonly string DummyHash =
        LatticePasswordHash.Hash("not-a-real-password-placeholder", LatticePasswordHash.DefaultIterations);

    private readonly IEnvironmentVariableReader _environment;
    private readonly IOptionsMonitor<EnvVarCredentialAuthorizerOptions> _options;
    private readonly ILogger<EnvVarCredentialAuthorizer> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, AttemptRecord> _attempts = new(StringComparer.Ordinal);

    /// <summary>Initialises the authorizer.</summary>
    /// <param name="environment">The environment-variable source.</param>
    /// <param name="options">The lockout and prefix options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="timeProvider">The time source driving the lockout window.</param>
    public EnvVarCredentialAuthorizer(
        IEnvironmentVariableReader environment,
        IOptionsMonitor<EnvVarCredentialAuthorizerOptions> options,
        ILogger<EnvVarCredentialAuthorizer> logger,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _environment = environment;
        _options = options;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeStateApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
    {
        var header = authorizationContext.Call.RequestHeaders.GetValue(AuthorizationHeaderName);
        return Task.FromResult(Authorize(header));
    }

    /// <summary>
    /// Validates an <c>authorization</c> header value (for example
    /// <c>Basic dXNlcjpwYXNz</c>) against the credential dictionary. Exposed for
    /// unit testing without constructing a gRPC <see cref="ServerCallContext"/>.
    /// </summary>
    /// <param name="authorizationHeader">The raw header value, or <see langword="null"/> when absent.</param>
    /// <returns><see langword="true"/> when the credential is valid and the username is not locked out.</returns>
    internal bool Authorize(string? authorizationHeader)
    {
        if (!TryDecodeBasic(authorizationHeader, out var username, out var password))
        {
            _logger.LogWarning("Api.State: rejected call - missing or malformed Basic authorization header.");
            return false;
        }

        if (!IsValidUsername(username))
        {
            _logger.LogWarning("Api.State: rejected call - username is not a valid environment-variable name.");
            return false;
        }

        var options = _options.CurrentValue;
        var record = _attempts.GetOrAdd(username, static _ => new AttemptRecord());

        lock (record.Gate)
        {
            var now = _timeProvider.GetUtcNow();
            if (record.LockedUntil is { } lockedUntil && now < lockedUntil)
            {
                _logger.LogWarning(
                    "Api.State: rejected call - user is temporarily locked out after repeated failures.");
                return false;
            }

            var encodedHash = _environment.GetVariable(options.EnvironmentVariablePrefix + username);

            bool verified;
            if (encodedHash is null)
            {
                // Unknown user: spend the same verification cost against a dummy
                // hash so response timing does not reveal whether the user exists.
                _ = LatticePasswordHash.Verify(password, DummyHash);
                verified = false;
            }
            else
            {
                verified = LatticePasswordHash.Verify(password, encodedHash);
            }

            if (verified)
            {
                record.Failures = 0;
                record.LockedUntil = null;
                return true;
            }

            record.Failures++;
            if (record.Failures >= options.MaxFailedAttempts)
            {
                record.LockedUntil = now + options.LockoutDuration;
                record.Failures = 0;
                _logger.LogWarning(
                    "Api.State: user locked out for {LockoutSeconds}s after {MaxFailedAttempts} consecutive failures.",
                    options.LockoutDuration.TotalSeconds,
                    options.MaxFailedAttempts);
            }

            return false;
        }
    }

    private static bool TryDecodeBasic(string? header, out string username, out string password)
    {
        username = string.Empty;
        password = string.Empty;

        if (string.IsNullOrWhiteSpace(header) ||
            !header.StartsWith(BasicScheme, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var encoded = header[BasicScheme.Length..].Trim();
        string decoded;
        try
        {
            decoded = Encoding.UTF8.GetString(Convert.FromBase64String(encoded));
        }
        catch (FormatException)
        {
            return false;
        }

        var separator = decoded.IndexOf(':', StringComparison.Ordinal);
        if (separator < 0)
        {
            return false;
        }

        username = decoded[..separator];
        password = decoded[(separator + 1)..];
        return username.Length > 0;
    }

    private static bool IsValidUsername(string username)
    {
        if (username.Length == 0)
        {
            return false;
        }

        var first = username[0];
        if (!char.IsAsciiLetter(first) && first != '_')
        {
            return false;
        }

        foreach (var c in username)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c != '_')
            {
                return false;
            }
        }

        return true;
    }

    private sealed class AttemptRecord
    {
        public object Gate { get; } = new();

        public int Failures { get; set; }

        public DateTimeOffset? LockedUntil { get; set; }
    }
}
