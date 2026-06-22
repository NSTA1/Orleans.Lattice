namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Options for <see cref="EnvVarCredentialAuthorizer"/>: the environment-variable
/// prefix the credential dictionary lives under, and the failed-attempt
/// backoff / lockout policy that guards against brute-force guessing of the
/// (low-entropy, human-chosen) username/password credentials.
/// </summary>
public sealed class EnvVarCredentialAuthorizerOptions
{
    /// <summary>
    /// The prefix prepended to a username to form the environment-variable name
    /// that holds that user's encoded password hash. Defaults to
    /// <c>LATTICE_STATE_USER_</c>, so user <c>alice</c> is read from
    /// <c>LATTICE_STATE_USER_alice</c>.
    /// </summary>
    public string EnvironmentVariablePrefix { get; set; } = "LATTICE_STATE_USER_";

    /// <summary>
    /// The number of consecutive failed authentication attempts for a single
    /// username that triggers a temporary lockout. Defaults to 5.
    /// </summary>
    public int MaxFailedAttempts { get; set; } = 5;

    /// <summary>
    /// How long a username stays locked out after
    /// <see cref="MaxFailedAttempts"/> consecutive failures, during which every
    /// attempt (even with the correct password) is denied. Defaults to 1 minute.
    /// </summary>
    public TimeSpan LockoutDuration { get; set; } = TimeSpan.FromMinutes(1);
}
