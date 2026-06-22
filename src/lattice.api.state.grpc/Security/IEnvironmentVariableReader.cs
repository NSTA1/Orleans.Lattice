namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Reads environment variables for the <see cref="EnvVarCredentialAuthorizer"/>.
/// Abstracted so the authorizer can be unit-tested against an in-memory
/// credential dictionary without mutating real process environment state.
/// </summary>
public interface IEnvironmentVariableReader
{
    /// <summary>
    /// Returns the value of the environment variable named <paramref name="name"/>,
    /// or <see langword="null"/> when it is not set.
    /// </summary>
    /// <param name="name">The environment variable name.</param>
    /// <returns>The value, or <see langword="null"/> when unset.</returns>
    string? GetVariable(string name);
}

/// <summary>
/// The default <see cref="IEnvironmentVariableReader"/>, backed by
/// <see cref="Environment.GetEnvironmentVariable(string)"/> against the current
/// process environment.
/// </summary>
public sealed class ProcessEnvironmentVariableReader : IEnvironmentVariableReader
{
    /// <inheritdoc />
    public string? GetVariable(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        return Environment.GetEnvironmentVariable(name);
    }
}
