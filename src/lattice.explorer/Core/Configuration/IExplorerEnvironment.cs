namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// A thin seam over process environment variables so the launcher-friendly
/// bootstrap (<see cref="EnvironmentExplorerBootstrap"/>) can be unit-tested
/// without mutating the real process environment.
/// </summary>
public interface IExplorerEnvironment
{
    /// <summary>
    /// Returns the value of the named environment variable, or
    /// <see langword="null"/> when it is unset.
    /// </summary>
    /// <param name="name">The environment-variable name.</param>
    string? GetVariable(string name);
}
