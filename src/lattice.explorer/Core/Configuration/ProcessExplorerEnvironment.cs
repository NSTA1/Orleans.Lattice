namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// The default <see cref="IExplorerEnvironment"/>, backed by the live process
/// environment via <see cref="Environment.GetEnvironmentVariable(string)"/>.
/// </summary>
public sealed class ProcessExplorerEnvironment : IExplorerEnvironment
{
    /// <inheritdoc />
    public string? GetVariable(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        return Environment.GetEnvironmentVariable(name);
    }
}
