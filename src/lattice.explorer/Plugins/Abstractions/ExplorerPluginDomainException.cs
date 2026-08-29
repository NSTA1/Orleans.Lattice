namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// Thrown when a plugin's controlled domain contract cannot be handed over:
/// the plugin declared none, asked for a type it did not declare, or its
/// declared contract is not registered in the container.
/// </summary>
public sealed class ExplorerPluginDomainException : InvalidOperationException
{
    /// <summary>Creates the exception with a default message.</summary>
    public ExplorerPluginDomainException()
        : base("The plugin's declared domain contract could not be resolved.")
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    /// <param name="message">The reason the domain contract could not be resolved.</param>
    public ExplorerPluginDomainException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/> and <paramref name="innerException"/>.</summary>
    /// <param name="message">The reason the domain contract could not be resolved.</param>
    /// <param name="innerException">The underlying failure.</param>
    public ExplorerPluginDomainException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
