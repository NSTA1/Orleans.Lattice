using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Builds an <see cref="McpServerTool"/> over a facade handler delegate with the
/// options every handler-bound tool group shares: the request
/// <see cref="IServiceProvider"/>, the shared
/// <see cref="LatticeApiMcpToolSerialization.Options"/>, and structured content.
/// The three entry points differ only in the read-only / destructive hints the
/// tool advertises, so the hint pair is chosen by name rather than by a pair of
/// booleans at every call site.
/// </summary>
internal static class McpHandlerToolFactory
{
    /// <summary>Builds a tool that only reads state (read-only, non-destructive).</summary>
    /// <param name="services">The service provider handler parameters are bound from.</param>
    /// <param name="handler">The handler delegate the tool invokes.</param>
    /// <param name="name">The tool name.</param>
    /// <param name="title">The human-readable tool title.</param>
    /// <param name="description">The tool description.</param>
    /// <returns>The tool.</returns>
    public static McpServerTool ReadOnlyTool(
        IServiceProvider services, Delegate handler, string name, string title, string description)
        => Create(services, handler, name, title, description, readOnly: true, destructive: false);

    /// <summary>Builds a tool that may overwrite or remove state (mutating, destructive).</summary>
    /// <param name="services">The service provider handler parameters are bound from.</param>
    /// <param name="handler">The handler delegate the tool invokes.</param>
    /// <param name="name">The tool name.</param>
    /// <param name="title">The human-readable tool title.</param>
    /// <param name="description">The tool description.</param>
    /// <returns>The tool.</returns>
    public static McpServerTool DestructiveTool(
        IServiceProvider services, Delegate handler, string name, string title, string description)
        => Create(services, handler, name, title, description, readOnly: false, destructive: true);

    /// <summary>Builds a tool that changes state additively (mutating, non-destructive).</summary>
    /// <param name="services">The service provider handler parameters are bound from.</param>
    /// <param name="handler">The handler delegate the tool invokes.</param>
    /// <param name="name">The tool name.</param>
    /// <param name="title">The human-readable tool title.</param>
    /// <param name="description">The tool description.</param>
    /// <returns>The tool.</returns>
    public static McpServerTool NonDestructiveMutatingTool(
        IServiceProvider services, Delegate handler, string name, string title, string description)
        => Create(services, handler, name, title, description, readOnly: false, destructive: false);

    private static McpServerTool Create(
        IServiceProvider services,
        Delegate handler,
        string name,
        string title,
        string description,
        bool readOnly,
        bool destructive)
        => McpServerTool.Create(
            handler,
            new McpServerToolCreateOptions
            {
                Services = services,
                Name = name,
                Title = title,
                Description = description,
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                ReadOnly = readOnly,
                Destructive = destructive,
                UseStructuredContent = true,
            });
}
