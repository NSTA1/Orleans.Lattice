namespace Orleans.Lattice.Api.Apps;

/// <summary>An inspectable manifest MCP tool declaration, not executable app code.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppMcpToolDescriptor), Immutable]
public sealed record AppMcpToolDescriptor
{
    /// <summary>The app-local tool name; bindings derive the wire name from the app slug.</summary>
    [Id(0)] public required string Name { get; init; }
    /// <summary>The tool's declared description.</summary>
    [Id(1)] public required string Description { get; init; }
    /// <summary>The declared app role required by the tool.</summary>
    [Id(2)] public required string Role { get; init; }
}
