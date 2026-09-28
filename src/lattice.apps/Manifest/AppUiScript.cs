namespace Orleans.Lattice.Apps;

/// <summary>
/// A script the frame bootstrap loads, in declaration order. Each script must be self-contained:
/// a module may import only from absolute <c>blob:</c> URLs obtained at runtime through the in-frame
/// API, never by a relative specifier, because bundle files are never served over HTTP. Authors
/// bundle with any tool they like.
/// </summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppUiScript), Immutable]
public sealed record AppUiScript
{
    /// <summary>Bundle path of a <c>text/javascript</c> asset.</summary>
    [Id(0)] public required string Path { get; init; }

    /// <summary>Whether the script loads as an ECMAScript module rather than a classic script.</summary>
    [Id(1)] public bool Module { get; init; }
}
