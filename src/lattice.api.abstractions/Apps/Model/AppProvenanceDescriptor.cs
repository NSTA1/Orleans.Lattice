namespace Orleans.Lattice.Api.Apps;

/// <summary>Inspectable source metadata, never proof of trust or authorization.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppProvenanceDescriptor), Immutable]
public sealed record AppProvenanceDescriptor
{
    /// <summary>The source provider identifier, such as in-image.</summary>
    [Id(0)] public required string Source { get; init; }
    /// <summary>The declared publisher.</summary>
    [Id(1)] public required string Publisher { get; init; }
    /// <summary>An optional source reference; it must not contain credentials.</summary>
    [Id(2)] public string? Reference { get; init; }
}
