namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The structural classification of a <see cref="SymbolRecord"/>. Captured once
/// at ingest from the source language's declaration and treated as immutable
/// record identity: two replicas that ingest the same fully-qualified name agree
/// on its kind, so the field is not a mutable CRDT scalar.
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.SymbolKind)]
internal enum SymbolKind
{
    /// <summary>Kind not specified (the default for a never-classified record).</summary>
    Unspecified = 0,

    /// <summary>A namespace or module container.</summary>
    Namespace,

    /// <summary>A class, struct, or record type.</summary>
    Type,

    /// <summary>An interface type.</summary>
    Interface,

    /// <summary>An enumeration type.</summary>
    Enum,

    /// <summary>A method declared on a type.</summary>
    Method,

    /// <summary>A property declared on a type.</summary>
    Property,

    /// <summary>A field declared on a type.</summary>
    Field,

    /// <summary>A free (non-member) function.</summary>
    Function,

    /// <summary>Any other symbol shape not covered by the more specific kinds.</summary>
    Other,
}
