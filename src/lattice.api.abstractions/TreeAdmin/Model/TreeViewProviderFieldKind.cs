namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>The JSON value type of a <see cref="TreeViewProviderField"/>.</summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeViewProviderFieldKind)]
public enum TreeViewProviderFieldKind
{
    /// <summary>A JSON string.</summary>
    Text = 0,

    /// <summary>A JSON number.</summary>
    Number = 1,

    /// <summary>A JSON <c>true</c> or <c>false</c>.</summary>
    Boolean = 2,
}
