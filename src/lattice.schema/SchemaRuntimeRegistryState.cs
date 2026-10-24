namespace Orleans.Lattice.Schema;

[GenerateSerializer]
[Alias(SchemaTypeAliases.SchemaRuntimeRegistryState)]
internal sealed class SchemaRuntimeRegistryState
{
    [Id(0)] public LatticeSchemaRuntimeRegistration Registration { get; set; } = new();
}
