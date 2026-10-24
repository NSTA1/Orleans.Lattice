namespace Orleans.Lattice.Schema;

[Alias(SchemaTypeAliases.ILatticeSchemaRuntimeRegistryGrain)]
internal interface ILatticeSchemaRuntimeRegistryGrain : IGrainWithStringKey
{
    Task<LatticeSchemaRuntimeRegistration> GetAsync();
    Task RegisterAsync(LatticeSchemaRuntimeRegistration registration);
}
