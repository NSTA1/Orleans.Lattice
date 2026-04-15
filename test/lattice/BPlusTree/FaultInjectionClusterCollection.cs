namespace Orleans.Lattice.Tests.BPlusTree;

[CollectionDefinition(Name)]
public class FaultInjectionClusterCollection : ICollectionFixture<FaultInjectionClusterFixture>
{
    public const string Name = "FaultInjectionClusterCollection";
}
