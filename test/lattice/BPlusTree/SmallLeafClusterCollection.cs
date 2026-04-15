namespace Orleans.Lattice.Tests.BPlusTree;

[CollectionDefinition(SmallLeafClusterCollection.Name)]
public class SmallLeafClusterCollection : ICollectionFixture<SmallLeafClusterFixture>
{
    public const string Name = "SmallLeafClusterCollection";
}
