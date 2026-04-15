namespace Orleans.Lattice.Tests.BPlusTree;

[CollectionDefinition(Name)]
public class FourShardClusterCollection : ICollectionFixture<FourShardClusterFixture>
{
    public const string Name = "FourShardClusterCollection";
}
