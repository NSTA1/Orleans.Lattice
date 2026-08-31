namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// A minimal activation for <see cref="ITestStringKeyedGrain"/>, so a query
/// integration test can resolve a matched grain to a real, addressable
/// reference. It carries no behaviour: the point is only that the interface has
/// an implementation the cluster can resolve.
/// </summary>
public sealed class TestStringKeyedGrain : Grain, ITestStringKeyedGrain;
