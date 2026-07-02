namespace Orleans.Lattice.Samples.MaterialisedViews;

/// <summary>
/// A minimal source record stored in the "people" tree. The view projections run
/// against this deserialized type (via the default JSON serializer), so the
/// predicate and aggregation selectors read strongly-typed properties.
/// </summary>
public sealed record User(string Name, int Age, string City);
