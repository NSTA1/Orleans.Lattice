/// <summary>
/// An order with a monetary total. Serialized as JSON so the atomic-batch guard
/// predicate (<c>o =&gt; o.Total &gt;= ...</c>) can be evaluated server-side.
/// </summary>
public sealed record Order(string Id, decimal Total);
