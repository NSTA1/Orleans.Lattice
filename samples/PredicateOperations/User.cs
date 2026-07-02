/// <summary>
/// A user with a name and age. Serialized as JSON so the server can evaluate a
/// predicate (e.g. <c>u =&gt; u.Age &gt;= 18</c>) against each value's document
/// on the owning leaf.
/// </summary>
public sealed record User(string Name, int Age);
