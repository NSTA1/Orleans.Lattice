namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// A fixed, typed key of the <see cref="StoryLedger"/>: the name a chapter records a run-time
/// fact under, and the type the fact has.
/// </summary>
/// <typeparam name="T">The fact's type.</typeparam>
/// <param name="Name">The key, such as <c>backup.orders-baseline</c>.</param>
public sealed record StoryKey<T>(string Name)
    where T : notnull
{
    /// <inheritdoc />
    public override string ToString() => Name;
}
