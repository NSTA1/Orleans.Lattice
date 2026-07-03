namespace Orleans.Lattice.Membership.Entra.Tests;

/// <summary>
/// A recording <see cref="IEntraGroupResolver"/> that returns a fixed set of
/// group ids and counts how often it was called. No network or Graph involved.
/// </summary>
internal sealed class FakeEntraGroupResolver(params string[] groups) : IEntraGroupResolver
{
    private readonly IReadOnlyCollection<string> _groups = groups;

    /// <summary>The number of times <see cref="ResolveGroupsAsync"/> was invoked.</summary>
    public int CallCount { get; private set; }

    /// <summary>The context of the most recent call, or <c>null</c> when never called.</summary>
    public EntraGroupResolutionContext? LastContext { get; private set; }

    /// <inheritdoc />
    public ValueTask<IReadOnlyCollection<string>> ResolveGroupsAsync(
        EntraGroupResolutionContext context,
        CancellationToken cancellationToken = default)
    {
        CallCount++;
        LastContext = context;
        return new ValueTask<IReadOnlyCollection<string>>(_groups);
    }
}
