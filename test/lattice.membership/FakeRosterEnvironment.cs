namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// An in-memory <see cref="IStaticRosterEnvironment"/> for unit tests: returns a
/// fixed set of variable names without touching the real process environment.
/// </summary>
internal sealed class FakeRosterEnvironment(params string[] names) : IStaticRosterEnvironment
{
    public IReadOnlyCollection<string> GetVariableNames() => names;
}
