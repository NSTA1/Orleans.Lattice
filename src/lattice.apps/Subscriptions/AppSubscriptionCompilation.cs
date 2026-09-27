namespace Orleans.Lattice.Apps;

/// <summary>
/// The outcome of <see cref="AppSubscriptionCompiler.Compile"/>: either every declared subscription
/// resolved inside the ceiling, or subscription activation fails with every denial listed.
/// </summary>
public sealed class AppSubscriptionCompilation
{
    internal AppSubscriptionCompilation(IReadOnlyList<AppSubscriptionContext> subscriptions, IReadOnlyList<AppSubscriptionDenial> denials)
    {
        Subscriptions = subscriptions;
        Denials = denials;
    }

    /// <summary><c>true</c> when no subscription was denied.</summary>
    public bool Succeeded => Denials.Count == 0;

    /// <summary>The resolved subscriptions in manifest order; empty when compilation failed.</summary>
    public IReadOnlyList<AppSubscriptionContext> Subscriptions { get; }

    /// <summary>Every denied subscription in manifest order; empty on success.</summary>
    public IReadOnlyList<AppSubscriptionDenial> Denials { get; }
}
