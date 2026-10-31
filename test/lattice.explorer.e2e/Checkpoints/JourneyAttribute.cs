namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>Gives a journey fixture its journey id.</summary>
/// <param name="id">The journey id, lower-case kebab-case.</param>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class JourneyAttribute(string id) : Attribute, IJourneyIdentity
{
    /// <inheritdoc />
    public string JourneyId { get; } = JourneyNames.RequireSlug(id, nameof(id));

    /// <summary>
    /// The journey id of the running NUnit test's fixture, from the first attribute on it
    /// that implements <see cref="IJourneyIdentity"/>, or <see langword="null"/>.
    /// </summary>
    public static string? Current()
    {
        var fixture = TestContext.CurrentContext.Test.Type;
        return fixture?.GetCustomAttributes(inherit: true).OfType<IJourneyIdentity>().FirstOrDefault()?.JourneyId;
    }
}
