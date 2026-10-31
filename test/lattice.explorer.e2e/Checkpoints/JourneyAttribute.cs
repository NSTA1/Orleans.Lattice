using NUnit.Framework.Interfaces;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// Gives a journey fixture its journey id, and after each of its tests discards the
/// session traces of a test that passed (a failed test keeps them beside the estate's logs).
/// </summary>
/// <param name="id">The journey id, lower-case kebab-case.</param>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class JourneyAttribute(string id) : Attribute, IJourneyIdentity, ITestAction
{
    /// <inheritdoc />
    public string JourneyId { get; } = JourneyNames.RequireSlug(id, nameof(id));

    /// <inheritdoc />
    public ActionTargets Targets => ActionTargets.Test;

    /// <summary>
    /// The journey id of the running NUnit test's fixture, from the first attribute on it
    /// that implements <see cref="IJourneyIdentity"/>, or <see langword="null"/>.
    /// </summary>
    public static string? Current()
    {
        var fixture = TestContext.CurrentContext.Test.Type;
        return fixture?.GetCustomAttributes(inherit: true).OfType<IJourneyIdentity>().FirstOrDefault()?.JourneyId;
    }

    /// <inheritdoc />
    public void BeforeTest(ITest test)
    {
    }

    /// <inheritdoc />
    public void AfterTest(ITest test)
    {
        ArgumentNullException.ThrowIfNull(test);
        if (TestContext.CurrentContext.Result.Outcome.Status == TestStatus.Failed)
        {
            JourneyTraces.Keep(test.Id);
        }
        else
        {
            JourneyTraces.Discard(test.Id);
        }
    }
}
