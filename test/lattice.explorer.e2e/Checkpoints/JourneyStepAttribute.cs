using System.Reflection;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// Names one step of a journey on its test method: the step id every screenshot taken
/// during the test is filed under (<c>{nnn}-{step}-{substep}.png</c>), and the step's title.
/// Each distinct UI state inside the step is a <c>SubstepAsync</c> checkpoint.
/// </summary>
/// <param name="id">The step id, lower-case kebab-case, such as <c>sign-in</c>.</param>
/// <param name="title">The step's title, in words.</param>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class JourneyStepAttribute(string id, string title) : Attribute
{
    /// <summary>The step id.</summary>
    public string Id { get; } = JourneyNames.RequireSlug(id, nameof(id));

    /// <summary>The step's title.</summary>
    public string Title { get; } = string.IsNullOrWhiteSpace(title) ? throw new ArgumentException("A step needs a title.", nameof(title)) : title;

    /// <summary>The step of the running NUnit test, or <see langword="null"/> when its method carries none.</summary>
    public static JourneyStepAttribute? Current()
    {
        var test = TestContext.CurrentContext.Test;
        var method = test.MethodName is { } name ? test.Type?.GetMethod(name, BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic) : null;
        return method?.GetCustomAttribute<JourneyStepAttribute>(inherit: true);
    }
}
