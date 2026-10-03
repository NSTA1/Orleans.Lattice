using System.Reflection;
using System.Runtime.ExceptionServices;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Enumerates the per-module Formal gates by reflection and runs them against
/// an arbitrary <see cref="SpecModule"/>.
/// <para>
/// A gate is any test in this namespace whose first parameter is a
/// <see cref="SpecModule"/>. Finding them by signature rather than by a list
/// is the point: a gate added next month is covered by the discovery control
/// without anyone remembering to enrol it, which is the same reason modules are
/// discovered from disk rather than listed.
/// </para>
/// </summary>
internal static class SpecModuleGates
{
    /// <summary>The test category of the gates that run TLC.</summary>
    public const string TlcCategory = "Tlc";

    /// <summary>One per-module gate.</summary>
    /// <param name="Method">The test method.</param>
    /// <param name="Source">Its <c>[TestCaseSource]</c>, or null when it has none.</param>
    public sealed record Gate(MethodInfo Method, TestCaseSourceAttribute? Source)
    {
        /// <summary>The fixture declaring the gate.</summary>
        public Type Fixture => Method.DeclaringType!;

        /// <summary><c>Fixture.Method</c>, for messages.</summary>
        public string Name => $"{Fixture.Name}.{Method.Name}";

        /// <summary>Whether the gate needs the TLA+ toolchain.</summary>
        public bool RunsTlc =>
            Fixture.GetCustomAttributes<CategoryAttribute>().Concat(Method.GetCustomAttributes<CategoryAttribute>())
                .Any(c => string.Equals(c.Name, TlcCategory, StringComparison.Ordinal));

        /// <summary>
        /// The expander on <see cref="SpecModuleCases"/> that produces this
        /// gate's cases for one module, or null when the gate does not draw
        /// from that class or the expander is missing.
        /// </summary>
        public MethodInfo? Expander =>
            Source?.SourceType == typeof(SpecModuleCases) && Source.SourceName is { } name
                ? typeof(SpecModuleCases).GetMethod(name + "For", BindingFlags.Public | BindingFlags.Static, [typeof(SpecModule)])
                : null;

        /// <inheritdoc />
        public override string ToString() => Name;
    }

    /// <summary>
    /// Every per-module gate in the Formal namespace, ordered by name. Public
    /// methods only, because NUnit runs nothing else; this reflection invokes
    /// test methods, never a private production member.
    /// </summary>
    public static IReadOnlyList<Gate> All() =>
        typeof(SpecModule).Assembly
            .GetTypes()
            .Where(t => t.Namespace == typeof(SpecModule).Namespace && t.IsClass)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
            .Where(m => m.GetParameters() is [{ ParameterType: var first }, ..] && first == typeof(SpecModule))
            .Where(m => m.IsDefined(typeof(TestAttribute)) || m.IsDefined(typeof(TestCaseSourceAttribute)) || m.IsDefined(typeof(TestCaseAttribute)))
            .Select(m => new Gate(m, m.GetCustomAttribute<TestCaseSourceAttribute>()))
            .OrderBy(g => g.Name, StringComparer.Ordinal)
            .ToArray();

    /// <summary>The cases <paramref name="gate"/> would run for <paramref name="module"/>.</summary>
    public static IReadOnlyList<object[]> CasesFor(Gate gate, SpecModule module)
    {
        var expander = gate.Expander
            ?? throw new InvalidOperationException($"{gate.Name} does not draw its cases from {nameof(SpecModuleCases)}.");
        return ((IEnumerable<object[]>)expander.Invoke(null, [module])!).ToArray();
    }

    /// <summary>
    /// Runs every case of <paramref name="gate"/> for <paramref name="module"/>,
    /// rethrowing the first failure unwrapped. <paramref name="fixture"/>
    /// supplies the instance for an instance gate; when null one is created.
    /// Returns the number of cases run.
    /// </summary>
    public static int Run(Gate gate, SpecModule module, object? fixture = null)
    {
        var cases = CasesFor(gate, module);
        var instance = gate.Method.IsStatic ? null : fixture ?? Activator.CreateInstance(gate.Fixture);

        foreach (var arguments in cases)
        {
            try
            {
                gate.Method.Invoke(instance, arguments);
            }
            catch (TargetInvocationException wrapped) when (wrapped.InnerException is not null)
            {
                ExceptionDispatchInfo.Capture(wrapped.InnerException).Throw();
            }
        }

        return cases.Count;
    }
}
