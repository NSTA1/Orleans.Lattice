using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable, library-agnostic public-API contract guard. For every public method
/// that takes a size- or limit-like <see cref="int"/> parameter (discovered by
/// reflection from <see cref="ApiTypes"/>), this fixture asserts the call
/// tolerates pathological inputs - <see cref="int.MaxValue"/>,
/// <see cref="int.MinValue"/>, <c>0</c> and <c>-1</c> - by never throwing
/// <see cref="OutOfMemoryException"/>. Either the call completes, or it rejects
/// the input with an ordinary documented exception (for example
/// <see cref="ArgumentOutOfRangeException"/>); what it must never do is reserve
/// an unbounded buffer sized straight from the caller's number and fault the
/// host before reading a single element.
/// <para>
/// The discovery and invocation machinery lives here so any test project can
/// reuse it: a concrete subclass only names its <see cref="ApiTypes"/>, hands
/// back a live instance from <see cref="ResolveInstanceAsync"/>, and supplies the
/// handful of meaningful non-size arguments (a live cursor id, an existing key)
/// through <see cref="ResolveArgumentAsync"/>. Because the table is rebuilt from
/// reflection on every run, a newly added size parameter is exercised
/// automatically - the next missed sibling is caught in CI rather than in
/// review.
/// </para>
/// <para>
/// This base is <see langword="abstract"/> and carries no cluster state, so it
/// is never discovered as a runnable fixture on its own; the inherited
/// <c>[Test]</c> is discovered through the concrete subclass in the consuming
/// test assembly.
/// </para>
/// <para>
/// The type parameter is the concrete subclass itself (the curiously-recurring
/// pattern): NUnit 4 requires a <see langword="static"/>
/// <c>[TestCaseSource]</c>, and a static member cannot read instance state, so
/// the static source constructs a throwaway <typeparamref name="TSelf"/> probe
/// to read <see cref="ApiTypes"/>. Construction at discovery time is cheap
/// because it must not touch any per-test setup.
/// </para>
/// </summary>
/// <typeparam name="TSelf">The concrete subclass, supplying its own <see cref="ApiTypes"/>.</typeparam>
public abstract class PublicApiSizeContractTestsBase<TSelf>
    where TSelf : PublicApiSizeContractTestsBase<TSelf>, new()
{
    /// <summary>
    /// The interfaces or classes whose public size/limit parameters are audited.
    /// Evaluated at NUnit discovery time, so it must not depend on per-test
    /// setup (it should return only <see cref="Type"/> literals).
    /// </summary>
    protected abstract IReadOnlyCollection<Type> ApiTypes { get; }

    /// <summary>
    /// The size/limit parameter names to treat as size-like. Defaults to
    /// <see cref="SizeParameterDiscovery.DefaultSizeParameterNames"/>; override to
    /// add or restrict names for a particular surface.
    /// </summary>
    protected virtual IReadOnlySet<string> SizeParameterNames =>
        SizeParameterDiscovery.DefaultSizeParameterNames;

    /// <summary>
    /// The boundary values each discovered size parameter is exercised with.
    /// Defaults to <see cref="SizeParameterDiscovery.PathologicalBoundaryValues"/>.
    /// </summary>
    protected virtual IReadOnlyList<int> BoundaryValues =>
        SizeParameterDiscovery.PathologicalBoundaryValues;

    /// <summary>
    /// Returns a live instance of <paramref name="apiType"/> to invoke the
    /// audited method on. Called once per test case (so a subclass may hand back
    /// a fresh, isolated instance per case, for example a freshly seeded tree).
    /// </summary>
    protected abstract ValueTask<object> ResolveInstanceAsync(Type apiType);

    /// <summary>
    /// Supplies a value for a non-size parameter of the audited method. Return
    /// <see cref="ContractArgument.UseDefault"/> to let the base substitute the
    /// parameter's own default. A subclass typically only special-cases the
    /// parameters that must be meaningful for the call to reach its
    /// size-sensitive allocation (for example opening a matching live cursor for
    /// a <c>cursorId</c> parameter, or returning an existing key).
    /// </summary>
    /// <param name="target">The method/parameter pair under audit.</param>
    /// <param name="parameter">The non-size parameter whose value is requested.</param>
    /// <param name="instance">The live instance returned by <see cref="ResolveInstanceAsync"/>.</param>
    protected virtual ValueTask<object?> ResolveArgumentAsync(
        SizeParameterTarget target,
        ParameterInfo parameter,
        object instance) => new(ContractArgument.UseDefault);

    /// <summary>
    /// The reflection-built test table: one row per discovered size parameter per
    /// <see cref="BoundaryValues"/> entry. Used as the <c>[TestCaseSource]</c> for
    /// <see cref="Public_size_parameter_tolerates_pathological_input"/>. Static
    /// (as NUnit 4 requires) - it builds a throwaway <typeparamref name="TSelf"/>
    /// probe to read the subclass's <see cref="ApiTypes"/> and tuning.
    /// </summary>
    public static IEnumerable<TestCaseData> SizeContractCases()
    {
        var probe = new TSelf();
        var targets = SizeParameterDiscovery.Discover(probe.ApiTypes, probe.SizeParameterNames);
        foreach (var target in targets)
        {
            foreach (var boundary in probe.BoundaryValues)
            {
                yield return new TestCaseData(target, boundary)
                    .SetName($"Size_param_tolerates_{target.DisplayName}_{SizeContractAssertions.Describe(boundary)}");
            }
        }
    }

    /// <summary>
    /// For each discovered public size/limit parameter and each pathological
    /// boundary value, invokes the method and asserts it never faults with
    /// <see cref="OutOfMemoryException"/>. Completion or any other (documented)
    /// exception is an acceptable outcome.
    /// </summary>
    [Test]
    [TestCaseSource(nameof(SizeContractCases))]
    public async Task Public_size_parameter_tolerates_pathological_input(
        SizeParameterTarget target,
        int boundaryValue)
    {
        var instance = await ResolveInstanceAsync(target.ApiType);
        var arguments = await BuildArgumentsAsync(target, boundaryValue, instance);

        try
        {
            var result = target.Method.Invoke(instance, arguments);
            await SizeContractAssertions.AwaitResultAsync(result);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            SizeContractAssertions.AssertNoOutOfMemory(ex.InnerException, target.DisplayName, boundaryValue);
        }
        catch (Exception ex)
        {
            SizeContractAssertions.AssertNoOutOfMemory(ex, target.DisplayName, boundaryValue);
        }
    }

    /// <summary>
    /// Builds the positional argument array for the reflected call: the
    /// boundary value for the size parameter, and a subclass-supplied (or
    /// defaulted) value for every other parameter.
    /// </summary>
    private async Task<object?[]> BuildArgumentsAsync(
        SizeParameterTarget target,
        int boundaryValue,
        object instance)
    {
        var parameters = target.Method.GetParameters();
        var arguments = new object?[parameters.Length];

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            if (ReferenceEquals(parameter, target.Parameter)
                || (parameter.Position == target.Parameter.Position
                    && parameter.Name == target.Parameter.Name))
            {
                arguments[i] = boundaryValue;
                continue;
            }

            var supplied = await ResolveArgumentAsync(target, parameter, instance);
            arguments[i] = ReferenceEquals(supplied, ContractArgument.UseDefault)
                ? SizeContractAssertions.DefaultFor(parameter)
                : supplied;
        }

        return arguments;
    }
}
