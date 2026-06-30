using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable, library-agnostic public-API contract guard for request-object
/// surfaces - the request-DTO analogue of
/// <see cref="PublicApiSizeContractTestsBase{TSelf}"/>. For every public method
/// that takes a request DTO carrying a size- or limit-like <see cref="int"/>
/// property (discovered by reflection from <see cref="ApiTypes"/> via
/// <see cref="SizeParameterDiscovery.DiscoverRequestSizeProperties"/>), this
/// fixture sets that property to a pathological value -
/// <see cref="int.MaxValue"/>, <see cref="int.MinValue"/>, <c>0</c> and
/// <c>-1</c> - invokes the method, and asserts the call never throws
/// <see cref="OutOfMemoryException"/>. Either the call completes (because the
/// size was clamped before any buffer was reserved), or it rejects the input
/// with an ordinary documented exception; what it must never do is reserve an
/// unbounded buffer sized straight from the caller's number and fault the host.
/// <para>
/// A read facade such as the State-API <c>ILatticeStateQuery</c> surface carries
/// its caller-influenced sizes on request records (for example
/// <c>CatalogRequest.PageSize</c>, <c>EntryScanRequest.PageSize</c>,
/// <c>EntryHistoryRequest.Limit</c>) rather than as bare method parameters, so
/// the method-parameter guard does not see them; this base closes that gap with
/// the same reflection-driven, auto-discovering approach. A newly added method,
/// request type, or size property is exercised automatically.
/// </para>
/// <para>
/// A concrete subclass only names its <see cref="ApiTypes"/>, hands back a live
/// service from <see cref="ResolveServiceAsync"/>, and builds a single valid
/// baseline request per request type from <see cref="BuildBaselineRequestAsync"/>
/// (with required fields pointing at live, seeded data so the call reaches its
/// size-sensitive path); the base sets the discovered size property on that
/// baseline by reflection and invokes the method.
/// </para>
/// <para>
/// This base is <see langword="abstract"/> and carries no cluster state, so it
/// is never discovered as a runnable fixture on its own; the inherited
/// <c>[Test]</c> is discovered through the concrete subclass. The type parameter
/// is the concrete subclass itself (the curiously-recurring pattern) so the
/// static <c>[TestCaseSource]</c> required by NUnit 4 can read the subclass's
/// <see cref="ApiTypes"/> via a throwaway probe instance.
/// </para>
/// </summary>
/// <typeparam name="TSelf">The concrete subclass, supplying its own <see cref="ApiTypes"/>.</typeparam>
public abstract class RequestSizeContractTestsBase<TSelf>
    where TSelf : RequestSizeContractTestsBase<TSelf>, new()
{
    /// <summary>
    /// The interfaces or classes whose request-DTO size/limit properties are
    /// audited. Evaluated at NUnit discovery time, so it must not depend on
    /// per-test setup (it should return only <see cref="Type"/> literals).
    /// </summary>
    protected abstract IReadOnlyCollection<Type> ApiTypes { get; }

    /// <summary>
    /// The size/limit property names to treat as size-like. Defaults to
    /// <see cref="SizeParameterDiscovery.DefaultSizeParameterNames"/>; override to
    /// add or restrict names for a particular surface.
    /// </summary>
    protected virtual IReadOnlySet<string> SizeParameterNames =>
        SizeParameterDiscovery.DefaultSizeParameterNames;

    /// <summary>
    /// The boundary values each discovered size property is exercised with.
    /// Defaults to <see cref="SizeParameterDiscovery.PathologicalBoundaryValues"/>.
    /// </summary>
    protected virtual IReadOnlyList<int> BoundaryValues =>
        SizeParameterDiscovery.PathologicalBoundaryValues;

    /// <summary>
    /// Returns a live instance of <paramref name="apiType"/> to invoke the
    /// audited method on (for example the resolved read facade).
    /// </summary>
    protected abstract ValueTask<object> ResolveServiceAsync(Type apiType);

    /// <summary>
    /// Builds one valid baseline request of <paramref name="requestType"/> with
    /// its size properties left at safe defaults and its required fields (tree
    /// id, key, ...) pointing at live, seeded data so the call reaches its
    /// size-sensitive allocation. The base mutates the discovered size property
    /// on the returned instance by reflection, so the request must be mutable
    /// through that property (an <see langword="init"/> accessor is sufficient -
    /// reflection sets it). A subclass should throw for an unconfigured request
    /// type so a newly introduced request DTO fails loudly rather than slipping
    /// past the guard.
    /// </summary>
    protected abstract ValueTask<object> BuildBaselineRequestAsync(Type requestType);

    /// <summary>
    /// The reflection-built test table: one row per discovered request-DTO size
    /// property per <see cref="BoundaryValues"/> entry. Static (as NUnit 4
    /// requires) - it builds a throwaway <typeparamref name="TSelf"/> probe to
    /// read the subclass's <see cref="ApiTypes"/> and tuning.
    /// </summary>
    public static IEnumerable<TestCaseData> RequestSizeContractCases()
    {
        var probe = new TSelf();
        var targets = SizeParameterDiscovery.DiscoverRequestSizeProperties(
            probe.ApiTypes,
            probe.SizeParameterNames);

        foreach (var target in targets)
        {
            foreach (var boundary in probe.BoundaryValues)
            {
                yield return new TestCaseData(target, boundary)
                    .SetName($"Request_size_property_tolerates_{target.DisplayName}_{SizeContractAssertions.Describe(boundary)}");
            }
        }
    }

    /// <summary>
    /// For each discovered request-DTO size/limit property and each pathological
    /// boundary value, sets the property to that value, invokes the method, and
    /// asserts it never faults with <see cref="OutOfMemoryException"/>.
    /// Completion or any other (documented) exception is an acceptable outcome.
    /// </summary>
    [Test]
    [TestCaseSource(nameof(RequestSizeContractCases))]
    public async Task Public_request_size_property_tolerates_pathological_input(
        RequestSizePropertyTarget target,
        int boundaryValue)
    {
        var service = await ResolveServiceAsync(target.ApiType);
        var request = await BuildBaselineRequestAsync(target.RequestType);
        target.SizeProperty.SetValue(request, boundaryValue);

        var arguments = BuildArguments(target, request);

        try
        {
            var result = Invoke(target.Method, service, arguments);
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
    /// Performs the reflective call <c>method.Invoke(service, arguments)</c>.
    /// The default runs the invocation from this shared library, which is correct
    /// for a <see langword="public"/> API surface. A surface whose facade type is
    /// <see langword="internal"/> (for example the State-API
    /// <c>ILatticeStateQuery</c>) must override this with the same one-line body
    /// so the reflective call is emitted in the consuming test assembly - the one
    /// granted <c>InternalsVisibleTo</c> access to the facade - avoiding a
    /// <see cref="MethodAccessException"/> that a cross-assembly invoke of an
    /// internal member would otherwise raise.
    /// </summary>
    /// <param name="method">The discovered method to invoke.</param>
    /// <param name="service">The live service instance to invoke it on.</param>
    /// <param name="arguments">The positional argument array.</param>
    protected virtual object? Invoke(MethodInfo method, object service, object?[] arguments) =>
        method.Invoke(service, arguments);

    /// <summary>
    /// Builds the positional argument array for the reflected call: the baseline
    /// request for the request-DTO parameter, and a defaulted value for every
    /// other parameter (typically a cancellation token).
    /// </summary>
    private static object?[] BuildArguments(RequestSizePropertyTarget target, object request)
    {
        var parameters = target.Method.GetParameters();
        var arguments = new object?[parameters.Length];

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            arguments[i] = ReferenceEquals(parameter, target.RequestParameter)
                || (parameter.Position == target.RequestParameter.Position
                    && parameter.Name == target.RequestParameter.Name)
                ? request
                : SizeContractAssertions.DefaultFor(parameter);
        }

        return arguments;
    }
}
