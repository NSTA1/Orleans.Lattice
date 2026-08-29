using System.Reflection;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Pins the epic's first binding decision as a structural fact: <b>no code path
/// accepts caller-supplied PromQL</b>. The contract's own guard proves the request
/// types have nowhere to put an expression; this one proves the implementation
/// never opens a back door - no public entry point takes query text, and every
/// expression that can reach the backend originates in the authored catalogue.
/// </summary>
[TestFixture]
public sealed class TelemetryCuratedQuerySurfaceTests
{
    private static readonly Assembly FacadeAssembly = typeof(LatticeTelemetry).Assembly;

    /// <summary>
    /// The seams that legitimately take query text: the backend client and its
    /// default implementation (which are the wire itself, driven only by the facade
    /// with a rendered template) and the transport-neutral scanner and gate that
    /// inspect an expression rather than author one.
    /// </summary>
    private static readonly Type[] BackendSeams =
    [
        typeof(IPrometheusQueryClient),
        typeof(PrometheusQueryClient),
        typeof(PromQlMetricExtractor),
        typeof(TelemetryQueryAuthorizer),
    ];

    [Test]
    public void The_facade_implements_the_abstractions_contract()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(ILatticeTelemetry).IsAssignableFrom(typeof(LatticeTelemetry)), Is.True);
            Assert.That(typeof(LatticeTelemetry).IsSealed, Is.True);
        });
    }

    [Test]
    public void No_public_facade_operation_accepts_a_string_that_could_carry_query_text()
    {
        var offenders = typeof(LatticeTelemetry)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .SelectMany(m => m.GetParameters().Select(p => (Method: m, Parameter: p)))
            .Where(x => x.Parameter.ParameterType == typeof(string))
            .Select(x => $"{x.Method.Name}({x.Parameter.Name})")
            .ToArray();

        Assert.That(offenders, Is.Empty,
            "A bare string parameter is how raw query text would reach the facade. Offenders: "
            + string.Join(", ", offenders));
    }

    [Test]
    public void The_only_public_types_taking_query_text_are_the_backend_seam_and_the_scanner()
    {
        var offenders = FacadeAssembly.GetExportedTypes()
            .Where(t => !BackendSeams.Contains(t))
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
                .Where(m => m.DeclaringType == t)
                .SelectMany(m => m.GetParameters().Select(p => (Type: t, Method: m, Parameter: p))))
            .Where(x => x.Parameter.ParameterType == typeof(string))
            .Where(x => IsQueryTextName(x.Parameter.Name))
            .Select(x => $"{x.Type.Name}.{x.Method.Name}({x.Parameter.Name})")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToArray();

        Assert.That(offenders, Is.Empty,
            "Only the backend client and the conservative scanner take an expression, and neither "
            + "is reachable from a caller with text of its own. Offenders: "
            + string.Join(", ", offenders));
    }

    [Test]
    public void The_query_definition_is_never_accepted_as_input_by_a_public_operation()
    {
        // A definition is the only shape carrying a template. Accepting one anywhere on
        // an instance operation would let a caller author a query.
        var offenders = FacadeAssembly.GetExportedTypes()
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .Where(m => m.DeclaringType == t)
                .SelectMany(m => m.GetParameters().Select(p => (Type: t, Method: m, Parameter: p))))
            .Where(x => Carries<TelemetryQueryDefinition>(x.Parameter.ParameterType))
            .Select(x => $"{x.Type.Name}.{x.Method.Name}")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToArray();

        Assert.That(offenders, Is.Empty, string.Join(", ", offenders));
    }

    [Test]
    public void Only_the_catalogue_constructor_ingests_definitions()
    {
        var accepting = FacadeAssembly.GetExportedTypes()
            .SelectMany(t => t.GetConstructors()
                .SelectMany(c => c.GetParameters().Select(p => (Type: t, Parameter: p))))
            .Where(x => Carries<TelemetryQueryDefinition>(x.Parameter.ParameterType))
            .Select(x => x.Type.Name)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToArray();

        Assert.That(accepting, Is.EqualTo(new[] { nameof(LatticeTelemetryQueryCatalog) }),
            "The catalogue is built once, by the host, from server-authored definitions. That is "
            + "the single ingestion point for a query expression.");
    }

    [Test]
    public void The_request_type_the_facade_accepts_carries_no_expression()
    {
        var offenders = typeof(TelemetryQueryRequest)
            .GetProperties()
            .Where(p => p.PropertyType == typeof(string))
            .Where(p => IsQueryTextName(p.Name))
            .Select(p => p.Name)
            .ToArray();

        Assert.That(offenders, Is.Empty, string.Join(", ", offenders));
    }

    [Test]
    public async Task Every_query_the_facade_sends_is_derived_from_an_authored_template()
    {
        // The end-to-end proof: drive every catalogue entry and check the text that
        // reached the backend is the entry's own template with only the scope and
        // window slots filled in.
        foreach (var definition in LatticeTelemetryQueries.Definitions)
        {
            var harness = new TelemetryFacadeHarness().ForTenant("acme");
            var descriptor = definition.Descriptor;
            var request = descriptor.Kind == TelemetryQueryKind.Range
                ? TelemetryFacadeHarness.RangeRequest(descriptor.QueryId)
                : TelemetryFacadeHarness.InstantRequest(descriptor.QueryId);

            await harness.Build().QueryAsync(request);

            var skeleton = Skeleton(harness.Backend.SingleQuery);
            Assert.That(skeleton, Is.EqualTo(Skeleton(definition.QueryTemplate)),
                $"'{descriptor.QueryId}' sent text that is not its authored template.");
        }
    }

    [Test]
    public void The_backend_seam_exemption_list_admits_exactly_the_wire_and_the_scanner()
    {
        Assert.That(
            BackendSeams.Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal),
            Is.EqualTo(new[]
            {
                nameof(IPrometheusQueryClient),
                nameof(PrometheusQueryClient),
                nameof(PromQlMetricExtractor),
                nameof(TelemetryQueryAuthorizer),
            }.OrderBy(n => n, StringComparer.Ordinal)),
            "The exemption list must not grow. It exists so the wire and the conservative scanner "
            + "can take an expression without disarming the guard; a fifth entry is how a "
            + "caller-supplied-PromQL path would be admitted one type at a time.");
    }

    [Test]
    public void The_guard_still_detects_a_type_that_took_query_text()
    {
        var offenders = typeof(QueryTextProbe)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .SelectMany(m => m.GetParameters().Select(p => (Method: m, Parameter: p)))
            .Where(x => x.Parameter.ParameterType == typeof(string))
            .Where(x => IsQueryTextName(x.Parameter.Name))
            .Select(x => $"{x.Method.Name}({x.Parameter.Name})")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToArray();

        Assert.That(offenders, Is.EqualTo(new[] { "Evaluate(promql)", "Run(expression)", "Send(query)" }),
            "An exemption that accidentally admitted everything would pass the real assertions "
            + "vacuously but fails here.");
    }

    [Test]
    public void The_guard_admits_a_query_id_parameter()
    {
        Assert.Multiple(() =>
        {
            Assert.That(IsQueryTextName("queryId"), Is.False,
                "Selecting a curated query by id is the whole point of the contract.");
            Assert.That(IsQueryTextName("query"), Is.True);
        });
    }

    /// <summary>
    /// A stand-in shaped like a type that grew a caller-supplied query parameter, so
    /// the guard's detection can be proven positively rather than only vacuously.
    /// </summary>
    private sealed class QueryTextProbe
    {
        public void Send(string query) => _ = query;

        public void Run(string expression) => _ = expression;

        public void Evaluate(string promql) => _ = promql;

        public void Select(string queryId) => _ = queryId;

        public void Narrow(string treeId) => _ = treeId;
    }

    /// <summary>
    /// Reduces a query to its template skeleton by blanking every quoted label value
    /// and every bracketed duration, so an authored template and the text rendered
    /// from it compare equal while any structural difference does not.
    /// </summary>
    private static string Skeleton(string query)
    {
        var withoutValues = System.Text.RegularExpressions.Regex.Replace(
            query, "\"(\\\\.|[^\"\\\\])*\"", "\"*\"");
        var withoutSlots = withoutValues
            .Replace(TelemetryQueryTemplate.ScopeToken, string.Empty, StringComparison.Ordinal)
            .Replace(TelemetryQueryTemplate.WindowToken, "*", StringComparison.Ordinal);

        return System.Text.RegularExpressions.Regex.Replace(
            System.Text.RegularExpressions.Regex.Replace(withoutSlots, @"\[[^\]]*\]", "[*]"),
            @"[a-z_]+=""\*"",",
            string.Empty);
    }

    private static bool Carries<T>(Type parameterType) =>
        parameterType == typeof(T)
        || (parameterType.IsGenericType
            && parameterType.GetGenericArguments().Any(a => a == typeof(T)));

    private static bool IsQueryTextName(string? name) =>
        name is not null
        && (name.Contains("query", StringComparison.OrdinalIgnoreCase)
            || name.Contains("expr", StringComparison.OrdinalIgnoreCase)
            || name.Contains("promql", StringComparison.OrdinalIgnoreCase)
            || name.Contains("template", StringComparison.OrdinalIgnoreCase))
        && !name.Contains("queryId", StringComparison.OrdinalIgnoreCase);
}
