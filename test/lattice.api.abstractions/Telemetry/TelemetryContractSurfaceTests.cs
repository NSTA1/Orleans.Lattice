using System.Reflection;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Abstractions.Tests.Telemetry;

/// <summary>
/// Pins the two load-bearing design rules of the telemetry contract as structural
/// facts rather than prose, so a later change that violates one fails here instead
/// of shipping.
/// <para>
/// <b>Curated queries only.</b> No type a caller can send to the facade carries a
/// query expression: the client-facing catalogue entry and the request have nowhere
/// to put one, and the only type that holds a template is the server-side
/// definition, which no operation accepts as input.
/// </para>
/// <para>
/// <b>The tenant is derived, never asserted.</b> The request carries no tenant-id
/// field at all, only a requested visibility, and the response always reports the
/// scope actually applied.
/// </para>
/// </summary>
[TestFixture]
public sealed class TelemetryContractSurfaceTests
{
    private static readonly Assembly AbstractionsAssembly = typeof(ILatticeTelemetry).Assembly;

    /// <summary>
    /// Substrings that would indicate a raw query expression is being carried on a
    /// caller-facing type. Matched case-insensitively against property names.
    /// </summary>
    private static readonly string[] QueryTextMarkers =
        ["promql", "expr", "expression", "template", "rawquery", "querytext"];

    /// <summary>
    /// Substrings that would indicate a caller-supplied tenant identity. Matched
    /// case-insensitively against property names.
    /// </summary>
    private static readonly string[] TenantIdentityMarkers = ["tenantid", "tenantname", "tenants"];

    /// <summary>
    /// The exhaustive exemption list for <see cref="TenantIdentityMarkers"/> on the
    /// request: the single operator-only tenant selector the contract deliberately
    /// carries. It is a <em>request</em> the facade re-validates and may refuse, not
    /// an assertion, which is why it is permitted at all.
    /// <para>
    /// This list must stay at exactly one entry. It exists to make room for that one
    /// field without disarming the guard, so a second tenant-ish property appearing
    /// on the request still breaks the build.
    /// </para>
    /// </summary>
    private static readonly string[] PermittedTenantSelectors =
        [nameof(TelemetryQueryRequest.RequestedTenantId)];

    [Test]
    public void The_facade_is_a_public_interface_in_the_abstractions_assembly()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(ILatticeTelemetry).IsInterface, Is.True);
            Assert.That(typeof(ILatticeTelemetry).IsPublic, Is.True);
            Assert.That(typeof(ILatticeTelemetry).Assembly, Is.EqualTo(AbstractionsAssembly));
            Assert.That(typeof(ILatticeTelemetry).Namespace, Is.EqualTo("Orleans.Lattice.Api.Telemetry"));
        });
    }

    [Test]
    public void The_facade_exposes_catalogue_discovery_and_query_evaluation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(ILatticeTelemetry).GetMethod(nameof(ILatticeTelemetry.GetCatalogAsync)),
                Is.Not.Null);
            Assert.That(typeof(ILatticeTelemetry).GetMethod(nameof(ILatticeTelemetry.QueryAsync)),
                Is.Not.Null);
            Assert.That(typeof(ILatticeTelemetry).GetMethods(), Has.Length.EqualTo(2),
                "The read-only facade evaluates queries and nothing else; a new operation is a "
                + "deliberate contract change.");
        });
    }

    [Test]
    public void The_facade_selects_a_query_by_id_and_never_accepts_a_definition()
    {
        var query = typeof(ILatticeTelemetry).GetMethod(nameof(ILatticeTelemetry.QueryAsync))!;
        var parameterTypes = query.GetParameters().Select(p => p.ParameterType).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(parameterTypes, Does.Contain(typeof(TelemetryQueryRequest)));
            Assert.That(parameterTypes, Does.Not.Contain(typeof(TelemetryQueryDefinition)),
                "Accepting a definition would let a caller supply the query expression.");
            Assert.That(parameterTypes, Does.Not.Contain(typeof(string)),
                "A bare string parameter is how raw query text would sneak onto the facade.");
        });
    }

    [Test]
    public void No_caller_facing_type_carries_a_query_expression()
    {
        Type[] callerFacing =
        [
            typeof(TelemetryQueryRequest),
            typeof(TelemetryQueryDescriptor),
            typeof(TelemetryQueryCatalog),
            typeof(TelemetryQueryResponse),
        ];

        var offenders = callerFacing
            .SelectMany(t => t.GetProperties().Select(p => (Type: t, Property: p)))
            .Where(x => QueryTextMarkers.Any(marker =>
                x.Property.Name.Contains(marker, StringComparison.OrdinalIgnoreCase)))
            .Select(x => $"{x.Type.Name}.{x.Property.Name}")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        Assert.That(offenders, Is.Empty,
            "The facade exposes server-authored queries selected by id and never accepts query text. "
            + "Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void Only_the_server_side_definition_carries_the_query_template()
    {
        var template = typeof(TelemetryQueryDefinition)
            .GetProperty(nameof(TelemetryQueryDefinition.QueryTemplate));

        Assert.That(template, Is.Not.Null,
            "The definition is the single home for a query expression, so the split is real rather "
            + "than nominal.");
    }

    [Test]
    public void The_request_carries_no_unexempted_tenant_identity_field()
    {
        var offenders = TenantIdentityOffenders(typeof(TelemetryQueryRequest));

        Assert.That(offenders, Is.Empty,
            "The effective tenant is derived server-side from the authenticated caller and must never "
            + "be taken on trust from a request field. Exactly one operator-only tenant selector is "
            + "exempt; anything else is a leak. Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void Exactly_one_tenant_selector_is_exempt_from_the_tenant_identity_guard()
    {
        Assert.Multiple(() =>
        {
            Assert.That(PermittedTenantSelectors, Has.Length.EqualTo(1),
                "The exemption list must stay at one entry. Growing it is how this guard would be "
                + "quietly disarmed one property at a time.");
            Assert.That(PermittedTenantSelectors[0],
                Is.EqualTo(nameof(TelemetryQueryRequest.RequestedTenantId)));
        });
    }

    [Test]
    public void The_exempted_tenant_selector_still_exists_on_the_request()
    {
        var selector = typeof(TelemetryQueryRequest)
            .GetProperty(nameof(TelemetryQueryRequest.RequestedTenantId));

        Assert.Multiple(() =>
        {
            Assert.That(selector, Is.Not.Null,
                "A stale exemption naming a property that no longer exists would silently widen the "
                + "guard's blind spot.");
            Assert.That(selector!.PropertyType, Is.EqualTo(typeof(string)),
                "The selector is optional - a caller that is not requesting a single tenant omits it.");
        });
    }

    [Test]
    public void The_narrowed_guard_still_detects_a_second_tenant_identity_field()
    {
        var offenders = TenantIdentityOffenders(typeof(TenantIdentityProbe));

        Assert.That(offenders, Is.EqualTo(new[] { "AssertedTenantId", "TenantName", "Tenants" }),
            "The exemption must admit exactly the one permitted selector by name and keep flagging "
            + "every other tenant-ish property, so the guard cannot be defeated by adding a second "
            + "field beside the exempted one.");
    }

    [Test]
    public void The_narrowed_guard_admits_the_permitted_selector_and_ignores_unrelated_properties()
    {
        var offenders = TenantIdentityOffenders(typeof(TenantIdentityProbe));

        Assert.Multiple(() =>
        {
            Assert.That(offenders, Does.Not.Contain(nameof(TelemetryQueryRequest.RequestedTenantId)));
            Assert.That(offenders, Does.Not.Contain("QueryId"));
        });
    }

    /// <summary>
    /// Applies the tenant-identity guard's predicate to <paramref name="type"/>:
    /// every public property whose name contains a
    /// <see cref="TenantIdentityMarkers"/> substring and is not named by
    /// <see cref="PermittedTenantSelectors"/>. Shared by the real assertion and the
    /// probe assertion so the two can never drift apart.
    /// </summary>
    private static IReadOnlyList<string> TenantIdentityOffenders(Type type) =>
        type.GetProperties()
            .Where(p => !PermittedTenantSelectors.Contains(p.Name, StringComparer.Ordinal))
            .Where(p => TenantIdentityMarkers.Any(marker =>
                p.Name.Contains(marker, StringComparison.OrdinalIgnoreCase)))
            .Select(p => p.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// A stand-in shaped like a request that has grown extra tenant-identity fields
    /// beside the one permitted selector. It exists so the narrowed guard's
    /// detection can be proven positively - an exemption that accidentally admitted
    /// everything would pass the real assertion vacuously but fails here.
    /// </summary>
    private sealed class TenantIdentityProbe
    {
        public string? RequestedTenantId { get; init; }

        public string? AssertedTenantId { get; init; }

        public string? TenantName { get; init; }

        public string? Tenants { get; init; }

        public string? QueryId { get; init; }
    }

    [Test]
    public void The_request_serialized_member_ids_are_sequential_from_zero()
    {
        var ids = typeof(TelemetryQueryRequest).GetProperties()
            .Select(p => p.GetCustomAttribute<Orleans.IdAttribute>())
            .Where(a => a is not null)
            .Select(a => (int)a!.Id)
            .OrderBy(id => id)
            .ToArray();

        Assert.That(ids, Is.EqualTo(new[] { 0, 1, 2, 3, 4 }),
            "The tenant selector was appended at the next free id. Reusing or reordering an id would "
            + "silently reinterpret an existing field on the wire.");
    }

    [Test]
    public void The_request_carries_a_requested_visibility_for_tenancy()
    {
        var visibility = typeof(TelemetryQueryRequest)
            .GetProperty(nameof(TelemetryQueryRequest.RequestedVisibility));

        Assert.Multiple(() =>
        {
            Assert.That(visibility, Is.Not.Null);
            Assert.That(visibility!.PropertyType, Is.EqualTo(typeof(TelemetryTenantVisibility)));
        });
    }

    [Test]
    public void The_response_always_reports_the_scope_that_was_applied()
    {
        var scope = typeof(TelemetryQueryResponse).GetProperty(nameof(TelemetryQueryResponse.Scope));

        Assert.Multiple(() =>
        {
            Assert.That(scope, Is.Not.Null);
            Assert.That(scope!.PropertyType, Is.EqualTo(typeof(TelemetryTenantScope)),
                "The scope is a value type, so there is no response shape that omits it by being null.");
        });
    }

    [Test]
    public void Every_public_telemetry_type_lives_in_the_telemetry_contract_namespace()
    {
        var strays = AbstractionsAssembly.GetExportedTypes()
            .Where(t => t.Namespace is null
                || !t.Namespace.StartsWith("OrleansCodeGen", StringComparison.Ordinal))
            .Where(t => t.Name.StartsWith("Telemetry", StringComparison.Ordinal)
                || t.Name.Equals(nameof(ILatticeTelemetry), StringComparison.Ordinal)
                || t.Name.Equals(nameof(ApiTelemetryTypeAliases), StringComparison.Ordinal))
            .Where(t => t.Namespace != "Orleans.Lattice.Api.Telemetry")
            .Select(t => t.FullName)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(strays, Is.Empty,
            "Every telemetry contract type must live in Orleans.Lattice.Api.Telemetry. Offenders: "
            + string.Join(", ", strays));
    }

    [TestCase(TelemetryQueryKind.Instant, 0)]
    [TestCase(TelemetryQueryKind.Range, 1)]
    public void Query_kind_values_are_stable(TelemetryQueryKind kind, int expected)
    {
        Assert.That((int)kind, Is.EqualTo(expected));
    }

    [TestCase(TelemetryResultKind.Empty, 0)]
    [TestCase(TelemetryResultKind.Vector, 1)]
    [TestCase(TelemetryResultKind.Matrix, 2)]
    [TestCase(TelemetryResultKind.Scalar, 3)]
    public void Result_kind_values_are_stable(TelemetryResultKind kind, int expected)
    {
        Assert.That((int)kind, Is.EqualTo(expected));
    }

    [TestCase(TelemetryMeasurementSemantic.Unspecified, 0)]
    [TestCase(TelemetryMeasurementSemantic.PerOperation, 1)]
    [TestCase(TelemetryMeasurementSemantic.PerRecord, 2)]
    [TestCase(TelemetryMeasurementSemantic.PerBatch, 3)]
    [TestCase(TelemetryMeasurementSemantic.Duration, 4)]
    [TestCase(TelemetryMeasurementSemantic.Level, 5)]
    [TestCase(TelemetryMeasurementSemantic.Ratio, 6)]
    public void Measurement_semantic_values_are_stable(TelemetryMeasurementSemantic semantic, int expected)
    {
        Assert.That((int)semantic, Is.EqualTo(expected));
    }

    [TestCase(TelemetryBoundsViolation.None, 0)]
    [TestCase(TelemetryBoundsViolation.RangeNotAscending, 1)]
    [TestCase(TelemetryBoundsViolation.StepBelowMinimum, 2)]
    [TestCase(TelemetryBoundsViolation.StepAboveMaximum, 3)]
    [TestCase(TelemetryBoundsViolation.RangeTooLong, 4)]
    [TestCase(TelemetryBoundsViolation.LookbackTooOld, 5)]
    [TestCase(TelemetryBoundsViolation.TooManyPoints, 6)]
    public void Bounds_violation_values_are_stable(TelemetryBoundsViolation violation, int expected)
    {
        Assert.That((int)violation, Is.EqualTo(expected));
    }

    [TestCase(TelemetryQueryParameters.None, 0)]
    [TestCase(TelemetryQueryParameters.TimeRange, 1)]
    [TestCase(TelemetryQueryParameters.Step, 2)]
    [TestCase(TelemetryQueryParameters.TreeFilter, 4)]
    public void Query_parameter_flags_are_stable_and_non_overlapping(
        TelemetryQueryParameters parameter,
        int expected)
    {
        Assert.That((int)parameter, Is.EqualTo(expected));
    }

    [Test]
    public void Query_parameters_is_a_flags_enum()
    {
        Assert.That(typeof(TelemetryQueryParameters).GetCustomAttribute<FlagsAttribute>(), Is.Not.Null);
    }

    [Test]
    public void Measurement_semantic_separates_per_operation_from_per_record()
    {
        Assert.That(TelemetryMeasurementSemantic.PerOperation,
            Is.Not.EqualTo(TelemetryMeasurementSemantic.PerRecord),
            "The per-operation versus per-record distinction is the whole point of the semantic: a "
            + "panel title cannot drift from its instrument if the two are distinguishable.");
    }
}
