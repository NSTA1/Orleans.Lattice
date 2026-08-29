namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Centralized Orleans serialization alias constants for the telemetry
/// control-API surface (the transport-agnostic contract in
/// <c>Orleans.Lattice.Api.Abstractions</c> and the sibling gRPC / MCP bindings
/// that reuse this registry). Mirrors the sibling <c>ApiTenantAdminTypeAliases</c>
/// / <c>ApiTreeAdminTypeAliases</c> tables: every constant must use the reserved
/// <c>oitl.</c> prefix, stay within seven characters, and be unique.
/// <para>
/// The <c>oitl.</c> prefix namespace keeps the telemetry control-API DTO types
/// from colliding with the core (<c>ol.</c>), the tree-admin control-API
/// (<c>oit.</c>), the tenant-admin control-API (<c>oitn.</c>), or the tenancy
/// engine (<c>olt.</c>) namespaces. It is deliberately neither a prefix of, nor
/// prefixed by, any other reserved namespace, so each package's alias-hygiene
/// audit still partitions the alias space exactly. New serializable types append
/// new <c>oitl.</c>-prefixed constants here.
/// </para>
/// <para>
/// An alias is wire format. A constant here may be added, but never renamed,
/// re-pointed at a different type, or removed.
/// </para>
/// </summary>
public static class ApiTelemetryTypeAliases
{
    /// <summary>
    /// The reserved alias prefix owned by the telemetry control-API surface.
    /// Every alias constant added here must start with this value.
    /// </summary>
    public const string AliasPrefix = "oitl.";

    /// <summary>Alias for <see cref="Telemetry.TelemetryQueryCatalog"/>.</summary>
    public const string TelemetryQueryCatalog = "oitl.ct";

    /// <summary>Alias for <see cref="Telemetry.TelemetryQueryDescriptor"/>.</summary>
    public const string TelemetryQueryDescriptor = "oitl.qd";

    /// <summary>Alias for <see cref="Telemetry.TelemetryQueryDefinition"/>.</summary>
    public const string TelemetryQueryDefinition = "oitl.qf";

    /// <summary>Alias for <see cref="Telemetry.TelemetryQueryBounds"/>.</summary>
    public const string TelemetryQueryBounds = "oitl.qb";

    /// <summary>Alias for <see cref="Telemetry.TelemetryQueryParameters"/>.</summary>
    public const string TelemetryQueryParameters = "oitl.qp";

    /// <summary>Alias for <see cref="Telemetry.TelemetryQueryKind"/>.</summary>
    public const string TelemetryQueryKind = "oitl.qk";

    /// <summary>Alias for <see cref="Telemetry.TelemetryInstrumentReference"/>.</summary>
    public const string TelemetryInstrumentReference = "oitl.ir";

    /// <summary>Alias for <see cref="Telemetry.TelemetryMeasurementSemantic"/>.</summary>
    public const string TelemetryMeasurementSemantic = "oitl.ms";

    /// <summary>Alias for <see cref="Telemetry.TelemetryQueryRequest"/>.</summary>
    public const string TelemetryQueryRequest = "oitl.rq";

    /// <summary>Alias for <see cref="Telemetry.TelemetryQueryResponse"/>.</summary>
    public const string TelemetryQueryResponse = "oitl.rs";

    /// <summary>Alias for <see cref="Telemetry.TelemetryTimeRange"/>.</summary>
    public const string TelemetryTimeRange = "oitl.tr";

    /// <summary>Alias for <see cref="Telemetry.TelemetryTimeSeries"/>.</summary>
    public const string TelemetryTimeSeries = "oitl.ts";

    /// <summary>Alias for <see cref="Telemetry.TelemetryDataPoint"/>.</summary>
    public const string TelemetryDataPoint = "oitl.dp";

    /// <summary>Alias for <see cref="Telemetry.TelemetryLabel"/>.</summary>
    public const string TelemetryLabel = "oitl.lb";

    /// <summary>Alias for <see cref="Telemetry.TelemetryTenantScope"/>.</summary>
    public const string TelemetryTenantScope = "oitl.sc";

    /// <summary>Alias for <see cref="Telemetry.TelemetryTenantVisibility"/>.</summary>
    public const string TelemetryTenantVisibility = "oitl.tv";

    /// <summary>Alias for <see cref="Telemetry.TelemetryResultKind"/>.</summary>
    public const string TelemetryResultKind = "oitl.rk";

    /// <summary>Alias for <see cref="Telemetry.TelemetryBoundsViolation"/>.</summary>
    public const string TelemetryBoundsViolation = "oitl.bv";
}
