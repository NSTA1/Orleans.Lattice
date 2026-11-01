namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U5;

/// <summary>
/// One area's palette commands as the palette titles them, for the U5 palette walk (#5387),
/// and the catalogue use case a walk of them proves, if U5 owns one.
/// </summary>
/// <param name="Area">The area key, which prefixes every command id it contributes (<c>schema.</c>).</param>
/// <param name="DisplayName">The area's name on the spine.</param>
/// <param name="Commands">
/// The titles the area contributes at Home to a caller who may use it, or <see langword="null"/>
/// when what it contributes depends on rights the walk does not pin (the walk then requires the
/// area to be withheld from sam).
/// </param>
/// <param name="UseCase">The catalogue use case its walk proves, or <see langword="null"/>.</param>
public sealed record AreaCommands(string Area, string DisplayName, IReadOnlyList<string>? Commands, string? UseCase = null)
{
    /// <summary>
    /// Every area that contributes palette commands, in directory order, with what each
    /// contributes at Home. Apps contributes only to an <c>AppInstall</c> holder, which sam is
    /// not, so it contributes nothing to him though its stop is open.
    /// </summary>
    public static IReadOnlyList<AreaCommands> All { get; } =
    [
        new("data", "Data", ["Refresh the tree directory"]),
        new("apps", "Apps", []),
        new("access", "Access", ["Explain access...", "Create an access rule", "Create a group"], "use-case:access/palette"),
        new("schema", "Schema", ["Scan compliance...", "Show every tree's schema"], "use-case:schema/palette"),
        new("tenancy", "Tenancy", null),
        new("replication", "Replication", ["Refresh replication status", "Show enrolled trees"]),
        new("backups", "Backups", ["Capture backup..."], "use-case:backups/palette"),
        new("telemetry", "Telemetry", ["Refresh telemetry"]),
        new("cluster", "Cluster", ["Reshard tree...", "Plan WAL move...", "New tree..."]),
    ];
}
