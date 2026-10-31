using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Trees = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Trees;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>One record of a pinned rule: the rule id written on one tree with one operation set.</summary>
/// <param name="RuleId">The rule id, shared by every record of one named rule.</param>
/// <param name="Subject">Who the rule grants.</param>
/// <param name="TreeId">The one tree the record governs (<see cref="LatticeScope.ClusterWideTreeId"/> for a scopeless capability).</param>
/// <param name="Operations">What the record allows.</param>
public sealed record PinnedRule(string RuleId, LatticeSubjectSelector Subject, string TreeId, LatticeOperation Operations)
{
    /// <summary>The rule as the Auth API stores it: an allow over the whole tree.</summary>
    public LatticeAuthorizationRule ToRule() =>
        new(RuleId, Subject, LatticeScope.Tree(TreeId), Operations, LatticeEffect.Allow);
}

/// <summary>A pinned group and its members.</summary>
/// <param name="GroupId">The group id.</param>
/// <param name="DisplayName">Its display name.</param>
/// <param name="Users">Its user members.</param>
/// <param name="Groups">Its group members (a nested group).</param>
public sealed record PinnedGroup(string GroupId, string DisplayName, IReadOnlyList<string> Users, IReadOnlyList<string> Groups);

/// <summary>
/// The Contoso access model, pinned (#5364). An authorization rule governs exactly one tree:
/// the Auth API has no tree pattern, and a rule's condition is never evaluated (#5426). So
/// where a journey's prose says a rule grants something "on <c>factory-*</c>", it means the
/// same rule id written once per tree in this table. The A2 and A9 journeys author exactly
/// these records through the Explorer, and the F6 seeders write them through the Auth API, so
/// both paths produce one world.
/// </summary>
public static class ContosoAccess
{
    /// <summary>Data-plane reads: one key and a range.</summary>
    public const LatticeOperation ReadOps = LatticeOperation.Read | LatticeOperation.RangeRead;

    /// <summary>Data-plane reads and point writes.</summary>
    public const LatticeOperation EditOps = ReadOps | LatticeOperation.Write | LatticeOperation.Delete;

    /// <summary>A2's groups, in the order the journey creates them.</summary>
    public static IReadOnlyList<PinnedGroup> A2Groups { get; } =
    [
        new(StoryNames.Contoso.Groups.DataStewards, "Data stewards", [Personas.Priya.Id], []),
        new(StoryNames.Contoso.Groups.SchemaOwners, "Schema owners", [Personas.Sam.Id], []),
        new(StoryNames.Contoso.Groups.Auditors, "Auditors", [Personas.Dana.Id], []),
        new(StoryNames.Contoso.Groups.IngestionServices, "Ingestion services", [Personas.LineFeed.Id], []),
    ];

    /// <summary>
    /// A2's four rules, one record per governed tree:
    /// <list type="bullet">
    /// <item><c>stewards-factory</c> (group <c>data-stewards</c>): edit, bulk load and administer the orders;
    /// edit the telemetry and the lines; create, load and delete the archive and the history trees
    /// (tree lifecycle there only, so resize is never offered on the orders); administer the
    /// <c>order-status</c> tag index's tree.</item>
    /// <item><c>schema-owners-factory</c> (group <c>schema-owners</c>): read and administer the schema of the
    /// orders, the lines and the archive.</item>
    /// <item><c>auditors-factory-read</c> (group <c>auditors</c>): read the orders, the lines and the archive,
    /// and the scopeless Telemetry capability. Not the telemetry tree: <c>dana</c> reads that only
    /// through the nested <c>factory-staff</c> group (A9).</item>
    /// <item><c>line-feed-factory</c> (group <c>ingestion-services</c>): write the telemetry (its first
    /// write creates the tree) and the orders.</item>
    /// </list>
    /// </summary>
    public static IReadOnlyList<PinnedRule> A2Rules { get; } =
    [
        Group(StoryNames.Contoso.Rules.StewardsFactory, StoryNames.Contoso.Groups.DataStewards, Trees.FactoryOrders,
            EditOps | LatticeOperation.BulkLoad | LatticeOperation.Admin),
        Group(StoryNames.Contoso.Rules.StewardsFactory, StoryNames.Contoso.Groups.DataStewards, Trees.FactoryTelemetry, EditOps),
        Group(StoryNames.Contoso.Rules.StewardsFactory, StoryNames.Contoso.Groups.DataStewards, Trees.FactoryLines, EditOps),
        Group(StoryNames.Contoso.Rules.StewardsFactory, StoryNames.Contoso.Groups.DataStewards, Trees.FactoryOrdersArchive,
            EditOps | LatticeOperation.BulkLoad | LatticeOperation.Admin | LatticeOperation.TreeLifecycle),
        Group(StoryNames.Contoso.Rules.StewardsFactory, StoryNames.Contoso.Groups.DataStewards, Trees.FactoryHistory,
            EditOps | LatticeOperation.BulkLoad | LatticeOperation.Admin | LatticeOperation.TreeLifecycle),
        Group(StoryNames.Contoso.Rules.StewardsFactory, StoryNames.Contoso.Groups.DataStewards, OrderStatusIndexTree,
            ReadOps | LatticeOperation.Admin),

        Group(StoryNames.Contoso.Rules.SchemaOwnersFactory, StoryNames.Contoso.Groups.SchemaOwners, Trees.FactoryOrders, ReadOps | LatticeOperation.SchemaAdmin),
        Group(StoryNames.Contoso.Rules.SchemaOwnersFactory, StoryNames.Contoso.Groups.SchemaOwners, Trees.FactoryLines, ReadOps | LatticeOperation.SchemaAdmin),
        Group(StoryNames.Contoso.Rules.SchemaOwnersFactory, StoryNames.Contoso.Groups.SchemaOwners, Trees.FactoryOrdersArchive, ReadOps | LatticeOperation.SchemaAdmin),

        Group(StoryNames.Contoso.Rules.AuditorsFactoryRead, StoryNames.Contoso.Groups.Auditors, Trees.FactoryOrders, ReadOps),
        Group(StoryNames.Contoso.Rules.AuditorsFactoryRead, StoryNames.Contoso.Groups.Auditors, Trees.FactoryLines, ReadOps),
        Group(StoryNames.Contoso.Rules.AuditorsFactoryRead, StoryNames.Contoso.Groups.Auditors, Trees.FactoryOrdersArchive, ReadOps),
        Group(StoryNames.Contoso.Rules.AuditorsFactoryRead, StoryNames.Contoso.Groups.Auditors, LatticeScope.ClusterWideTreeId, LatticeOperation.Telemetry),

        Group(StoryNames.Contoso.Rules.LineFeedFactory, StoryNames.Contoso.Groups.IngestionServices, Trees.FactoryTelemetry, ReadOps | LatticeOperation.Write),
        Group(StoryNames.Contoso.Rules.LineFeedFactory, StoryNames.Contoso.Groups.IngestionServices, Trees.FactoryOrders, ReadOps | LatticeOperation.Write),
    ];

    /// <summary>A9's nested group: <c>factory-staff</c> holds the stewards and the auditors.</summary>
    public static PinnedGroup FactoryStaff { get; } = new(
        StoryNames.Contoso.Groups.FactoryStaff,
        "Factory staff",
        [],
        [StoryNames.Contoso.Groups.DataStewards, StoryNames.Contoso.Groups.Auditors]);

    /// <summary>A9's rule: <c>factory-staff</c> reads the telemetry.</summary>
    public static PinnedRule FactoryStaffTelemetryRead { get; } = Group(
        StoryNames.Contoso.Rules.FactoryStaffTelemetryRead, StoryNames.Contoso.Groups.FactoryStaff, Trees.FactoryTelemetry, ReadOps);

    /// <summary>
    /// A5's group the task board's viewer role binds to: the auditors and the schema owners,
    /// nested. An app role binds to exactly one group, so the two teams that view the board are
    /// gathered in one group rather than bound twice.
    /// </summary>
    public static PinnedGroup TaskBoardViewers { get; } = new(
        "task-board-viewers",
        "Task board viewers",
        [],
        [StoryNames.Contoso.Groups.Auditors, StoryNames.Contoso.Groups.SchemaOwners]);

    /// <summary>The tree backing the line feed's <c>order-status</c> tag index.</summary>
    public const string OrderStatusIndexTree = "tag-" + StoryNames.Contoso.TagIndexes.OrderStatus;

    /// <summary>
    /// The id prefix of every scratch rule a journey authors and deletes (A2.6, A9.4, A9.5), so a
    /// verifier can assert none remains.
    /// </summary>
    public const string ScratchRulePrefix = "scratch-";

    /// <summary>The A2 records of one named rule.</summary>
    /// <param name="ruleId">The rule id.</param>
    public static IEnumerable<PinnedRule> A2RulesNamed(string ruleId) => A2Rules.Where(rule => rule.RuleId == ruleId);

    private static PinnedRule Group(string ruleId, string groupId, string treeId, LatticeOperation operations) =>
        new(ruleId, LatticeSubjectSelector.Group(groupId), treeId, operations);
}
