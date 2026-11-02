using System.Globalization;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// The off-UI actors of the Contoso story (#5364): the corporate directory sync, the
/// policy-as-code pipeline, and the compressed backup history. A journey whose contract names
/// one of these as a consume ("off-UI, harness") calls it before its first step, exactly as the
/// chapter's seeder does, so the journey and the seeder start from one world. Each is
/// idempotent.
/// </summary>
public static class ContosoHarness
{
    /// <summary>The rule id the pipeline authors with a condition, which the Explorer shows read-only (A9, #5426).</summary>
    public const string ConditionalLegacyRule = "factory-conditional-legacy";

    /// <summary>The scratch tree <see cref="ConditionalLegacyRule"/> governs.</summary>
    public const string ConditionalLegacyTree = "factory-legacy";

    /// <summary>The condition <see cref="ConditionalLegacyRule"/> carries; never evaluated.</summary>
    public const string ConditionalLegacyCondition = "request.shift == 'night'";

    /// <summary>The name prefix of A10's historical telemetry backups.</summary>
    public const string HistoricalBackupPrefix = "telemetry-history-";

    /// <summary>The <paramref name="number"/>th directory-synced group's display name.</summary>
    /// <param name="number">From 1 to 230.</param>
    public static string SyncedTeamName(int number) => string.Create(CultureInfo.InvariantCulture, $"Org team {number:D3}");

    /// <summary>The <paramref name="number"/>th pipeline rule's id: <c>org-team-001-read</c> to <c>org-team-229-read</c>.</summary>
    /// <param name="number">From 1 to 229.</param>
    public static string SyncedRuleId(int number) => Names.Groups.SyncedTeam(number) + "-read";

    /// <summary>The tree the <paramref name="number"/>th pipeline rule governs.</summary>
    /// <param name="number">From 1 to 229.</param>
    public static string SyncedRuleTree(int number) => Names.Groups.SyncedTeam(number) + "-workspace";

    /// <summary>
    /// A2's consume: the corporate directory sync creates the 230 team groups
    /// <c>org-team-001</c> to <c>org-team-230</c> as stored cluster groups, as a sync job does.
    /// </summary>
    /// <param name="estate">The Contoso estate.</param>
    public static async Task SyncDirectoryGroupsAsync(JourneyEstate estate)
    {
        var auth = ContractFacades.Of(estate).Auth;
        var existing = (await ContractCalls.AllGroupsAsync(auth)).Select(group => group.GroupId).ToHashSet(StringComparer.Ordinal);
        foreach (var number in Enumerable.Range(1, Names.Groups.SyncedTeamCount))
        {
            var id = Names.Groups.SyncedTeam(number);
            if (!existing.Contains(id))
            {
                await auth.UpsertGroupAsync(new AuthGroup { GroupId = id, DisplayName = SyncedTeamName(number) });
            }
        }
    }

    /// <summary>
    /// A9's consume: the policy-as-code pipeline syncs 230 rules - 229 team reads, each on its
    /// team's workspace tree, and <see cref="ConditionalLegacyRule"/>, an older rule carrying a
    /// condition the cluster never evaluates.
    /// </summary>
    /// <param name="estate">The Contoso estate.</param>
    public static async Task SyncPolicyRulesAsync(JourneyEstate estate)
    {
        var auth = ContractFacades.Of(estate).Auth;
        var existing = (await ContractCalls.AllRulesAsync(auth)).Select(rule => (rule.Scope.TreeId, rule.RuleId)).ToHashSet();
        foreach (var number in Enumerable.Range(1, Names.Rules.SyncedRuleCount - 1))
        {
            if (!existing.Contains((SyncedRuleTree(number), SyncedRuleId(number))))
            {
                await auth.PutRuleAsync(new AuthPutRule
                {
                    Rule = new LatticeAuthorizationRule(
                        SyncedRuleId(number),
                        LatticeSubjectSelector.Group(Names.Groups.SyncedTeam(number)),
                        LatticeScope.Tree(SyncedRuleTree(number)),
                        ContosoAccess.ReadOps,
                        LatticeEffect.Allow),
                });
            }
        }

        await auth.PutRuleAsync(new AuthPutRule { Rule = ConditionalLegacy });
    }

    /// <summary>The pipeline's conditional rule.</summary>
    public static LatticeAuthorizationRule ConditionalLegacy { get; } = new(
        ConditionalLegacyRule,
        LatticeSubjectSelector.Group(Names.Groups.SyncedTeam(Names.Groups.SyncedTeamCount)),
        LatticeScope.Tree(ConditionalLegacyTree),
        ContosoAccess.ReadOps,
        LatticeEffect.Allow,
        ConditionalLegacyCondition);

    /// <summary>
    /// A10's consume: 60 historical full backups of <c>factory-telemetry</c>, named
    /// <c>telemetry-history-001</c> to <c>telemetry-history-060</c>, as an hourly schedule would
    /// have left them over the previous days (the cadence is compressed to back-to-back captures).
    /// </summary>
    /// <param name="estate">The Contoso estate.</param>
    public static async Task SeedHistoricalBackupsAsync(JourneyEstate estate)
    {
        var backup = ContractFacades.Of(estate).Backup;
        var existing = await HistoricalNamesAsync(backup);
        var number = 0;
        while (existing.Count < Names.Backups.HistoricalCount)
        {
            number++;
            if (number > Names.Backups.HistoricalCount * 2)
            {
                throw new InvalidOperationException($"Only {existing.Count} historical backups are catalogued after {number - 1} captures.");
            }

            var name = HistoricalBackupName(number);
            if (existing.Contains(name))
            {
                continue;
            }

            var handle = await backup.StartBackupAsync(new LatticeBackupCaptureRequest(name, BackupScopeSelector.WholeTree(Names.Trees.FactoryTelemetry)));
            await ContractCalls.CapturedBackupIdAsync(backup, handle.OperationId, $"the historical capture {name}");
            existing.Add(name);
            if (existing.Count == Names.Backups.HistoricalCount)
            {
                // A capture whose content another backup already holds is catalogued under the
                // earlier name, so count what the catalogue lists, once it has settled, and top up.
                await Task.Delay(TimeSpan.FromSeconds(5));
                existing = await HistoricalNamesAsync(backup);
            }
        }
    }

    private static async Task<HashSet<string>> HistoricalNamesAsync(Orleans.Lattice.Api.Backup.Grpc.LatticeBackupApiGrpcClient backup) =>
        (await ContractCalls.BackupsAsync(backup, Names.Trees.FactoryTelemetry))
            .Select(manifest => manifest.Name)
            .Where(name => name.StartsWith(HistoricalBackupPrefix, StringComparison.Ordinal))
            .ToHashSet(StringComparer.Ordinal);

    /// <summary>The <paramref name="number"/>th historical backup's name.</summary>
    /// <param name="number">From 1 to 60.</param>
    public static string HistoricalBackupName(int number) =>
        string.Create(CultureInfo.InvariantCulture, $"{HistoricalBackupPrefix}{number:D3}");

    /// <summary>
    /// U6's consume: the policy-as-code pipeline sets a policy on <c>factory-lines</c> holding one
    /// rule the rule builder cannot represent (<see cref="ContosoSchema.LinesPipelineRule"/>).
    /// Leaves a policy that already holds the rule alone.
    /// </summary>
    /// <param name="estate">The Contoso estate.</param>
    public static async Task AuthorLinesPipelineRuleAsync(JourneyEstate estate)
    {
        var schema = ContractFacades.Of(estate).Schema;
        var current = await schema.GetPolicyAsync(Names.Trees.FactoryLines);
        if (!ContosoSchema.Holds(current, ContosoSchema.LinesPipelineRule))
        {
            await schema.SetPolicyAsync(Names.Trees.FactoryLines, ContosoSchema.LinesPipeline);
        }
    }
}
