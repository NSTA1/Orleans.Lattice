using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the enum-parsing surface of <see cref="BackupToolMappings"/> -
/// the point where a wire-supplied <c>scopeKind</c> or restore <c>mode</c> string
/// is reconstructed into its domain enum before a backup or restore is dispatched.
/// </summary>
/// <remarks>
/// The documented contract is that an unrecognised value is rejected with an
/// <see cref="ArgumentException"/> naming the parameter. A numeric string is the
/// gap that regressed: <see cref="Enum.TryParse{TEnum}(string, bool, out TEnum)"/>
/// happily binds <c>"99"</c> to the undefined enum value <c>(TEnum)99</c> and
/// returns <c>true</c>, so without an <see cref="Enum.IsDefined{TEnum}(TEnum)"/>
/// guard an out-of-range ordinal slipped past validation and reached the facade as
/// a garbage mode/scope. These tests pin the guard: a defined name still parses, an
/// unknown name is rejected, and an out-of-range numeric ordinal is rejected too.
/// The <see cref="Enum.IsDefined{TEnum}(TEnum)"/> guard alone was insufficient,
/// because it rejects only an <em>out-of-range</em> ordinal: an <em>in-range</em>
/// numeric string (<c>"1"</c> binds to the defined <c>ShadowCutover</c>/<c>Prefix</c>)
/// and a comma-combined name that <see cref="Enum.TryParse{TEnum}(string, bool, out TEnum)"/>
/// treats as a bitwise OR (<c>"InPlace,ShadowCutover"</c> folds onto <c>ShadowCutover</c>)
/// both still slipped past it. The ordinal round-trip name-equality check the sibling
/// <c>ReplicationToolMappings.ToMergeMode</c> uses now rejects both, and these tests
/// pin that too.
/// </remarks>
[TestFixture]
public sealed class BackupToolMappingsTests
{
    // ---- restore mode ------------------------------------------------------

    [TestCase("InPlace", LatticeRestoreMode.InPlace)]
    [TestCase("inplace", LatticeRestoreMode.InPlace)]
    [TestCase("ShadowCutover", LatticeRestoreMode.ShadowCutover)]
    [TestCase("shadowcutover", LatticeRestoreMode.ShadowCutover)]
    public void ToRestoreMode_parses_a_defined_mode_name_case_insensitively(string mode, LatticeRestoreMode expected)
        => Assert.That(BackupToolMappings.ToRestoreMode(mode), Is.EqualTo(expected));

    [TestCase(null)]
    [TestCase("")]
    public void ToRestoreMode_defaults_to_in_place_when_absent(string? mode)
        => Assert.That(BackupToolMappings.ToRestoreMode(mode), Is.EqualTo(LatticeRestoreMode.InPlace));

    [Test]
    public void ToRestoreMode_rejects_an_unrecognised_name()
        => Assert.That(
            () => BackupToolMappings.ToRestoreMode("Sideways"),
            Throws.ArgumentException.With.Message.Contains("Sideways"));

    [TestCase("99")]
    [TestCase("-1")]
    [TestCase("2147483647")]
    public void ToRestoreMode_rejects_an_out_of_range_numeric_ordinal(string mode)
        => Assert.That(
            () => BackupToolMappings.ToRestoreMode(mode),
            Throws.ArgumentException,
            "A numeric string that is not a defined LatticeRestoreMode must be rejected, not bound to (LatticeRestoreMode)n.");

    [TestCase("0")]
    [TestCase("1")]
    public void ToRestoreMode_rejects_an_in_range_numeric_ordinal(string mode)
        => Assert.That(
            () => BackupToolMappings.ToRestoreMode(mode),
            Throws.ArgumentException,
            "An in-range numeric string names no mode; Enum.IsDefined accepts (LatticeRestoreMode)1, so a round-trip name check must reject it.");

    [TestCase("InPlace,ShadowCutover")]
    [TestCase("inplace,shadowcutover")]
    public void ToRestoreMode_rejects_a_comma_combined_name(string mode)
        => Assert.That(
            () => BackupToolMappings.ToRestoreMode(mode),
            Throws.ArgumentException,
            "Enum.TryParse folds a comma-combined name as a bitwise OR onto a defined member; a round-trip name check must reject it.");

    // ---- scope kind --------------------------------------------------------

    [TestCase(null)]
    [TestCase("")]
    public void ToScope_defaults_to_whole_tree_when_kind_absent(string? kind)
    {
        var scope = BackupToolMappings.ToScope("orders", kind, keyOrPrefix: null);

        Assert.That(scope.Kind, Is.EqualTo(BackupScopeKind.WholeTree));
    }

    [Test]
    public void ToScope_parses_a_defined_kind_name()
    {
        var scope = BackupToolMappings.ToScope("orders", "Prefix", keyOrPrefix: "p");

        Assert.Multiple(() =>
        {
            Assert.That(scope.Kind, Is.EqualTo(BackupScopeKind.Prefix));
            Assert.That(scope.KeyOrPrefix, Is.EqualTo("p"));
        });
    }

    [Test]
    public void ToScope_rejects_an_unrecognised_kind_name()
        => Assert.That(
            () => BackupToolMappings.ToScope("orders", "Everything", keyOrPrefix: null),
            Throws.ArgumentException.With.Message.Contains("Everything"));

    [TestCase("99")]
    [TestCase("-1")]
    [TestCase("2147483647")]
    public void ToScope_rejects_an_out_of_range_numeric_ordinal(string kind)
        => Assert.That(
            () => BackupToolMappings.ToScope("orders", kind, keyOrPrefix: null),
            Throws.ArgumentException,
            "A numeric string that is not a defined BackupScopeKind must be rejected, not silently treated as WholeTree.");

    [TestCase("0")]
    [TestCase("1")]
    [TestCase("2")]
    public void ToScope_rejects_an_in_range_numeric_ordinal(string kind)
        => Assert.That(
            () => BackupToolMappings.ToScope("orders", kind, keyOrPrefix: null),
            Throws.ArgumentException,
            "An in-range numeric string names no kind; Enum.IsDefined accepts (BackupScopeKind)1, so a round-trip name check must reject it.");

    [TestCase("WholeTree,Prefix")]
    [TestCase("wholetree,prefix")]
    public void ToScope_rejects_a_comma_combined_name(string kind)
        => Assert.That(
            () => BackupToolMappings.ToScope("orders", kind, keyOrPrefix: null),
            Throws.ArgumentException,
            "Enum.TryParse folds a comma-combined name as a bitwise OR onto a defined member; a round-trip name check must reject it.");

    [TestCase(null, BackupSetMembershipMode.Explicit)]
    [TestCase("", BackupSetMembershipMode.Explicit)]
    [TestCase("explicit", BackupSetMembershipMode.Explicit)]
    [TestCase("AllTrees", BackupSetMembershipMode.AllTrees)]
    public void ToMembership_parses_a_defined_name_case_insensitively(string? membership, BackupSetMembershipMode expected)
        => Assert.That(BackupToolMappings.ToMembership(membership), Is.EqualTo(expected));

    [TestCase("1")]
    [TestCase("Explicit,AllTrees")]
    [TestCase("all-trees")]
    public void ToMembership_rejects_an_ordinal_combined_or_unknown_name(string membership)
        => Assert.That(() => BackupToolMappings.ToMembership(membership), Throws.ArgumentException);

    [Test]
    public void ToMcp_projects_a_backup_set_schedule_with_its_runs_and_retained_sets()
    {
        var orders = BackupScopeSelector.WholeTree("orders");
        var request = LatticeBackupSetScheduleRequest.ForTrees(
            "nightly", new[] { orders }, TimeSpan.FromMinutes(30), retentionKeepLast: 2, retentionMaxAge: TimeSpan.FromHours(6));
        var status = new BackupSetScheduleStatus(
            "nightly",
            request,
            new[] { orders },
            isScheduled: true,
            nextRunUtc: DateTimeOffset.UnixEpoch.AddHours(1),
            isRunning: true,
            registeredAtUtc: DateTimeOffset.UnixEpoch,
            lastRunUtc: DateTimeOffset.UnixEpoch.AddMinutes(30),
            lastSuccessUtc: null,
            BackupScopeRunOutcome.Failure,
            lastFailureReason: "sink down",
            consecutiveFailures: 2,
            new[]
            {
                new BackupSetScheduleRun(
                    DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddSeconds(5), BackupSetScheduleTrigger.Manual,
                    BackupScopeRunOutcome.Success, "set-1", memberCount: 1, attempts: 1, failureReason: null),
            },
            new[]
            {
                new ScheduledBackupSet(
                    "set-1", "nightly", DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch, crossTreeConsistent: true,
                    new[] { new ScheduledBackupSetMember(BackupScopeSelector.Prefix("orders", "eu/"), "bk-1") }),
            },
            new BackupSetScheduleRunProgress(
                DateTimeOffset.UnixEpoch, BackupSetScheduleTrigger.Schedule, attempt: 2,
                BackupOperationPhases.WritingMembers, completedUnits: 1, totalUnits: 3,
                BackupOperationUnits.Members, "orders", DateTimeOffset.UnixEpoch.AddSeconds(9)));

        var mcp = BackupToolMappings.ToMcp(status);

        Assert.Multiple(() =>
        {
            Assert.That(mcp.Found, Is.True);
            Assert.That(mcp.IntervalSeconds, Is.EqualTo(1800));
            Assert.That(mcp.RetentionKeepLast, Is.EqualTo(2));
            Assert.That(mcp.RetentionMaxAgeSeconds, Is.EqualTo(21600));
            Assert.That(mcp.IsRunning, Is.True);
            Assert.That(mcp.LastOutcome, Is.EqualTo("Failure"));
            Assert.That(mcp.LastFailureReason, Is.EqualTo("sink down"));
            Assert.That(mcp.ConsecutiveFailures, Is.EqualTo(2));
            Assert.That(mcp.RecentRuns.Single().Trigger, Is.EqualTo("Manual"));
            Assert.That(mcp.RecentRuns.Single().SetId, Is.EqualTo("set-1"));
            var member = mcp.RetainedSets.Single().Members.Single();
            Assert.That((member.TreeId, member.ScopeKind, member.KeyOrPrefix, member.BackupId), Is.EqualTo(("orders", "Prefix", "eu/", "bk-1")));
            var current = mcp.CurrentRun!;
            Assert.That(
                (current.Trigger, current.Attempt, current.Phase, current.CompletedUnits, current.TotalUnits, current.UnitName, current.CurrentTreeId),
                Is.EqualTo(("Schedule", 2, "WritingMembers", 1L, (long?)3, "members", "orders")));
        });
    }

    [Test]
    public void ToMcp_maps_a_missing_backup_set_schedule_to_not_found()
        => Assert.That(BackupToolMappings.ToMcp((BackupSetScheduleStatus?)null).Found, Is.False);
}
