using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// The single guard standing between a future tidy-up and a container that
/// reports healthy backups and takes none.
/// <para>
/// <c>lattice.backup</c> ships a reminder-driven scheduler grain, and enabling
/// its two schedule flags is the obvious way to get an hourly cadence - obvious
/// enough that somebody will eventually delete
/// <c>RepoContextBackupService</c> in favour of it. They must not, until issue
/// #2608 is fixed. A reminder tick carries no ambient credential, so the
/// fail-closed capture authorizer resolves the caller as the anonymous subject;
/// this host registers a real access gate whose default effect is Deny, so every
/// reminder-driven capture is refused. Crucially the refusal is <b>invisible
/// through the scheduler</b>: <c>HasScheduleAsync</c> and every
/// <c>BackupSchedulerRuntimeStatus</c> registration flag keep reporting healthy,
/// because registration and capture are different facts and only the first is
/// observable there.
/// </para>
/// <para>
/// So the arrangement is asserted against the <b>resolved options</b> rather than
/// left as a comment. When #2608 lands, this fixture is the thing to revisit -
/// deliberately, with the reminder path proven under a gated host first.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextBackupScheduleGuardTests
{
    /// <summary>
    /// A minimal <see cref="ISiloBuilder"/> over a plain service collection, so the
    /// production registration path can be run and its options resolved without
    /// standing up a silo.
    /// </summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }

    /// <summary>Satisfies the add-on ordering guard, which probes for what AddLattice installs.</summary>
    private sealed class NoOpLatticeOptionsValidator : IValidateOptions<LatticeOptions>
    {
        public ValidateOptionsResult Validate(string? name, LatticeOptions options) =>
            ValidateOptionsResult.Success;
    }

    private static LatticeBackupScheduleOptions ResolveMemoryScopeSchedule()
    {
        var settings = RepoContextBackup.Resolve(
            new ConfigurationBuilder()
                .AddInMemoryCollection(
                [
                    new KeyValuePair<string, string?>(
                        RepoContextBackup.BlobConnectionStringKey, "UseDevelopmentStorage=true"),
                ])
                .Build());

        var silo = new FakeSiloBuilder();
        silo.Services.AddSingleton<IValidateOptions<LatticeOptions>, NoOpLatticeOptionsValidator>();
        silo.Services.AddOptions();

        silo.ConfigureRepoContextBackup(settings);

        return silo.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptionsMonitor<LatticeBackupScheduleOptions>>()
            .Get(RepoContextBackup.MemoryScopeKey);
    }

    [Test]
    public void The_reminder_driven_full_backup_schedule_stays_disabled()
    {
        Assert.That(
            ResolveMemoryScopeSchedule().FullBackupScheduleEnabled,
            Is.False,
            "See issue #2608. A reminder-driven capture runs with no ambient credential and is denied by this "
                + "host's default-deny access gate, while the schedule keeps reporting itself registered. "
                + "Enabling this flag produces a container that reports healthy backups and takes none. The "
                + "cadence is owned by RepoContextBackupService, which captures inside an explicit credential "
                + "scope.");
    }

    [Test]
    public void The_reminder_driven_incremental_backup_schedule_stays_disabled()
    {
        Assert.That(
            ResolveMemoryScopeSchedule().IncrementalBackupScheduleEnabled,
            Is.False,
            "See issue #2608, and the sibling assertion on the full schedule.");
    }

    [Test]
    public void Retention_is_enabled_so_the_sink_does_not_grow_without_bound()
    {
        var options = ResolveMemoryScopeSchedule();

        // Retention is safe on the reminder path in a way capture is not: pruning is
        // driven from the host's own credential scope alongside the captures.
        Assert.That(options.RetentionEnabled, Is.True);
        Assert.That(options.RetentionKeepLast, Is.EqualTo(RepoContextBackup.DefaultRetentionKeepLast));
        Assert.That(
            options.RetentionMaxAge,
            Is.EqualTo(TimeSpan.FromDays(RepoContextBackup.DefaultRetentionMaxAgeDays)));
    }

    [Test]
    public void The_configured_cadence_is_recorded_on_the_scope_options_as_one_source_of_truth()
    {
        var options = ResolveMemoryScopeSchedule();

        Assert.That(
            options.IncrementalBackupInterval,
            Is.EqualTo(LatticeBackupScheduleOptions.DefaultIncrementalBackupInterval));
        Assert.That(
            options.FullBackupInterval,
            Is.EqualTo(LatticeBackupScheduleOptions.DefaultFullBackupInterval));
    }

    [Test]
    public void The_schedule_is_scoped_to_the_memory_tree_and_not_applied_globally()
    {
        var settings = RepoContextBackup.Resolve(
            new ConfigurationBuilder()
                .AddInMemoryCollection(
                [
                    new KeyValuePair<string, string?>(
                        RepoContextBackup.BlobConnectionStringKey, "UseDevelopmentStorage=true"),
                ])
                .Build());

        var silo = new FakeSiloBuilder();
        silo.Services.AddSingleton<IValidateOptions<LatticeOptions>, NoOpLatticeOptionsValidator>();
        silo.Services.AddOptions();
        silo.ConfigureRepoContextBackup(settings);

        var monitor = silo.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptionsMonitor<LatticeBackupScheduleOptions>>();

        // A global schedule would also sweep up the code-index trees, which are
        // orders of magnitude larger and are rebuildable from the workspace: the
        // sink would fill with the one thing that does not need protecting, and
        // retention would age out the one thing that does.
        Assert.That(monitor.Get(Options.DefaultName).RetentionEnabled, Is.False);
        Assert.That(monitor.Get(RepoContextBackup.MemoryScopeKey).RetentionEnabled, Is.True);
    }
}
