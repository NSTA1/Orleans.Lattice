using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Backup.AzureBlob;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers how the container resolves its agent-memory backup configuration
/// (issue #2602), and in particular the two ways it refuses rather than starting
/// a container whose backup behaviour does not match what its configuration
/// says.
/// </summary>
[TestFixture]
public sealed class RepoContextBackupSettingsTests
{
    private const string Sink = "UseDevelopmentStorage=true";

    private static IConfiguration Config(params (string Key, string Value)[] pairs) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(pairs.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)))
            .Build();

    [Test]
    public void Backup_is_disabled_when_no_external_sink_is_configured()
    {
        var settings = RepoContextBackup.Resolve(Config());

        // The default in-cluster sink stores backups in the very store being
        // captured, so "enabled with no external sink" would be false protection.
        Assert.That(settings.Enabled, Is.False);
        Assert.That(settings.BlobConnectionString, Is.Null.Or.Empty);
    }

    [Test]
    public void Backup_is_enabled_when_an_external_sink_is_configured()
    {
        var settings = RepoContextBackup.Resolve(
            Config((RepoContextBackup.BlobConnectionStringKey, Sink)));

        Assert.That(settings.Enabled, Is.True);
        Assert.That(settings.BlobConnectionString, Is.EqualTo(Sink));
        Assert.That(settings.ContainerName, Is.EqualTo(LatticeBackupAzureBlobOptions.DefaultContainerName));
    }

    [Test]
    public void An_explicit_false_kill_switch_disables_backup_even_with_a_sink_configured()
    {
        var settings = RepoContextBackup.Resolve(Config(
            (RepoContextBackup.BlobConnectionStringKey, Sink),
            (RepoContextBackup.EnabledKey, "false")));

        Assert.That(settings.Enabled, Is.False);
    }

    [Test]
    public void The_default_incremental_cadence_is_the_packages_own_hourly_default()
    {
        var settings = RepoContextBackup.Resolve(
            Config((RepoContextBackup.BlobConnectionStringKey, Sink)));

        // Deliberately compared against the library constant rather than a literal
        // "1 hour": the cadence issue #2602 asks for is already what the package
        // ships, and restating it here would create a second source of truth.
        Assert.That(
            settings.IncrementalInterval,
            Is.EqualTo(LatticeBackupScheduleOptions.DefaultIncrementalBackupInterval));
        Assert.That(
            settings.FullInterval,
            Is.EqualTo(LatticeBackupScheduleOptions.DefaultFullBackupInterval));
    }

    [Test]
    public void Explicit_intervals_are_honoured()
    {
        var settings = RepoContextBackup.Resolve(Config(
            (RepoContextBackup.BlobConnectionStringKey, Sink),
            (RepoContextBackup.IncrementalMinutesKey, "15"),
            (RepoContextBackup.FullHoursKey, "6")));

        Assert.That(settings.IncrementalInterval, Is.EqualTo(TimeSpan.FromMinutes(15)));
        Assert.That(settings.FullInterval, Is.EqualTo(TimeSpan.FromHours(6)));
    }

    [Test]
    public void An_unparseable_interval_is_refused_rather_than_silently_defaulted()
    {
        // Falling back to a default here would produce a container backing up on a
        // cadence nobody asked for while its configuration says otherwise.
        Assert.That(
            () => RepoContextBackup.Resolve(Config(
                (RepoContextBackup.BlobConnectionStringKey, Sink),
                (RepoContextBackup.IncrementalMinutesKey, "hourly"))),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains(RepoContextBackup.IncrementalMinutesKey));
    }

    [Test]
    public void An_interval_below_the_reminder_minimum_is_refused()
    {
        Assert.That(
            () => RepoContextBackup.Resolve(Config(
                (RepoContextBackup.BlobConnectionStringKey, Sink),
                (RepoContextBackup.IncrementalMinutesKey, "0"))),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void A_restore_id_without_a_sink_is_refused_rather_than_ignored()
    {
        // An operator sets this during an incident. Ignoring it would look like a
        // successful restore that restored nothing.
        Assert.That(
            () => RepoContextBackup.Resolve(
                Config((RepoContextBackup.RestoreBackupIdKey, "backup-123"))),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains(RepoContextBackup.BlobConnectionStringKey));
    }

    [Test]
    public void A_restore_id_with_a_sink_is_resolved()
    {
        var settings = RepoContextBackup.Resolve(Config(
            (RepoContextBackup.BlobConnectionStringKey, Sink),
            (RepoContextBackup.RestoreBackupIdKey, "backup-123")));

        Assert.That(settings.RestoreBackupId, Is.EqualTo("backup-123"));
    }

    [Test]
    public void The_blob_connection_string_is_never_offered_as_safe_to_print()
    {
        // It carries an account key. The effective-configuration dump prints every
        // key on this list verbatim.
        Assert.That(
            RepoContextBackup.SafeToPrintKeys,
            Does.Not.Contain(RepoContextBackup.BlobConnectionStringKey));

        Assert.That(RepoContextBackup.SafeToPrintKeys, Is.Not.Empty);
        Assert.That(RepoContextBackup.SafeToPrintKeys, Contains.Item(RepoContextBackup.IncrementalMinutesKey));
    }

    [Test]
    public void The_capture_scope_names_the_agent_memory_tree_and_nothing_else()
    {
        // Criterion 4 of issue #2602. The code-index trees are rebuildable from the
        // working tree; the memory tree is not derivable from anything, so it is the
        // one that has to be captured, and it is named here rather than implied by
        // a global schedule that would also sweep up the rebuildable trees.
        Assert.That(RepoContextBackup.MemoryScope.TreeId, Is.EqualTo(RepoContextHostTrees.Memory));
        Assert.That(RepoContextHostTrees.Memory, Is.EqualTo("repo-context-memory"));
    }
}
