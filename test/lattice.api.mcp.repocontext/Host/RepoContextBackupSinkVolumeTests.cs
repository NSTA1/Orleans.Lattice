namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Holds the one property the agent-memory backup sink exists for: that
/// <c>docker compose down -v</c> cannot reach it.
/// <para>
/// Issue #2602. Several hundred durable agent-memory entries were permanently
/// lost to a routine <c>docker compose down -v</c> intended only to clear the
/// code index. <c>down -v</c> removes every <b>named volume declared in the
/// project's top-level <c>volumes:</c> block</b>. A backup sink stored in such a
/// volume is therefore destroyed by the exact gesture it exists to survive -
/// which is strictly worse than having no backup at all, because an absent
/// backup is visible and a false one is not.
/// </para>
/// <para>
/// The sink is consequently a <b>host bind mount</b>, which is not a
/// project-managed volume and so is not enumerated by <c>down -v</c>. That is a
/// structural property of the compose file, so it is asserted structurally here
/// rather than asserted in prose in a document nothing checks. The two halves
/// have to hold together and neither is sufficient alone: the mount source must
/// be a host path, AND that source must not also appear as an entry in the
/// top-level <c>volumes:</c> block.
/// </para>
/// <para>
/// Deliberately a small hand-rolled scan rather than a YAML dependency, matching
/// <see cref="RepoContextComposeShutdownBudgetTests"/>: the file's two-space
/// service indentation is stable and the alternative is adding a parser to the
/// test project for a handful of assertions.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextBackupSinkVolumeTests
{
    private const string SinkService = "azurite-backup-sink";

    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    private static string ComposePath => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "docker-compose.yml");

    /// <summary>
    /// Reads the <c>volumes:</c> entries of each service, keyed by service name.
    /// Only the short <c>source:target</c> string form is recognized, which is the
    /// form the sample uses.
    /// </summary>
    private static Dictionary<string, List<string>> ReadServiceVolumeMounts()
    {
        var result = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var service = string.Empty;
        var inServices = false;
        var inVolumesKey = false;

        foreach (var raw in File.ReadAllLines(ComposePath))
        {
            var trimmed = raw.TrimStart();
            if (trimmed.Length == 0 || trimmed.StartsWith('#'))
            {
                continue;
            }

            var indent = raw.Length - trimmed.Length;

            // A top-level key ends any service scope we were in.
            if (indent == 0)
            {
                inServices = trimmed.StartsWith("services:", StringComparison.Ordinal);
                service = string.Empty;
                inVolumesKey = false;
                continue;
            }

            if (!inServices)
            {
                continue;
            }

            // A service key sits at exactly two spaces under `services:`.
            if (indent == 2 && trimmed.EndsWith(':') && !trimmed.Contains(' ', StringComparison.Ordinal))
            {
                service = trimmed[..^1];
                result[service] = [];
                inVolumesKey = false;
                continue;
            }

            if (service.Length == 0)
            {
                continue;
            }

            if (indent == 4)
            {
                inVolumesKey = trimmed.StartsWith("volumes:", StringComparison.Ordinal);
                continue;
            }

            if (inVolumesKey && indent >= 6 && trimmed.StartsWith("- ", StringComparison.Ordinal))
            {
                result[service].Add(trimmed[2..].Trim().Trim('"'));
            }
        }

        return result;
    }

    /// <summary>Reads the entry names declared in the top-level <c>volumes:</c> block.</summary>
    private static List<string> ReadProjectManagedVolumeNames()
    {
        var result = new List<string>();
        var inTopLevelVolumes = false;

        foreach (var raw in File.ReadAllLines(ComposePath))
        {
            var trimmed = raw.TrimStart();
            if (trimmed.Length == 0 || trimmed.StartsWith('#'))
            {
                continue;
            }

            var indent = raw.Length - trimmed.Length;

            if (indent == 0)
            {
                inTopLevelVolumes = trimmed.StartsWith("volumes:", StringComparison.Ordinal);
                continue;
            }

            if (inTopLevelVolumes && indent == 2 && trimmed.EndsWith(':'))
            {
                result.Add(trimmed[..^1]);
            }
        }

        return result;
    }

    [Test]
    public void The_sample_compose_file_declares_a_dedicated_backup_sink_service()
    {
        Assert.That(File.Exists(ComposePath), Is.True, $"expected the sample compose file at {ComposePath}");

        var mounts = ReadServiceVolumeMounts();

        Assert.That(
            mounts.ContainsKey(SinkService),
            Is.True,
            $"expected a dedicated '{SinkService}' service; agent memory has no backup without one. "
                + $"Services found: {string.Join(", ", mounts.Keys)}");
    }

    [Test]
    public void The_backup_sink_data_directory_is_a_host_bind_mount_and_not_a_named_volume()
    {
        var mounts = ReadServiceVolumeMounts();
        Assert.That(mounts.ContainsKey(SinkService), Is.True, $"expected a '{SinkService}' service");

        var dataMount = mounts[SinkService]
            .FirstOrDefault(m => m.EndsWith(":/data", StringComparison.Ordinal));

        Assert.That(
            dataMount,
            Is.Not.Null,
            $"expected '{SinkService}' to mount something at /data; found: "
                + $"{string.Join(", ", mounts[SinkService])}");

        var source = dataMount![..^"/data".Length].TrimEnd(':');

        // A bind mount's source is a path. Compose treats a source containing a
        // slash, or beginning with '.', '/' or '~', as a host path; anything else
        // is a named volume reference. The sample's source is behind a ${...}
        // default, so the default is what carries the property.
        var isPathLike =
            source.Contains('/', StringComparison.Ordinal)
            || source.StartsWith('.')
            || source.StartsWith('~');

        Assert.That(
            isPathLike,
            Is.True,
            $"the '{SinkService}' /data mount source '{source}' does not look like a host path, so it is a "
                + "project-managed named volume that 'docker compose down -v' would destroy along with the "
                + "backups. That is the exact failure issue #2602 exists to remove.");
    }

    [Test]
    public void The_backup_sink_source_is_not_declared_in_the_top_level_volumes_block()
    {
        var mounts = ReadServiceVolumeMounts();
        var projectVolumes = ReadProjectManagedVolumeNames();

        Assert.That(mounts.ContainsKey(SinkService), Is.True, $"expected a '{SinkService}' service");

        var dataMount = mounts[SinkService]
            .FirstOrDefault(m => m.EndsWith(":/data", StringComparison.Ordinal));
        Assert.That(dataMount, Is.Not.Null, $"expected '{SinkService}' to mount something at /data");

        var source = dataMount![..^"/data".Length].TrimEnd(':');

        // 'down -v' enumerates exactly the entries of the top-level volumes block,
        // so membership of that block is the precise predicate for destruction.
        foreach (var declared in projectVolumes)
        {
            Assert.That(
                source.Contains(declared, StringComparison.Ordinal),
                Is.False,
                $"the '{SinkService}' /data source '{source}' references the project-managed volume "
                    + $"'{declared}', which 'docker compose down -v' removes. The backup sink must not live "
                    + "in any volume named in the top-level volumes block.");
        }

        Assert.That(
            projectVolumes,
            Is.Not.Empty,
            "expected the compose file to still declare at least one project-managed volume; if this block "
                + "became empty the assertion above would pass vacuously and would prove nothing.");
    }

    [Test]
    public void The_backup_sink_is_a_separate_service_from_the_primary_cluster_storage()
    {
        var mounts = ReadServiceVolumeMounts();

        // The point of a dedicated sink is that destroying primary storage does not
        // destroy the backups. If the same service carried both, one 'docker rm -v'
        // would take them together.
        var repocontextMounts = mounts.TryGetValue("repocontext", out var m) ? m : [];

        Assert.That(
            repocontextMounts.Any(v => v.Contains("backup-sink", StringComparison.Ordinal)),
            Is.False,
            "the primary repocontext service must not mount the backup sink directory; the sink is reached "
                + "over the blob endpoint precisely so the two lifetimes stay separate.");
    }
}
