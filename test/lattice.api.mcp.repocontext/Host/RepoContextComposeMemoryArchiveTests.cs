namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Asserts that the sample container does not ship the layout that destroys durable
/// agent memory (issue #2601, acceptance criterion 4).
/// <para>
/// The defect the sample shipped was not a wrong value but a wrong shape: every kind
/// of durable state sat on one named volume, so <c>docker compose down -v</c> - the
/// documented way to reset an index - destroyed the authored memory too. The memory
/// and the index cannot be split across volumes, and a second NAMED volume would not
/// have helped even if they could, because <c>-v</c> removes every volume a project
/// declares. Only a bind mount survives it. So what is asserted here is the mount
/// TYPE, which is the part that carries the protection, rather than the presence of
/// a second mount, which does not.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextComposeMemoryArchiveTests
{
    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    private static string ComposePath => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "docker-compose.yml");

    private static string SampleReadmePath => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "README.md");

    private static string DocsPath => Path.Combine(
        RepoRoot, "docs", "lattice.api.mcp.repocontext", "memory-durability.md");

    /// <summary>
    /// The mount entries of one service, as written. A small hand-rolled scan rather
    /// than a YAML dependency, matching the sibling compose fixture: the file's
    /// two-space service indentation is stable, and the alternative is adding a parser
    /// for a handful of assertions.
    /// </summary>
    private static List<string> ReadServiceVolumes(string serviceName)
    {
        var mounts = new List<string>();
        var service = string.Empty;
        var inVolumes = false;

        foreach (var raw in File.ReadAllLines(ComposePath))
        {
            var trimmed = raw.TrimStart();
            if (trimmed.Length == 0 || trimmed.StartsWith('#'))
            {
                continue;
            }

            var indent = raw.Length - trimmed.Length;

            if (indent <= 2 && trimmed.EndsWith(':') && !trimmed.Contains(' ', StringComparison.Ordinal))
            {
                service = indent == 2 ? trimmed[..^1] : string.Empty;
                inVolumes = false;
                continue;
            }

            if (indent == 4 && trimmed.StartsWith("volumes:", StringComparison.Ordinal))
            {
                inVolumes = string.Equals(service, serviceName, StringComparison.Ordinal);
                continue;
            }

            if (indent == 4 && trimmed.EndsWith(':'))
            {
                inVolumes = false;
                continue;
            }

            if (inVolumes && trimmed.StartsWith("- ", StringComparison.Ordinal))
            {
                mounts.Add(trimmed[2..].Trim());
            }
        }

        return mounts;
    }

    /// <summary>The names declared in the top-level <c>volumes:</c> block.</summary>
    private static List<string> ReadDeclaredVolumeNames()
    {
        var names = new List<string>();
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
                names.Add(trimmed[..^1]);
            }
        }

        return names;
    }

    [Test]
    public void The_sample_mounts_a_memory_archive_that_is_a_bind_mount_not_a_named_volume()
    {
        Assert.That(File.Exists(ComposePath), Is.True, $"expected the sample compose file at {ComposePath}");

        var mounts = ReadServiceVolumes("repocontext");
        var archiveMounts = mounts
            .Where(m => m.Contains(":/memory-archive", StringComparison.Ordinal))
            .ToList();

        Assert.That(
            archiveMounts,
            Has.Count.EqualTo(1),
            "the repocontext service must mount exactly one memory archive at /memory-archive");

        var mount = archiveMounts[0];
        var target = ":/memory-archive";
        var source = mount[..mount.IndexOf(target, StringComparison.Ordinal)];

        // The source is written as `${VAR:-default}` so an operator can relocate it.
        // What matters for the protection is the DEFAULT, because that is what the
        // sample actually ships and what an operator who changes nothing will get.
        var effective = source;
        if (effective.StartsWith("${", StringComparison.Ordinal) && effective.EndsWith('}'))
        {
            var inner = effective[2..^1];
            var defaultAt = inner.IndexOf(":-", StringComparison.Ordinal);
            Assert.That(
                defaultAt,
                Is.GreaterThan(0),
                $"the archive source '{source}' interpolates a variable with no default, so a "
                    + "host that does not set it gets an empty source and compose fails to start");
            effective = inner[(defaultAt + 2)..];
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                effective,
                Does.Contain("/").Or.StartWith("."),
                "the archive source must resolve to a host path (a bind mount). A bare name would "
                    + "be a NAMED volume, which docker compose down -v removes along with everything "
                    + "else, so it would read as protection while providing none.");
            Assert.That(
                ReadDeclaredVolumeNames(),
                Does.Not.Contain(effective),
                "the archive source must not be a declared project volume - declared volumes are "
                    + "exactly what down -v destroys");
        });
    }

    [Test]
    public void The_sample_keeps_the_live_store_on_the_named_data_volume()
    {
        // The fix must not have moved the hot path onto a bind mount: bind-mount IO on
        // Docker Desktop is materially slower, and only the small archive needs to
        // survive a wipe.
        var mounts = ReadServiceVolumes("repocontext");

        Assert.Multiple(() =>
        {
            Assert.That(mounts, Does.Contain("repocontext-data:/data"));
            Assert.That(ReadDeclaredVolumeNames(), Does.Contain("repocontext-data"));
        });
    }

    [Test]
    public void The_sample_points_the_host_at_the_mounted_archive_directory()
    {
        // A mount nothing is configured to write to is decoration. The env var and the
        // mount target have to agree, and a substring search for either alone would
        // pass while they disagreed.
        var text = File.ReadAllText(ComposePath);

        Assert.That(
            text,
            Does.Contain("LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_DIR: /memory-archive"),
            "the archive directory the host writes to must be the path the bind mount targets");
    }

    [Test]
    public void The_sample_documents_what_the_archive_does_not_protect()
    {
        // Criterion 3's rule applied to the sample's own prose: a reassurance whose
        // limits are not stated converts a visible risk into an invisible one.
        var compose = File.ReadAllText(ComposePath);
        var readme = File.ReadAllText(SampleReadmePath);

        Assert.Multiple(() =>
        {
            Assert.That(
                compose,
                Does.Contain("not a backup"),
                "the compose comments must disclaim the word rather than imply it");
            Assert.That(
                readme,
                Does.Contain("repocontext_reset_index"),
                "the sample README must name the lossless gesture for the rebuildable half");
            Assert.That(
                readme,
                Does.Contain("since the last export"),
                "and must state the residual window rather than leaving it to be inferred");
        });
    }

    [Test]
    public void The_package_documents_the_rebuildable_and_irreplaceable_split()
    {
        // Criterion 5.
        Assert.That(File.Exists(DocsPath), Is.True, $"expected package documentation at {DocsPath}");

        var docs = File.ReadAllText(DocsPath);

        Assert.Multiple(() =>
        {
            Assert.That(docs, Does.Contain("Rebuildable"));
            Assert.That(docs, Does.Contain("Irreplaceable"));
            Assert.That(docs, Does.Contain("repocontext_reset_index"));
            Assert.That(docs, Does.Contain("repocontext_remove_repo"));
            Assert.That(
                docs,
                Does.Contain("#2602"),
                "the boundary with the scheduled whole-store backup must be stated, so neither "
                    + "mechanism is mistaken for the other");
        });
    }
}
