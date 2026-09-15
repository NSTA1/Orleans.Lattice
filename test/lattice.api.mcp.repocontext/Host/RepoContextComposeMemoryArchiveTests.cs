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

    private static string EnvExamplePath => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", ".env.example");

    private static string ProvenanceLibraryPath => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "scripts", "_provenance.ps1");

    private static string ProvenanceEntryPointPath => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "scripts", "Assert-ContainerProvenance.ps1");

    private static string ProvenanceTestSuitePath => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "scripts", "Test-ContainerProvenance.ps1");

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

        var mount = Unquote(archiveMounts[0]);
        var target = ":/memory-archive";
        var source = mount[..mount.IndexOf(target, StringComparison.Ordinal)];

        Assert.Multiple(() =>
        {
            Assert.That(
                source,
                Does.StartWith("${").And.EndWith("}"),
                "the archive source must be operator-supplied through a variable, not hard-coded");
            Assert.That(
                ReadDeclaredVolumeNames(),
                Does.Not.Contain(source),
                "the archive source must not be a declared project volume - declared volumes are "
                    + "exactly what down -v destroys");
        });

        var inner = source[2..^1];
        var defaultAt = inner.IndexOf(":-", StringComparison.Ordinal);

        // ISSUE #2627. This assertion used to run the other way: it REQUIRED a ":-"
        // default, on the reasoning that a variable without one leaves an unset host
        // with an empty source. That reasoning was sound and the conclusion was wrong,
        // because it never asked what the default RESOLVED to. The shipped default was
        // "./memory-archive", and a relative bind source resolves against the compose
        // INVOCATION directory - which the documented gate procedure deliberately makes
        // the candidate git worktree, so the only working backup of durable agent memory
        // was written into a directory that "git worktree remove" deletes. A default is a
        // path nobody chose; here that is the whole defect, so there is no default to fix
        // and the variable is required instead. The empty-source worry is answered by
        // "${VAR:?message}", which fails the compose command by name before a container
        // starts or a directory is created.
        if (defaultAt > 0)
        {
            var fallback = inner[(defaultAt + 2)..];
            Assert.That(
                IsAbsoluteHostPath(fallback),
                Is.True,
                $"the archive source defaults to '{fallback}', which is RELATIVE. A relative bind "
                    + "source resolves against the directory compose was invoked from, so the only "
                    + "working backup of durable agent memory lands wherever the operator happened "
                    + "to be standing - in the documented gate procedure, an ephemeral git worktree "
                    + "(issue #2627). Either default to an absolute path outside every checkout, or "
                    + "make the variable required with ${VAR:?message}.");
        }
        else
        {
            Assert.That(
                inner,
                Does.Contain(":?"),
                $"the archive source '{source}' neither defaults to an absolute path nor is required "
                    + "with ${VAR:?message}, so an unset host gets an empty source");
            Assert.That(
                inner[(inner.IndexOf(":?", StringComparison.Ordinal) + 2)..],
                Is.Not.Empty,
                "a required archive variable must carry a message naming what to set and why, "
                    + "because the operator sees only that message");
        }
    }

    /// <summary>
    /// Strips the optional surrounding double quotes from a compose mount entry. The
    /// archive entry is quoted because its failure message contains a colon, which YAML
    /// would otherwise read as a mapping.
    /// </summary>
    private static string Unquote(string value) =>
        value.Length >= 2 && value.StartsWith('"') && value.EndsWith('"')
            ? value[1..^1]
            : value;

    /// <summary>
    /// Whether a bind source is anchored rather than resolved against wherever compose
    /// was invoked. Both platforms' forms count: the sample is run from Windows and from
    /// Linux, and only one of them is ever the host of any given run.
    /// </summary>
    private static bool IsAbsoluteHostPath(string path)
    {
        if (path.StartsWith('/') || path.StartsWith("\\\\", StringComparison.Ordinal))
        {
            return true;
        }

        return path.Length >= 3
            && char.IsLetter(path[0])
            && path[1] == ':'
            && (path[2] == '\\' || path[2] == '/');
    }

    [Test]
    public void The_sample_env_example_warns_about_the_archive_path_as_loudly_as_the_repo_path()
    {
        // ISSUE #2627. Before the fix .env.example mentioned worktrees twelve times and
        // the word ARCHIVE zero times: every warning was about the READ path (REPO_PATH,
        // where a stale worktree yields a silently wrong index) and none about the WRITE
        // path (where a worktree yields a backup that is deleted). The asymmetry was part
        // of the defect, because the file an operator copies is where they learn what a
        // variable costs to get wrong.
        Assert.That(File.Exists(EnvExamplePath), Is.True, $"expected the sample env template at {EnvExamplePath}");

        var text = File.ReadAllText(EnvExamplePath);

        Assert.Multiple(() =>
        {
            Assert.That(
                text,
                Does.Contain("REPOCONTEXT_MEMORY_ARCHIVE_PATH"),
                "the template must name the archive variable at all");
            Assert.That(
                text,
                Does.Contain("worktree"),
                "and must say what the failure is, not merely that the variable exists");
            Assert.That(
                text,
                Does.Contain("#2627"),
                "and must cite the observed failure, so the warning reads as a report rather "
                    + "than as caution");
        });

        var assignment = File.ReadAllLines(EnvExamplePath)
            .Select(l => l.Trim())
            .FirstOrDefault(l => l.StartsWith("REPOCONTEXT_MEMORY_ARCHIVE_PATH=", StringComparison.Ordinal));

        Assert.That(
            assignment,
            Is.Not.Null,
            "the template must ASSIGN the archive variable. The compose file requires it, so a "
                + "template that only discusses it leaves every compose command in the directory "
                + "failing for an operator who copied the file as instructed.");

        var value = assignment!["REPOCONTEXT_MEMORY_ARCHIVE_PATH=".Length..].Trim();

        Assert.That(
            IsAbsoluteHostPath(value),
            Is.True,
            $"the template assigns '{value}', which is RELATIVE, so copying the template reproduces "
                + "the exact defect the compose file was changed to prevent (issue #2627)");
    }

    [Test]
    public void The_provenance_guard_inspects_the_archive_bind_destination()
    {
        // ISSUE #2627, the guard-direction finding. Grepping all three provenance scripts
        // for Mounts|Destination|memory-archive|MEMORY_ARCHIVE returned ZERO matches: the
        // guard checked four INPUTS (checkout, commit, image id, config file count) and
        // never a bind DESTINATION, so a container whose memory archive pointed into a
        // doomed directory passed every check it had. This turns that measurement into a
        // standing assertion.
        var library = File.ReadAllText(ProvenanceLibraryPath);
        var entryPoint = File.ReadAllText(ProvenanceEntryPointPath);

        Assert.Multiple(() =>
        {
            Assert.That(
                library,
                Does.Contain("Get-ArchiveDurabilityViolation"),
                "the pure adjudication library must carry an archive durability check");
            Assert.That(
                entryPoint,
                Does.Contain("Mounts"),
                "the operator entry point must read the container's mount table, which is the "
                    + "only place a bind destination can be observed");
            Assert.That(
                entryPoint,
                Does.Contain("/memory-archive"),
                "and must look for the archive destination by name");
            Assert.That(
                entryPoint,
                Does.Contain("Get-ArchiveDurabilityViolation").Or.Contain("ArchiveDestination"),
                "and must feed those readings to the check rather than merely printing them");
        });
    }

    [Test]
    public void The_archive_durability_check_is_keyed_on_the_archive_path_not_the_compose_directory()
    {
        // The load-bearing constraint, and the one a plausible fix gets backwards.
        // Provenance check 2 REQUIRES the compose working directory to be a git worktree
        // (that is what proves the image was built from the tree under test), so a check
        // that refused a worktree WORKING DIRECTORY would refuse every legitimate gate run
        // while satisfying every archive fixture. The guarantee is structural: the check
        // is a pure function, and its parameter list contains no compose directory and no
        // expected checkout, so it CANNOT key on either.
        var library = File.ReadAllText(ProvenanceLibraryPath);

        var start = library.IndexOf("function Get-ArchiveDurabilityViolation", StringComparison.Ordinal);
        Assert.That(start, Is.GreaterThan(0), "expected Get-ArchiveDurabilityViolation in the library");

        var paramStart = library.IndexOf("param(", start, StringComparison.Ordinal);
        Assert.That(paramStart, Is.GreaterThan(start), "expected a param block on the check");

        var paramEnd = library.IndexOf(")\n", paramStart, StringComparison.Ordinal);
        if (paramEnd < 0)
        {
            paramEnd = library.IndexOf(")\r\n", paramStart, StringComparison.Ordinal);
        }

        Assert.That(paramEnd, Is.GreaterThan(paramStart), "expected the param block to be closed");

        var parameters = library[paramStart..paramEnd];

        Assert.Multiple(() =>
        {
            Assert.That(
                parameters,
                Does.Not.Contain("WorkingDirectory"),
                "the archive check must not be given the compose working directory - keying on it "
                    + "would contradict provenance check 2 and refuse every gate run (issue #2627)");
            Assert.That(
                parameters,
                Does.Not.Contain("ExpectedCheckout"),
                "nor the expected checkout, for the same reason");
            Assert.That(
                parameters,
                Does.Contain("ArchiveSource"),
                "and must be given the resolved archive source, which is the only thing it may judge");
        });
    }

    [Test]
    public void The_provenance_test_suite_fixes_the_guard_direction_in_both_directions()
    {
        // A refusal fixture alone cannot distinguish the right fix from the wrong one:
        // a check keyed on the compose directory passes every refusal case in the file.
        // Only an ACCEPTING fixture whose compose directory is itself a worktree can, so
        // its presence is asserted rather than left to survive a future edit by luck.
        var suite = File.ReadAllText(ProvenanceTestSuitePath);

        // The total is deliberately NOT part of the pattern. Asserting "Check 5 of 5"
        // coupled this check to how many checks exist, so adding check 6 (the build
        // provenance check, issue 2686) failed THIS test with a message about archive
        // durability - an assertion that reports a fault it is not testing sends the
        // next reader to the wrong file. What is guarded is that the archive section
        // is still present and still exercised, which its title establishes on its own.
        Assert.Multiple(() =>
        {
            Assert.That(
                suite,
                Does.Match(@"Check 5 of \d+: archive durability"),
                "the provenance test suite must exercise the archive durability check");
            Assert.That(
                suite,
                Does.Contain("compose directory IS a git worktree"),
                "and must carry the accepting fixture that fails a check keyed on the compose "
                    + "directory, which is the plausible wrong fix for issue #2627");
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
