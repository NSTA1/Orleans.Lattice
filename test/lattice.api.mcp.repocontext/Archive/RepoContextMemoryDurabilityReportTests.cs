namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Archive;

/// <summary>
/// Tests for the startup statement about durable-memory durability (issue #2601,
/// acceptance criterion 3).
/// <para>
/// These are wording tests, unusually, and deliberately so. The defect being fixed is
/// not that a container lost data; it is that the gesture which lost it read as safe.
/// A statement that overclaims - "memory is protected" without saying against what, or
/// until when - reproduces exactly that failure at the level of the warning, and turns
/// a visible risk into an invisible one. So the assertions here are about what the
/// statement must never say as much as about what it must.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextMemoryDurabilityReportTests
{
    private static string Text(RepoContextMemoryDurabilityStatement statement) =>
        string.Join('\n', statement.Lines);

    private static RepoContextMemoryDurabilityStatement Describe(
        RepoContextMemoryArchiveOptions options,
        string dataRoot = "/data")
        => RepoContextMemoryDurabilityReport.Describe(
            dataRoot, $"{dataRoot}/wal", $"{dataRoot}/repocontext.db", options);

    [Test]
    public void The_statement_is_always_a_warning_because_the_co_location_is_always_true()
    {
        var withoutArchive = Describe(new RepoContextMemoryArchiveOptions());
        var withArchive = Describe(new RepoContextMemoryArchiveOptions { Directory = "/memory-archive" });

        Assert.Multiple(() =>
        {
            Assert.That(withoutArchive.IsWarning, Is.True);
            Assert.That(
                withArchive.IsWarning,
                Is.True,
                "configuring an archive removes one consequence of the co-location, not the "
                    + "co-location, so the severity must not drop");
        });
    }

    [Test]
    public void The_statement_names_the_memory_tree_and_both_durable_planes()
    {
        var text = Text(Describe(new RepoContextMemoryArchiveOptions()));

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain(RepoContextTrees.Memory));
            Assert.That(text, Does.Contain("/data/wal"), "the write-ahead log plane");
            Assert.That(text, Does.Contain("/data/repocontext.db"), "the grain-storage plane");
        });
    }

    [Test]
    public void The_statement_says_memory_shares_a_volume_and_cannot_be_separated()
    {
        var text = Text(Describe(new RepoContextMemoryArchiveOptions { Directory = "/memory-archive" }));

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("Shares a volume with rebuildable index state: YES"));
            Assert.That(text, Does.Contain("cannot be separated"));
        });
    }

    [Test]
    public void The_statement_never_claims_memory_is_on_its_own_volume()
    {
        // The claim the fix must not make. Per-tree volume isolation is unachievable
        // (one WAL root per provider, one grain-storage provider for every tree), so a
        // statement asserting it would be false in the one direction that matters.
        foreach (var options in new[]
                 {
                     new RepoContextMemoryArchiveOptions(),
                     new RepoContextMemoryArchiveOptions { Directory = "/memory-archive" },
                     new RepoContextMemoryArchiveOptions { Directory = "/data/inside" },
                 })
        {
            var text = Text(Describe(options));

            Assert.That(
                text,
                Does.Not.Contain("own volume").IgnoreCase,
                "the statement must never claim a separate volume for memory");
            Assert.That(
                text,
                Does.Not.Contain("separate volume").IgnoreCase,
                "the statement must never claim a separate volume for memory");
        }
    }

    [Test]
    public void The_statement_never_describes_the_archive_as_a_backup()
    {
        // Boundary with issue #2602: the scheduled whole-store backup is a different
        // mechanism with manifests, retention, and operator-driven restore. This one
        // covers one tree and keeps two generations. Calling it a backup would let an
        // operator believe the store was covered when only memory is - so the statement
        // must disclaim the word rather than merely avoid it.
        var text = Text(Describe(new RepoContextMemoryArchiveOptions { Directory = "/memory-archive" }));

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("This is not a backup"));
            Assert.That(
                text,
                Does.Not.Contain("is a backup"),
                "the only sentence carrying the word must be the disclaimer");
        });
    }

    [Test]
    public void With_no_archive_the_statement_says_a_wipe_is_unrecoverable_and_names_the_variable()
    {
        var statement = Describe(new RepoContextMemoryArchiveOptions());
        var text = Text(statement);

        Assert.Multiple(() =>
        {
            Assert.That(statement.IsArchived, Is.False);
            Assert.That(statement.ArchiveSharesDataRoot, Is.False);
            Assert.That(text, Does.Contain("NOT CONFIGURED"));
            Assert.That(text, Does.Contain(RepoContextMemoryArchiveOptions.DirectoryKey));
            Assert.That(text, Does.Contain("unrecoverable"));
        });
    }

    [Test]
    public void The_statement_names_the_safe_gesture_for_the_rebuildable_half()
    {
        var text = Text(Describe(new RepoContextMemoryArchiveOptions()));

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("repocontext_reset_index"));
            Assert.That(text, Does.Contain("down -v"), "and names the gesture that is not safe");
        });
    }

    [Test]
    public void With_an_archive_outside_the_data_root_the_statement_states_the_residual_window()
    {
        var statement = Describe(new RepoContextMemoryArchiveOptions { Directory = "/memory-archive" });
        var text = Text(statement);

        Assert.Multiple(() =>
        {
            Assert.That(statement.IsArchived, Is.True);
            Assert.That(statement.ArchiveSharesDataRoot, Is.False);
            Assert.That(text, Does.Contain("GUARDED AGAINST"));
            Assert.That(text, Does.Contain("NOT GUARDED AGAINST"));
            Assert.That(
                text,
                Does.Contain("since the last"),
                "protection reaches exactly as far as the last successful export, and saying so "
                    + "is what stops the guarantee reading as unconditional");
        });
    }

    [Test]
    public void The_statement_admits_it_cannot_see_what_the_archive_path_is_mounted_from()
    {
        // The honest limit of the check. Being outside the data root is necessary and
        // not sufficient: a plain directory inside the container is also outside /data
        // and is still destroyed with the container.
        var text = Text(Describe(new RepoContextMemoryArchiveOptions { Directory = "/memory-archive" }));

        Assert.That(text, Does.Contain("cannot see what the path is mounted from"));
    }

    [TestCase("/data")]
    [TestCase("/data/archive")]
    [TestCase("/data/nested/deeper")]
    public void An_archive_inside_the_data_root_is_reported_as_guarding_nothing(string archiveDirectory)
    {
        var statement = Describe(new RepoContextMemoryArchiveOptions { Directory = archiveDirectory });
        var text = Text(statement);

        Assert.Multiple(() =>
        {
            Assert.That(statement.ArchiveSharesDataRoot, Is.True);
            Assert.That(statement.IsWarning, Is.True);
            Assert.That(text, Does.Contain("GUARDED AGAINST A VOLUME WIPE: NO"));
            Assert.That(text, Does.Contain("protects nothing"));
            Assert.That(
                text,
                Does.Not.Contain("NOT GUARDED AGAINST"),
                "the misleading-archive branch replaces the reassurance rather than qualifying it");
        });
    }

    [Test]
    public void A_sibling_of_the_data_root_that_merely_shares_a_prefix_is_not_treated_as_inside()
    {
        var statement = Describe(
            new RepoContextMemoryArchiveOptions { Directory = "/data-archive" });

        Assert.That(
            statement.ArchiveSharesDataRoot,
            Is.False,
            "/data-archive is a sibling of /data, not a child of it");
    }

    [Test]
    public void An_unknown_data_root_is_not_asserted_to_contain_the_archive()
    {
        var statement = RepoContextMemoryDurabilityReport.Describe(
            dataRoot: null,
            walDirectory: null,
            grainStorePath: null,
            new RepoContextMemoryArchiveOptions { Directory = "/memory-archive" });

        Assert.Multiple(() =>
        {
            Assert.That(statement.ArchiveSharesDataRoot, Is.False, "containment needs evidence");
            Assert.That(Text(statement), Does.Contain("not reported by this host"));
        });
    }

    [Test]
    public void The_statement_reports_the_effective_cadence_not_the_declared_one()
    {
        var statement = Describe(new RepoContextMemoryArchiveOptions
        {
            Directory = "/memory-archive",
            Interval = TimeSpan.FromSeconds(1),
        });

        Assert.That(
            Text(statement),
            Does.Contain("every 30s"),
            "a report that echoed the declared 1s would describe a cadence that is not in force");
    }

    // The mode travels as a string: the enum is internal, and an internal parameter
    // type on a public fixture method is a compile error.
    [TestCase("Off", "never imported automatically")]
    [TestCase("Auto", "only when the memory tree is empty")]
    [TestCase("Always", "every startup")]
    public void The_statement_explains_what_the_restore_mode_actually_does(
        string mode, string expected)
    {
        var statement = Describe(new RepoContextMemoryArchiveOptions
        {
            Directory = "/memory-archive",
            RestoreMode = Enum.Parse<RepoContextMemoryArchiveRestoreMode>(mode),
        });

        Assert.That(Text(statement), Does.Contain(expected));
    }

    [Test]
    public void Describe_rejects_a_null_options()
    {
        Assert.That(
            () => RepoContextMemoryDurabilityReport.Describe("/data", "/data/wal", "/data/db", null!),
            Throws.ArgumentNullException);
    }
}
