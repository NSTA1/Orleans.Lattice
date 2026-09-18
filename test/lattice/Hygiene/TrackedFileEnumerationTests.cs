using System.IO;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Regression coverage for issue #3134: the content hygiene gates enumerate
/// the files git TRACKS, not every file present in the worktree, and they
/// classify what they find rather than assuming an unknown file is text.
/// </summary>
/// <remarks>
/// <para>
/// Each test here is written so that reverting the fix fails it. The
/// enumeration tests do not merely assert that an untracked file is absent -
/// absence is also what a broken enumerator that found nothing would produce.
/// They first prove the file IS on disk and WOULD be reached by the
/// filesystem walk the fix removed, and only then assert the tracked
/// enumeration omits it. That pairing is what distinguishes "correctly
/// excluded" from "vacuously empty".
/// </para>
/// </remarks>
[TestFixture]
[Category("Unit")]
[NonParallelizable]
public sealed class TrackedFileEnumerationTests
{
    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    [Test]
    public void Tracked_enumeration_omits_an_untracked_file_the_filesystem_walk_would_reach()
    {
        // docs/ belongs to no package slice, so it is covered by the core
        // fixture's repo-level scan - the same scan that reached the running
        // container's gitignored Azurite store before this fix.
        var probe = Path.Combine(RepoRoot, "docs", "hygiene-untracked-probe-" + Guid.NewGuid().ToString("N") + ".md");

        try
        {
            File.WriteAllText(probe, "# probe" + Environment.NewLine);

            // Non-vacuity: the probe really is where the removed filesystem
            // walk would have found it. Without this the assertion below
            // would also pass if the probe had never been written.
            Assert.That(File.Exists(probe), Is.True, "The probe file was not created.");
            var walked = Directory.EnumerateFiles(
                Path.Combine(RepoRoot, "docs"), "*", SearchOption.AllDirectories);
            Assert.That(walked, Does.Contain(probe),
                "A filesystem walk of docs/ did not reach the probe, so this test cannot tell a correct "
                + "exclusion from an enumeration that found nothing.");

            // git never reported it, so the gate never sees it.
            var tracked = HygieneFiles.EnumerateTextFiles(RepoRoot, CoreHygieneScope.Value).ToList();
            Assert.That(tracked, Is.Not.Empty, "The tracked enumeration returned nothing at all.");
            Assert.That(tracked, Does.Not.Contain(probe),
                "An untracked file in the worktree reached the content gates. The gates are named for tracked "
                + "files and must enumerate what git tracks, not what happens to be on disk.");
        }
        finally
        {
            if (File.Exists(probe)) File.Delete(probe);
        }
    }

    [Test]
    public void Tracked_enumeration_count_is_unchanged_by_an_untracked_file()
    {
        // The denominator is the anti-vacuity control of issue #2275. If an
        // untracked file could inflate it, the control would be measuring the
        // worktree's contents rather than the repository's.
        var before = HygieneFiles.EnumerateTextFiles(RepoRoot, CoreHygieneScope.Value).Count();
        Assert.That(before, Is.GreaterThan(100), "The core scope examined implausibly few files.");

        var probe = Path.Combine(RepoRoot, "docs", "hygiene-denominator-probe-" + Guid.NewGuid().ToString("N") + ".md");
        try
        {
            File.WriteAllText(probe, "# probe" + Environment.NewLine);
            var after = HygieneFiles.EnumerateTextFiles(RepoRoot, CoreHygieneScope.Value).Count();
            Assert.That(after, Is.EqualTo(before),
                "Creating an untracked file changed the hygiene denominator.");
        }
        finally
        {
            if (File.Exists(probe)) File.Delete(probe);
        }
    }

    [Test]
    public void Tracked_enumeration_omits_an_untracked_file_in_a_slice_root()
    {
        // The repo-level scan and the slice scan are separate code paths.
        // A probe in docs/ exercises only the first, so a regression confined
        // to slice enumeration would pass unnoticed. src/lattice is a slice
        // root of the core scope.
        var probe = Path.Combine(RepoRoot, "src", "lattice", "HygieneSliceProbe" + Guid.NewGuid().ToString("N") + ".md");

        try
        {
            File.WriteAllText(probe, "# probe" + Environment.NewLine);
            Assert.That(File.Exists(probe), Is.True, "The probe file was not created.");

            // Non-vacuity: the removed filesystem walk would have reached it.
            var walked = Directory.EnumerateFiles(
                Path.Combine(RepoRoot, "src", "lattice"), "*", SearchOption.AllDirectories);
            Assert.That(walked, Does.Contain(probe),
                "A filesystem walk of src/lattice did not reach the probe, so this test proves nothing.");

            var tracked = HygieneFiles.EnumerateTextFiles(RepoRoot, CoreHygieneScope.Value).ToList();
            Assert.That(tracked, Is.Not.Empty, "The tracked enumeration returned nothing at all.");
            Assert.That(tracked, Does.Not.Contain(probe),
                "An untracked file inside a slice root reached the content gates.");
        }
        finally
        {
            if (File.Exists(probe)) File.Delete(probe);
        }
    }

    [Test]
    public void Tracked_enumeration_omits_a_gitignored_directory_the_filesystem_walk_would_reach()
    {
        // The concrete trigger for #3134 was a gitignored, locked directory
        // created by a running sample container. A synthetic equivalent keeps
        // the coverage portable: .gitignore already ignores TestResults/
        // anywhere in the tree.
        var directory = Path.Combine(RepoRoot, "docs", "TestResults");
        var probe = Path.Combine(directory, "ignored-probe-" + Guid.NewGuid().ToString("N") + ".md");
        var createdDirectory = !Directory.Exists(directory);

        try
        {
            Directory.CreateDirectory(directory);
            File.WriteAllText(probe, "# probe" + Environment.NewLine);
            Assert.That(File.Exists(probe), Is.True, "The probe file was not created.");

            var tracked = HygieneFiles.EnumerateTextFiles(RepoRoot, CoreHygieneScope.Value).ToList();
            Assert.That(tracked, Is.Not.Empty, "The tracked enumeration returned nothing at all.");
            Assert.That(tracked, Does.Not.Contain(probe),
                "A gitignored file reached the content gates.");
        }
        finally
        {
            if (File.Exists(probe)) File.Delete(probe);
            if (createdDirectory && Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public void Every_tracked_file_classifies_as_text_or_binary()
    {
        // The allow-list's failure mode is silence: an unrecognised extension
        // is simply not scanned, and no gate result would ever reveal it.
        // This is the only assertion in the estate that would notice.
        var tracked = HygieneRepository.TrackedFiles(RepoRoot);
        Assert.That(tracked.Count, Is.GreaterThan(1000),
            "git reported implausibly few tracked files, so this audit proves nothing.");

        var unclassified = new List<string>();
        var examined = 0;
        foreach (var file in tracked)
        {
            if (HygieneRepository.HasExcludedSegment(file)) continue;
            examined++;
            if (HygieneFiles.Classify(file) == HygieneFileKind.Unclassified)
            {
                unclassified.Add(Path.GetRelativePath(RepoRoot, file).Replace('\\', '/'));
            }
        }

        Assert.That(examined, Is.GreaterThan(1000), "Too few tracked files survived the segment filter.");
        Assert.That(unclassified, Is.Empty,
            "Tracked files match no classification rule and would be silently excluded from every content scan:"
            + Environment.NewLine + string.Join(Environment.NewLine, unclassified));
    }

    [Test]
    public void Classify_recognises_text_binary_and_unclassified()
    {
        Assert.Multiple(() =>
        {
            Assert.That(HygieneFiles.Classify("a/b/File.cs"), Is.EqualTo(HygieneFileKind.Text));
            Assert.That(HygieneFiles.Classify("a/b/Readme.md"), Is.EqualTo(HygieneFileKind.Text));
            Assert.That(HygieneFiles.Classify("a/b/LICENSE"), Is.EqualTo(HygieneFileKind.Text));
            Assert.That(HygieneFiles.Classify("a/b/logo.png"), Is.EqualTo(HygieneFileKind.Binary));
            Assert.That(HygieneFiles.Classify("a/b/font.ttf"), Is.EqualTo(HygieneFileKind.Binary));

            // The two shapes a deny-list assumed were text and read anyway.
            Assert.That(HygieneFiles.Classify("a/b/payload.unheardof"), Is.EqualTo(HygieneFileKind.Unclassified));
            Assert.That(HygieneFiles.Classify("a/b/mystery"), Is.EqualTo(HygieneFileKind.Unclassified));
        });
    }

    [Test]
    public void ShouldScan_throws_for_an_unclassified_file_rather_than_skipping_it()
    {
        Assert.Multiple(() =>
        {
            Assert.That(HygieneFiles.ShouldScan(Path.Combine(RepoRoot, "README.md"), RepoRoot), Is.True);
            Assert.That(HygieneFiles.ShouldScan(Path.Combine(RepoRoot, "logo.png"), RepoRoot), Is.False);
        });

        var unclassified = Path.Combine(RepoRoot, "docs", "payload.unheardof");
        var ex = Assert.Throws<InvalidOperationException>(() => HygieneFiles.ShouldScan(unclassified, RepoRoot));
        Assert.That(ex!.Message, Does.Contain("docs/payload.unheardof"),
            "The guard must name the offending file, or classifying it becomes a hunt.");
        Assert.That(ex.Message, Does.Contain("TextExtensions"),
            "The guard must say how to resolve itself.");
    }

    [Test]
    public void TryReadLines_reports_an_unreadable_path_instead_of_throwing()
    {
        // A directory is the portable stand-in for the locked file that
        // aborted the whole em-dash gate: unreadable on every platform,
        // without depending on Windows mandatory locking.
        var unreadable = Path.Combine(RepoRoot, "docs");

        var lines = HygieneFiles.TryReadLines(unreadable, out var failure);
        Assert.Multiple(() =>
        {
            Assert.That(lines, Is.Null, "An unreadable path must not yield content.");
            Assert.That(failure, Is.Not.Null.And.Contains("docs"),
                "The failure must name the path, so it reads as an environment condition.");
        });
    }

    [Test]
    public void TryReadText_reports_an_unreadable_path_instead_of_throwing()
    {
        var unreadable = Path.Combine(RepoRoot, "docs");

        var text = HygieneFiles.TryReadText(unreadable, out var failure);
        Assert.Multiple(() =>
        {
            Assert.That(text, Is.Null, "An unreadable path must not yield content.");
            Assert.That(failure, Is.Not.Null.And.Contains("docs"),
                "The failure must name the path, so it reads as an environment condition.");
        });
    }

    [Test]
    public void TryRead_helpers_return_content_and_no_failure_for_a_readable_file()
    {
        // The counterpart to the two tests above: a guard that reported every
        // file as unreadable would satisfy them and scan nothing.
        var readable = Path.Combine(RepoRoot, "README.md");

        var lines = HygieneFiles.TryReadLines(readable, out var lineFailure);
        var text = HygieneFiles.TryReadText(readable, out var textFailure);

        Assert.Multiple(() =>
        {
            Assert.That(lineFailure, Is.Null);
            Assert.That(lines, Is.Not.Null.And.Not.Empty);
            Assert.That(textFailure, Is.Null);
            Assert.That(text, Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public void Tracked_files_are_sorted_so_a_directory_prefix_is_one_contiguous_run()
    {
        // EnumerateTracked binary-searches for a directory prefix and stops at
        // the first entry that no longer carries it. That is only correct if
        // the array is sorted with the very comparer the prefix test uses;
        // a mismatch would truncate a slice silently and under-report.
        var tracked = HygieneRepository.TrackedFiles(RepoRoot);
        Assert.That(tracked.Count, Is.GreaterThan(1000));

        for (var i = 1; i < tracked.Count; i++)
        {
            Assert.That(
                StringComparer.OrdinalIgnoreCase.Compare(tracked[i - 1], tracked[i]),
                Is.LessThanOrEqualTo(0),
                "Tracked files are not ordered by the comparer the prefix search relies on.");
        }
    }

    [Test]
    public void Slice_enumeration_reaches_every_tracked_file_under_the_slice_root()
    {
        // The binary-search range must not lose entries. Compare it against
        // an independent linear filter over the same tracked set.
        var sliceRoot = Path.Combine(RepoRoot, "src", "lattice") + Path.DirectorySeparatorChar;
        var expected = HygieneRepository.TrackedFiles(RepoRoot)
            .Where(f => f.StartsWith(sliceRoot, StringComparison.OrdinalIgnoreCase))
            .Where(f => !HygieneRepository.HasExcludedSegment(f))
            .Where(f => f.EndsWith(".cs", StringComparison.OrdinalIgnoreCase))
            .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
            .ToList();

        Assert.That(expected, Is.Not.Empty, "No tracked C# files under src/lattice, so this proves nothing.");

        var actual = HygieneRepository
            .EnumerateFiles(Path.Combine(RepoRoot, "src", "lattice"), "*.cs")
            .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
            .ToList();

        Assert.That(actual, Is.EqualTo(expected),
            "The prefix-ranged enumeration disagrees with a linear scan of the same tracked set.");
    }
}
