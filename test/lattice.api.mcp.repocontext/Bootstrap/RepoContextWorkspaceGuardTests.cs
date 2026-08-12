using System.IO;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextWorkspaceGuard"/>: the fail-closed workspace
/// boundary that resolves a caller-supplied path to its real on-disk location and
/// admits it only when it sits inside a configured workspace root. Covers the
/// disabled (no-root) pass-through, the in-bounds admit, and the out-of-bounds
/// refusals for lexical <c>..</c> escape, an unrelated absolute path, a
/// prefix-sibling directory, and a symbolic-link escape.
/// </summary>
[TestFixture]
public sealed class RepoContextWorkspaceGuardTests
{
    private string _workspace = null!;
    private string _repo = null!;
    private string _outside = null!;

    [SetUp]
    public void SetUp()
    {
        var scratch = Path.Combine(Path.GetTempPath(), "repoctx-guard-" + Guid.NewGuid().ToString("N"));
        _workspace = Path.Combine(scratch, "workspace");
        _repo = Path.Combine(_workspace, "repo");
        _outside = Path.Combine(scratch, "outside");

        Directory.CreateDirectory(_repo);
        Directory.CreateDirectory(_outside);
    }

    [TearDown]
    public void TearDown()
    {
        var scratch = Directory.GetParent(_workspace)?.FullName;
        if (scratch is not null && Directory.Exists(scratch))
        {
            try
            {
                Directory.Delete(scratch, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup; a lingering handle must not fail the test.
            }
        }
    }

    [Test]
    public void A_guard_with_no_roots_is_not_enforcing()
        => Assert.That(new RepoContextWorkspaceGuard([]).IsEnforcing, Is.False);

    [Test]
    public void A_guard_with_a_root_is_enforcing()
        => Assert.That(new RepoContextWorkspaceGuard([_workspace]).IsEnforcing, Is.True);

    [Test]
    public void A_disabled_guard_passes_a_path_through_normalised()
    {
        var guard = new RepoContextWorkspaceGuard([]);
        Assert.That(guard.Resolve(_outside), Is.EqualTo(Path.GetFullPath(_outside)));
    }

    [Test]
    public void An_empty_or_whitespace_root_is_ignored()
    {
        var guard = new RepoContextWorkspaceGuard(new[] { "  ", string.Empty });
        Assert.That(guard.IsEnforcing, Is.False);
    }

    [Test]
    public void A_path_inside_the_workspace_is_admitted()
    {
        var guard = new RepoContextWorkspaceGuard([_workspace]);
        Assert.That(guard.Resolve(_repo), Is.EqualTo(Path.GetFullPath(_repo)));
    }

    [Test]
    public void The_workspace_root_itself_is_admitted()
    {
        var guard = new RepoContextWorkspaceGuard([_workspace]);
        Assert.That(guard.Resolve(_workspace), Is.EqualTo(Path.GetFullPath(_workspace)));
    }

    [Test]
    public void A_lexical_dotdot_escape_is_refused()
    {
        var guard = new RepoContextWorkspaceGuard([_workspace]);
        var escape = Path.Combine(_repo, "..", "..", "outside");
        Assert.Throws<RepoContextWorkspaceViolationException>(() => guard.Resolve(escape));
    }

    [Test]
    public void An_unrelated_absolute_path_is_refused()
    {
        var guard = new RepoContextWorkspaceGuard([_workspace]);
        Assert.Throws<RepoContextWorkspaceViolationException>(() => guard.Resolve(_outside));
    }

    [Test]
    public void A_prefix_sibling_directory_is_refused()
    {
        // A sibling whose path shares the root's string prefix ("workspace" vs
        // "workspace-other") must not be admitted by a naive StartsWith.
        var sibling = _workspace + "-other";
        Directory.CreateDirectory(sibling);

        var guard = new RepoContextWorkspaceGuard([_workspace]);
        Assert.Throws<RepoContextWorkspaceViolationException>(() => guard.Resolve(sibling));
    }

    [Test]
    public void A_symbolic_link_that_escapes_the_workspace_is_refused()
    {
        var linkPath = Path.Combine(_workspace, "escape-link");
        try
        {
            Directory.CreateSymbolicLink(linkPath, _outside);
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException or PlatformNotSupportedException)
        {
            Assert.Ignore("Creating a symbolic link is not permitted in this environment.");
            return;
        }

        var guard = new RepoContextWorkspaceGuard([_workspace]);
        Assert.Throws<RepoContextWorkspaceViolationException>(() => guard.Resolve(linkPath));
    }

    [Test]
    public void A_null_or_empty_requested_path_is_rejected()
    {
        var guard = new RepoContextWorkspaceGuard([_workspace]);
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentException>(() => guard.Resolve(null!));
            Assert.Throws<ArgumentException>(() => guard.Resolve("   "));
        });
    }

    [Test]
    public void A_null_allowed_roots_enumerable_is_rejected()
        => Assert.Throws<ArgumentNullException>(() => new RepoContextWorkspaceGuard(null!));
}
