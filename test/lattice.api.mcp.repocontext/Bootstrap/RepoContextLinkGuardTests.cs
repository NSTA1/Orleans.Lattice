namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Unit tests for <see cref="RepoContextLinkGuard"/>, the read-time symlink check
/// every ingestion read site consults before opening a file under the repository
/// root.
/// </summary>
[TestFixture]
public sealed class RepoContextLinkGuardTests
{
    private string _root = string.Empty;
    private string _outside = string.Empty;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "rclg-root-" + Guid.NewGuid().ToString("N"));
        _outside = Path.Combine(Path.GetTempPath(), "rclg-out-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
        Directory.CreateDirectory(_outside);
    }

    [TearDown]
    public void TearDown()
    {
        foreach (var dir in new[] { _root, _outside })
        {
            try
            {
                if (Directory.Exists(dir))
                {
                    Directory.Delete(dir, recursive: true);
                }
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }
    }

    [Test]
    public void Regular_file_under_the_root_is_admitted()
    {
        Directory.CreateDirectory(Path.Combine(_root, "src"));
        var file = Path.Combine(_root, "src", "a.cs");
        File.WriteAllText(file, "x");

        Assert.That(RepoContextLinkGuard.TraversesLink(_root, file), Is.False);
    }

    [Test]
    public void Missing_file_is_not_reported_as_a_link()
    {
        Assert.That(RepoContextLinkGuard.TraversesLink(_root, Path.Combine(_root, "gone", "a.cs")), Is.False);
    }

    [Test]
    public void Path_outside_the_root_is_refused()
    {
        var file = Path.Combine(_outside, "secret.txt");
        File.WriteAllText(file, "secret");

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextLinkGuard.TraversesLink(_root, file), Is.True);
            Assert.That(
                RepoContextLinkGuard.TraversesLink(_root, Path.Combine(_root, "..", Path.GetFileName(_outside), "secret.txt")),
                Is.True,
                "A dot-dot relative path that escapes the root is refused lexically.");
            Assert.That(RepoContextLinkGuard.TraversesLink(_root, _root), Is.True,
                "The root itself is not a file under the root.");
        });
    }

    [Test]
    public void Symlinked_file_is_refused()
    {
        var target = Path.Combine(_outside, "secret.txt");
        File.WriteAllText(target, "secret");
        var link = Path.Combine(_root, "link.cs");
        if (!TryCreateLink(() => File.CreateSymbolicLink(link, target)))
        {
            return;
        }

        Assert.That(RepoContextLinkGuard.TraversesLink(_root, link), Is.True);
    }

    [Test]
    public void File_under_a_symlinked_directory_is_refused()
    {
        File.WriteAllText(Path.Combine(_outside, "secret.txt"), "secret");
        var link = Path.Combine(_root, "linked");
        if (!TryCreateLink(() => Directory.CreateSymbolicLink(link, _outside)))
        {
            return;
        }

        Assert.That(RepoContextLinkGuard.TraversesLink(_root, Path.Combine(link, "secret.txt")), Is.True);
    }

    private static bool TryCreateLink(Action create)
    {
        try
        {
            create();
            return true;
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException or PlatformNotSupportedException)
        {
            Assert.Ignore("Creating a symbolic link is not permitted in this environment.");
            return false;
        }
    }
}
