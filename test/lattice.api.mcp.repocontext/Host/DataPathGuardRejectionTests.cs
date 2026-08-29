using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit coverage for the two rejection arms of
/// <see cref="DataPathGuard.EnsureDirectoryWritable"/>: a data path that cannot
/// be created at all, and one that exists but the runtime UID cannot write to.
///
/// These are the arms that give the guard its purpose. The container's whole
/// durability story rests on the WAL and SQLite directories being real, writable
/// host mounts; a misconfigured mount that the guard waved through would let the
/// silo start and accept writes it silently cannot persist. Both arms must fail
/// fast with a message that names the offending path and says what to fix.
/// </summary>
[TestFixture]
public sealed class DataPathGuardRejectionTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-guard-reject-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        if (!Directory.Exists(_root))
        {
            return;
        }

        RestoreWritable(_root);
        Directory.Delete(_root, recursive: true);
    }

    [Test]
    public void A_directory_that_cannot_be_created_is_rejected()
    {
        // A file already occupies the path, so the directory can never be created.
        // This is the shape a mount typo takes when it lands on an existing file.
        var file = Path.Combine(_root, "occupied");
        File.WriteAllText(file, "not a directory");
        var target = Path.Combine(file, "wal");

        var ex = Assert.Throws<InvalidOperationException>(
            () => DataPathGuard.EnsureDirectoryWritable(target, "WAL"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("WAL").And.Contain("could not be created"));
            Assert.That(ex.InnerException, Is.InstanceOf<IOException>().Or.InstanceOf<UnauthorizedAccessException>(),
                "The original filesystem fault must be preserved for diagnosis.");
        });
    }

    [Test]
    public void An_existing_but_unwritable_directory_is_rejected()
    {
        var target = Path.Combine(_root, "readonly");
        Directory.CreateDirectory(target);

        if (!TryMakeUnwritable(target))
        {
            Assert.Ignore(
                "Could not revoke write permission on this platform or identity "
                + "(running elevated / as root bypasses the check).");
        }

        try
        {
            var ex = Assert.Throws<InvalidOperationException>(
                () => DataPathGuard.EnsureDirectoryWritable(target, "SQLite data"));

            Assert.Multiple(() =>
            {
                Assert.That(ex!.Message, Does.Contain("SQLite data").And.Contain("not writable"));
                Assert.That(ex.InnerException, Is.Not.Null);
            });
        }
        finally
        {
            RestoreWritable(target);
        }
    }

    [Test]
    public void A_writable_directory_is_still_accepted_after_permissions_are_restored()
    {
        var target = Path.Combine(_root, "restored");
        Directory.CreateDirectory(target);

        if (TryMakeUnwritable(target))
        {
            RestoreWritable(target);
        }

        Assert.That(() => DataPathGuard.EnsureDirectoryWritable(target, "data"), Throws.Nothing);
    }

    /// <summary>
    /// Revokes write permission on <paramref name="directory"/> for the current
    /// identity, returning <see langword="false"/> when the platform or the
    /// running identity makes that impossible (an elevated or root process
    /// bypasses the permission check, so the guard could not observe it either).
    /// </summary>
    private static bool TryMakeUnwritable(string directory)
    {
        try
        {
            if (OperatingSystem.IsWindows())
            {
                DenyWriteOnWindows(directory);
            }
            else
            {
                File.SetUnixFileMode(
                    directory,
                    UnixFileMode.UserRead | UnixFileMode.UserExecute);
            }
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException or PlatformNotSupportedException)
        {
            return false;
        }

        // Confirm the revocation actually bites before asserting on it.
        var probe = Path.Combine(directory, "permission-probe");
        try
        {
            File.WriteAllText(probe, "x");
            File.Delete(probe);
            return false;
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
        {
            return true;
        }
    }

    private static void RestoreWritable(string directory)
    {
        try
        {
            if (OperatingSystem.IsWindows())
            {
                AllowWriteOnWindows(directory);
            }
            else
            {
                File.SetUnixFileMode(
                    directory,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            }
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException or PlatformNotSupportedException)
        {
            // Best effort: teardown must not mask a test failure.
        }
    }

    [SupportedOSPlatform("windows")]
    private static void DenyWriteOnWindows(string directory)
    {
        var info = new DirectoryInfo(directory);
        var security = info.GetAccessControl();
        security.AddAccessRule(new FileSystemAccessRule(
            WindowsIdentity.GetCurrent().User!,
            FileSystemRights.CreateFiles | FileSystemRights.WriteData,
            AccessControlType.Deny));
        info.SetAccessControl(security);
    }

    [SupportedOSPlatform("windows")]
    private static void AllowWriteOnWindows(string directory)
    {
        var info = new DirectoryInfo(directory);
        var security = info.GetAccessControl();
        security.RemoveAccessRuleAll(new FileSystemAccessRule(
            WindowsIdentity.GetCurrent().User!,
            FileSystemRights.CreateFiles | FileSystemRights.WriteData,
            AccessControlType.Deny));
        info.SetAccessControl(security);
    }
}
