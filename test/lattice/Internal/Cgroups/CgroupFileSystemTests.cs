using Orleans.Lattice.Internal.Cgroups;

namespace Orleans.Lattice.Tests.Internal.Cgroups;

/// <summary>
/// Covers <see cref="CgroupFileSystem"/>, the one accessor every cgroup reader
/// goes through (issue #2828). The probe and the defensive read are exercised
/// through their platform-independent cores against ordinary temporary files,
/// so the rules hold on every operating system the suite runs on, not only
/// inside a Linux container.
/// </summary>
[TestFixture]
public sealed class CgroupFileSystemTests
{
    private string _directory = null!;

    [SetUp]
    public void SetUp()
    {
        _directory = Path.Combine(Path.GetTempPath(), "cgroupfs-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_directory);
    }

    [TearDown]
    public void TearDown() => Directory.Delete(_directory, recursive: true);

    private string Write(string name, string content)
    {
        var path = Path.Combine(_directory, name);
        File.WriteAllText(path, content);
        return path;
    }

    private string Missing(string name) => Path.Combine(_directory, name);

    [Test]
    public void ProbeFirstKnown_returns_the_first_path_that_parses_to_a_known_value()
    {
        var first = Write("first", "11");
        var second = Write("second", "22");

        Assert.That(
            CgroupFileSystem.ProbeFirstKnown<long>([first, second], ContainerMemoryLimit.Parse),
            Is.EqualTo(11L));
    }

    [Test]
    public void ProbeFirstKnown_skips_a_path_that_parses_to_unknown()
    {
        // The rule that makes the probe "first known value wins": a v2 file that
        // reads "max" exists, but says nothing, so the next candidate is tried.
        var unlimited = Write("memory.max", "max\n");
        var limit = Write("memory.limit_in_bytes", "4096\n");

        Assert.That(
            CgroupFileSystem.ProbeFirstKnown<long>([unlimited, limit], ContainerMemoryLimit.Parse),
            Is.EqualTo(4096L));
    }

    [Test]
    public void ProbeFirstKnown_skips_a_missing_path()
    {
        var limit = Write("present", "8192");

        Assert.That(
            CgroupFileSystem.ProbeFirstKnown<long>([Missing("absent"), limit], ContainerMemoryLimit.Parse),
            Is.EqualTo(8192L));
    }

    [Test]
    public void ProbeFirstKnown_returns_null_when_nothing_is_known()
    {
        var unlimited = Write("memory.max", "max");

        Assert.Multiple(() =>
        {
            Assert.That(
                CgroupFileSystem.ProbeFirstKnown<long>([unlimited, Missing("absent")], ContainerMemoryLimit.Parse),
                Is.Null);
            Assert.That(
                CgroupFileSystem.ProbeFirstKnown<long>([], ContainerMemoryLimit.Parse),
                Is.Null);
        });
    }

    [Test]
    public void ProbeFirstKnown_rejects_a_null_parser()
    {
        Assert.Throws<ArgumentNullException>(
            () => CgroupFileSystem.ProbeFirstKnown<long>([Missing("absent")], null!));
    }

    [Test]
    public void TryReadFile_returns_the_content_of_a_present_file()
    {
        Assert.That(CgroupFileSystem.TryReadFile(Write("cpu.max", "200000 100000")), Is.EqualTo("200000 100000"));
    }

    [Test]
    public void TryReadFile_degrades_a_missing_file_and_a_directory_to_null()
    {
        Assert.Multiple(() =>
        {
            Assert.That(CgroupFileSystem.TryReadFile(Missing("absent")), Is.Null);
            Assert.That(CgroupFileSystem.TryReadFile(_directory), Is.Null, "a directory is not a readable cgroup file");
        });
    }

    [Test]
    public void IsSupported_is_true_only_on_linux()
    {
        Assert.That(CgroupFileSystem.IsSupported, Is.EqualTo(OperatingSystem.IsLinux()));
    }

    [Test]
    public void Public_entry_points_report_unknown_without_reading_off_linux()
    {
        // Off Linux the canonical paths resolve against the current drive root,
        // where a stray file would be believed; the accessor must not look. The
        // file here is real and readable, so a null proves the short-circuit.
        if (OperatingSystem.IsLinux())
        {
            Assert.Ignore("The short-circuit only applies off Linux.");
        }

        var real = Write("memory.max", "4096");

        Assert.Multiple(() =>
        {
            Assert.That(CgroupFileSystem.TryReadAllText(real), Is.Null);
            Assert.That(CgroupFileSystem.ReadFirstKnown<long>([real], ContainerMemoryLimit.Parse), Is.Null);
        });
    }

    [Test]
    public void Public_entry_points_read_through_on_linux()
    {
        if (!OperatingSystem.IsLinux())
        {
            Assert.Ignore("Reading through only applies on Linux.");
        }

        var real = Write("memory.max", "4096");

        Assert.Multiple(() =>
        {
            Assert.That(CgroupFileSystem.TryReadAllText(real), Is.EqualTo("4096"));
            Assert.That(CgroupFileSystem.ReadFirstKnown<long>([real], ContainerMemoryLimit.Parse), Is.EqualTo(4096L));
        });
    }
}
