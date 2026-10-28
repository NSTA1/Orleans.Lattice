using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Pins the write-once, fail-closed contract of <see cref="TlcEvidenceRetention"/>
/// without a Java runtime: a retained run records its exact inputs, argument
/// vector, output and native exit code; a missing root is refused rather than
/// created; and an existing file is never overwritten.
/// </summary>
[TestFixture]
public sealed class TlcEvidenceRetentionTests
{
    private string _work = string.Empty;

    [SetUp]
    public void CreateWorkDirectory()
    {
        _work = Path.Combine(Path.GetTempPath(), $"lattice-tlc-evidence-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_work);
    }

    [TearDown]
    public void RemoveWorkDirectory()
    {
        if (Directory.Exists(_work))
        {
            Directory.Delete(_work, recursive: true);
        }
    }

    [Test]
    public void A_retained_run_records_its_inputs_argv_output_and_nonzero_exit_code()
    {
        var scratch = Directory.CreateDirectory(Path.Combine(_work, "scratch")).FullName;
        var root = Directory.CreateDirectory(Path.Combine(_work, "evidence")).FullName;
        File.WriteAllText(Path.Combine(scratch, "Probe.tla"), "---- MODULE Probe ----\n====\n");
        File.WriteAllText(Path.Combine(scratch, "Probe.cfg"), "INVARIANT TypeOK\n");
        File.WriteAllText(Path.Combine(scratch, "ignored.txt"), "not an input");

        var info = new ProcessStartInfo("java");
        info.ArgumentList.Add("tlc2.TLC");
        info.ArgumentList.Add("Probe.tla");

        // A clean banner with a failing exit code is exactly the shape a
        // banner-only verdict accepts; the receipt must keep the exit code.
        var arm = TlcEvidenceRetention.Retain(
            root, "Probe.Test", "Probe", scratch, info, 12,
            "Model checking completed. No error has been found.\n", "boom\n", TimeSpan.FromSeconds(3));

        Assert.Multiple(() =>
        {
            Assert.That(Path.GetDirectoryName(arm), Is.EqualTo(root));
            Assert.That(File.ReadAllText(Path.Combine(arm, "Probe.tla")), Does.Contain("MODULE Probe"));
            Assert.That(File.Exists(Path.Combine(arm, "Probe.cfg")), Is.True);
            Assert.That(File.Exists(Path.Combine(arm, "ignored.txt")), Is.False);
            Assert.That(File.ReadAllText(Path.Combine(arm, "argv.txt")), Is.EqualTo("java\ntlc2.TLC\nProbe.tla\n"));
            Assert.That(File.ReadAllText(Path.Combine(arm, "stderr.txt")), Is.EqualTo("boom\n"));
        });

        using var receipt = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(arm, "receipt.json")));
        Assert.Multiple(() =>
        {
            Assert.That(receipt.RootElement.GetProperty("exitCode").GetInt32(), Is.EqualTo(12));
            Assert.That(receipt.RootElement.GetProperty("test").GetString(), Is.EqualTo("Probe.Test"));
            Assert.That(receipt.RootElement.GetProperty("elapsedMilliseconds").GetInt64(), Is.EqualTo(3000));
            Assert.That(
                receipt.RootElement.GetProperty("inputs").EnumerateObject().Select(p => p.Name),
                Is.EqualTo(new[] { "Probe.cfg", "Probe.tla" }));
        });
    }

    [Test]
    public void A_missing_evidence_root_is_refused_rather_than_created()
    {
        var scratch = Directory.CreateDirectory(Path.Combine(_work, "scratch")).FullName;
        var missing = Path.Combine(_work, "no-such-root");

        Assert.Throws<DirectoryNotFoundException>(() => TlcEvidenceRetention.Retain(
            missing, "Probe.Test", "Probe", scratch, new ProcessStartInfo("java"), 0, string.Empty, string.Empty, TimeSpan.Zero));
        Assert.That(Directory.Exists(missing), Is.False);
    }

    [Test]
    public void An_existing_evidence_file_is_never_overwritten()
    {
        var path = Path.Combine(_work, "receipt.json");
        File.WriteAllText(path, "original");

        Assert.Throws<IOException>(() => TlcEvidenceRetention.WriteNew(path, Encoding.UTF8.GetBytes("replacement")));
        Assert.That(File.ReadAllText(path), Is.EqualTo("original"));
    }

    [Test]
    public void Retention_is_off_unless_the_variable_names_a_root()
    {
        var previous = Environment.GetEnvironmentVariable(TlcEvidenceRetention.DirectoryVariable);
        try
        {
            Environment.SetEnvironmentVariable(TlcEvidenceRetention.DirectoryVariable, null);
            Assert.That(TlcEvidenceRetention.ConfiguredRoot, Is.Null);
            Environment.SetEnvironmentVariable(TlcEvidenceRetention.DirectoryVariable, _work);
            Assert.That(TlcEvidenceRetention.ConfiguredRoot, Is.EqualTo(_work));
        }
        finally
        {
            Environment.SetEnvironmentVariable(TlcEvidenceRetention.DirectoryVariable, previous);
        }
    }
}
