using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// The checkpoint's parts that need no browser (#5360): the volatile-text masks, the
/// host-fault log reader, the id rule, the manifest's round trip, and the run's viewport.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class CheckpointSeamTests
{
    private string _scratch = null!;

    /// <summary>A scratch directory per test.</summary>
    [SetUp]
    public void CreateScratch()
    {
        _scratch = Path.Combine(Path.GetTempPath(), "lattice-e2e-checkpoint-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_scratch);
    }

    /// <summary>Removes the scratch directory.</summary>
    [TearDown]
    public void RemoveScratch() => Directory.Delete(_scratch, recursive: true);

    /// <summary>Run-to-run values are masked; names a journey asserts on are not.</summary>
    [TestCase("127.0.0.1:43211", true)]
    [TestCase("https://127.0.0.1:5001/", true)]
    [TestCase("2026-10-31 12:00:05 UTC", true)]
    [TestCase("12:00", true)]
    [TestCase("3 s ago", true)]
    [TestCase("12.5/s", true)]
    [TestCase("4,210 ops/s", true)]
    [TestCase("3f2504e0-4f89-11d3-9a0c-0305e82c3301", true)]
    [TestCase("factory-orders", false)]
    [TestCase("Connected", false)]
    [TestCase("order/000017", false)]
    [TestCase("orders-baseline", false)]
    [TestCase("240 entries", false)]
    public void Volatile_text_is_masked_and_names_are_not(string text, bool masked) =>
        Assert.That(JourneyMasks.VolatileText.IsMatch(text), Is.EqualTo(masked));

    /// <summary>An error record and its indented lines fail; a warning does not; each record is read once.</summary>
    [Test]
    public void A_host_error_is_reported_once_with_its_detail()
    {
        var explorer = Path.Combine(_scratch, "west-explorer-0.log");
        var silo = Path.Combine(_scratch, "west-silo-0.log");
        File.WriteAllLines(explorer,
        [
            "12:00:00.000 [harness] --- start #1: dotnet JourneyExplorer.dll ---",
            "12:00:01.000 [out] warn: Microsoft.AspNetCore.Server[1]",
            "12:00:01.001 [out]       a warning is not a fault",
            "12:00:02.000 [out] fail: Microsoft.AspNetCore.Components.Server.Circuits.CircuitHost[111]",
            "12:00:02.001 [out]       Unhandled exception in circuit 'abc'.",
            "12:00:02.002 [out]       System.InvalidOperationException: deliberate",
            "12:00:03.000 [out] info: Orleans.Lattice[0]",
        ]);
        File.WriteAllLines(silo, ["12:00:02.000 [out] crit: Orleans.Runtime.Silo[0]", "12:00:02.001 [out]       a silo went down"]);
        var log = new HostFaultLog(() =>
        [
            new HostLogSource("west-explorer-0", explorer, FailsCheckpoint: true),
            new HostLogSource("west-silo-0", silo, FailsCheckpoint: false),
            new HostLogSource("never-started", Path.Combine(_scratch, "missing.log"), FailsCheckpoint: true),
        ]);

        var (failures, notes) = log.Drain();
        var (later, laterNotes) = log.Drain();

        Assert.Multiple(() =>
        {
            Assert.That(failures, Has.Count.EqualTo(1));
            Assert.That(failures[0], Does.StartWith("[west-explorer-0] fail: Microsoft.AspNetCore.Components.Server.Circuits.CircuitHost[111]"));
            Assert.That(failures[0], Does.Contain("System.InvalidOperationException: deliberate"));
            Assert.That(failures[0], Does.Not.Contain("info:"));
            Assert.That(notes, Has.Count.EqualTo(1).And.Some.Contains("a silo went down"));
            Assert.That(later, Is.Empty, "a record is attributed to one checkpoint only");
            Assert.That(laterNotes, Is.Empty);
        });
    }

    /// <summary>A record written after a checkpoint is read at the next one; a tolerated one is only noted.</summary>
    [Test]
    public void A_later_record_is_read_at_the_next_checkpoint_and_a_tolerated_one_is_only_noted()
    {
        var explorer = Path.Combine(_scratch, "west-explorer-0.log");
        File.WriteAllText(explorer, string.Empty);
        var log = new HostFaultLog(() => [new HostLogSource("west-explorer-0", explorer, FailsCheckpoint: true)]);
        Assert.That(log.Drain().Failures, Is.Empty);

        File.AppendAllLines(explorer, ["12:00:05.000 [out] fail: Grpc.Net.Client[0]", "12:00:05.001 [out]       Call failed: Unavailable while silo-0 was killed"]);
        log.Tolerate(new System.Text.RegularExpressions.Regex("silo-0 was killed"));
        File.AppendAllLines(explorer, ["12:00:06.000 [out] fail: Orleans.Lattice.Explorer[0]", "12:00:06.001 [out]       not tolerated"]);

        var (failures, notes) = log.Drain();
        Assert.Multiple(() =>
        {
            Assert.That(failures, Has.Count.EqualTo(1).And.Some.Contains("not tolerated"));
            Assert.That(notes, Has.Count.EqualTo(1).And.Some.Contains("silo-0 was killed"));
        });
    }

    /// <summary>Ids become file names, so only lower-case kebab-case is accepted.</summary>
    [TestCase("sign-in", true)]
    [TestCase("a3", true)]
    [TestCase("f2-personas", true)]
    [TestCase("Sign-in", false)]
    [TestCase("sign in", false)]
    [TestCase("-sign", false)]
    [TestCase("sign--in", false)]
    [TestCase("../escape", false)]
    [TestCase("", false)]
    public void Only_kebab_case_ids_are_accepted(string id, bool accepted)
    {
        if (accepted)
        {
            Assert.That(JourneyNames.RequireSlug(id, "id"), Is.EqualTo(id));
            Assert.That(new JourneyStepAttribute(id, "A step").Id, Is.EqualTo(id));
            Assert.That(new JourneyAttribute(id).JourneyId, Is.EqualTo(id));
        }
        else
        {
            Assert.That(() => JourneyNames.RequireSlug(id, "id"), Throws.ArgumentException);
        }
    }

    /// <summary>The manifest reads back what it wrote, and renders one captioned row per checkpoint.</summary>
    [Test]
    public void The_manifest_round_trips_and_renders_in_order()
    {
        var manifest = new JourneyManifest
        {
            Journey = "f2-personas",
            Commit = "0123456789abcdef",
            RunId = "42",
            RunAttempt = "1",
            Viewport = JourneyViewport.Desktop.Name,
            Width = JourneyViewport.Desktop.Width,
            Height = JourneyViewport.Desktop.Height,
            Entries =
            [
                new(1, "sign-in", "signed-out", "Olivia arrives signed out", "olivia", "001-sign-in-signed-out.png", new string('a', 64), Passed: true),
                new(2, "sign-in", "wrong-password", "A | wrong password", "olivia", "002-sign-in-wrong-password.png", new string('b', 64), Passed: false),
            ],
        };
        var path = Path.Combine(_scratch, JourneyManifest.FileName);

        manifest.Write(path);
        var read = JourneyManifest.Read(path);
        var markdown = read.ToMarkdown();

        Assert.Multiple(() =>
        {
            Assert.That(read.Journey, Is.EqualTo("f2-personas"));
            Assert.That(read.Commit, Is.EqualTo("0123456789abcdef"));
            Assert.That(read.RunId, Is.EqualTo("42"));
            Assert.That((read.Viewport, read.Width, read.Height), Is.EqualTo(("desktop", 1440, 900)));
            Assert.That(read.Entries, Is.EqualTo(manifest.Entries));
            Assert.That(File.Exists(path + ".tmp"), Is.False);
            Assert.That(markdown.IndexOf("Olivia arrives signed out", StringComparison.Ordinal), Is.LessThan(markdown.IndexOf("A \\| wrong password", StringComparison.Ordinal)));
            Assert.That(markdown, Does.Contain("| failed |"));
            Assert.That(read.ToHtml(), Does.Contain("<td>Olivia arrives signed out</td>").And.Contain("class=\"failed\">failed"));
        });
    }

    /// <summary>The fixed viewports: 1440x900 on a desktop run, 390x844 (compact chrome) on a narrow one.</summary>
    [Test]
    public void The_viewports_are_fixed()
    {
        Assert.Multiple(() =>
        {
            Assert.That((JourneyViewport.Desktop.Width, JourneyViewport.Desktop.Height, JourneyViewport.Desktop.IsCompact), Is.EqualTo((1440, 900, false)));
            Assert.That((JourneyViewport.Narrow.Width, JourneyViewport.Narrow.Height, JourneyViewport.Narrow.IsCompact), Is.EqualTo((390, 844, true)));
            Assert.That(JourneyViewport.Run, Is.EqualTo(
                Environment.GetEnvironmentVariable(JourneyViewport.EnvironmentVariable) == "narrow" ? JourneyViewport.Narrow : JourneyViewport.Desktop));
        });
    }
}
