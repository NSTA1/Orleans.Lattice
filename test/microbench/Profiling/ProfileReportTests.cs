using System.Text.Json;
using Orleans.Lattice.Benchmark.Microbench.Profiling;

namespace Orleans.Lattice.Benchmark.Microbench.Tests.Profiling;

/// <summary>
/// Tests for <see cref="ProfileReport.WriteJson"/> on-disk shape and
/// round-trippability.
/// </summary>
[TestFixture]
public sealed class ProfileReportTests
{
    [Test]
    public void WriteJson_emits_indented_utf8_with_expected_top_level_keys()
    {
        var report = new ProfileReport(
            RunId: "run-1",
            GitSha: "abcdef0",
            CapturedAt: new DateTime(2026, 5, 12, 13, 0, 0, DateTimeKind.Utc),
            Mode: ProfileMode.Alloc,
            DurationMs: 1234,
            TotalAllocationsB: 4096,
            TotalCpuSamples: 0,
            TopAllocators: new[]
            {
                new ProfileFrameRow("My.Type.Method", "MyAsm", 4096, 100.0, 0, 0.0),
            },
            TopCpu: Array.Empty<ProfileFrameRow>());

        var path = Path.Combine(Path.GetTempPath(), $"orleans-lattice-profile-{Guid.NewGuid():N}.json");
        try
        {
            report.WriteJson(path);
            Assert.That(File.Exists(path), Is.True);

            // No BOM, UTF-8, indented.
            var raw = File.ReadAllBytes(path);
            Assert.That(raw.Length, Is.GreaterThan(0));
            // UTF-8 BOM is 0xEF 0xBB 0xBF; must be absent.
            Assert.That(raw.Take(3).ToArray(), Is.Not.EqualTo(new byte[] { 0xEF, 0xBB, 0xBF }));

            var json = File.ReadAllText(path);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.That(root.GetProperty("run_id").GetString(), Is.EqualTo("run-1"));
            Assert.That(root.GetProperty("git_sha").GetString(), Is.EqualTo("abcdef0"));
            Assert.That(root.GetProperty("mode").GetString(), Is.EqualTo("alloc"));
            Assert.That(root.GetProperty("duration_ms").GetInt64(), Is.EqualTo(1234));
            Assert.That(root.GetProperty("total_allocations_b").GetInt64(), Is.EqualTo(4096));
            Assert.That(root.GetProperty("total_cpu_samples").GetInt64(), Is.EqualTo(0));

            var allocators = root.GetProperty("top_allocators");
            Assert.That(allocators.GetArrayLength(), Is.EqualTo(1));
            var first = allocators[0];
            Assert.That(first.GetProperty("method").GetString(), Is.EqualTo("My.Type.Method"));
            Assert.That(first.GetProperty("module").GetString(), Is.EqualTo("MyAsm"));
            Assert.That(first.GetProperty("alloc_b").GetInt64(), Is.EqualTo(4096));
            Assert.That(first.GetProperty("alloc_pct").GetDouble(), Is.EqualTo(100.0).Within(0.001));

            var cpu = root.GetProperty("top_cpu");
            Assert.That(cpu.GetArrayLength(), Is.EqualTo(0));
        }
        finally
        {
            try { File.Delete(path); } catch { /* best effort */ }
        }
    }

    [Test]
    public void WriteJson_creates_parent_directory()
    {
        var subDir = Path.Combine(Path.GetTempPath(), $"orleans-lattice-profile-dir-{Guid.NewGuid():N}");
        var path = Path.Combine(subDir, "profile.json");
        try
        {
            var report = new ProfileReport(
                RunId: "r", GitSha: "g",
                CapturedAt: DateTime.UtcNow,
                Mode: ProfileMode.Off,
                DurationMs: 0,
                TotalAllocationsB: 0,
                TotalCpuSamples: 0,
                TopAllocators: Array.Empty<ProfileFrameRow>(),
                TopCpu: Array.Empty<ProfileFrameRow>());
            report.WriteJson(path);
            Assert.That(File.Exists(path), Is.True);
        }
        finally
        {
            try { Directory.Delete(subDir, recursive: true); } catch { /* best effort */ }
        }
    }

    [Test]
    public void WriteJson_throws_on_null_or_whitespace_path()
    {
        var report = new ProfileReport(
            RunId: "r", GitSha: "g",
            CapturedAt: DateTime.UtcNow,
            Mode: ProfileMode.Off,
            DurationMs: 0,
            TotalAllocationsB: 0,
            TotalCpuSamples: 0,
            TopAllocators: Array.Empty<ProfileFrameRow>(),
            TopCpu: Array.Empty<ProfileFrameRow>());
        Assert.That(() => report.WriteJson(null!), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => report.WriteJson("   "), Throws.InstanceOf<ArgumentException>());
    }
}
