using System.Text;
using System.Text.Json;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Explorer.Tests.UI.Design;
using F1 = Orleans.Lattice.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// Keeps the hostile-bundle browser fixtures (run by U1, issue #3832) well formed: every
/// fixture names files that exist, valid bundle paths, operations from the bridge
/// vocabulary, and an expected failure the frame host can actually show.
/// </summary>
[TestFixture]
public sealed class HostileBundleFixtureTests
{
    private const string Root = "test/lattice.explorer/UI/AppFrame/Fixtures/HostileBundles";

    private static IEnumerable<string> Fixtures() =>
        Directory.GetDirectories(ShellStylesheets.Absolute(Root)).Select(Path.GetFileName).OfType<string>().Order(StringComparer.Ordinal);

    [Test]
    public void The_fixture_set_covers_every_documented_attack()
    {
        Assert.That(Fixtures(), Is.EquivalentTo(new[]
        {
            "digest-tamper", "entry-script", "exfiltrate", "flood", "forge-ready", "navigate-top", "physical-tree", "self-reload", "unknown-operation",
        }));
    }

    [TestCaseSource(nameof(Fixtures))]
    public void Every_fixture_is_well_formed(string name)
    {
        var folder = Path.Combine(ShellStylesheets.Absolute(Root), name);
        var manifest = JsonDocument.Parse(File.ReadAllText(Path.Combine(folder, "fixture.json"))).RootElement;
        var assets = Directory.GetFiles(folder).Select(Path.GetFileName).Where(file => file != "fixture.json").ToHashSet(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(manifest.GetProperty("name").GetString(), Is.EqualTo(name));
            Assert.That(assets, Does.Contain(manifest.GetProperty("entry").GetString()));
            foreach (var asset in assets)
            {
                Assert.That(AppFrameBundleRules.IsValidPath(asset), Is.True, asset);
                Assert.That(File.ReadAllBytes(Path.Combine(folder, asset!)).All(b => b < 0x80), Is.True, asset + " must be ASCII");
            }

            foreach (var script in manifest.GetProperty("scripts").EnumerateArray())
            {
                Assert.That(assets, Does.Contain(script.GetProperty("path").GetString()));
            }

            foreach (var grant in manifest.GetProperty("bridge").EnumerateArray())
            {
                Assert.That(F1.AppUiBridgeOperations.IsKnown(grant.GetProperty("operation").GetString()), Is.True);
            }

            var failure = manifest.GetProperty("expect").GetProperty("failure");
            if (failure.ValueKind == JsonValueKind.String)
            {
                Assert.That(Enum.TryParse<AppFrameFailure>(failure.GetString(), out _), Is.True, failure.GetString());
            }

            if (manifest.TryGetProperty("serve", out var serve))
            {
                foreach (var substitution in serve.EnumerateObject())
                {
                    Assert.That(assets, Does.Contain(substitution.Name));
                    Assert.That(assets, Does.Contain(substitution.Value.GetString()));
                }
            }
        });
    }

    [Test]
    public void The_entry_script_fixture_is_one_the_host_refuses()
    {
        var entry = File.ReadAllBytes(Path.Combine(ShellStylesheets.Absolute(Root), "entry-script", "index.html"));
        Assert.That(AppFrameBundleRules.IsValidEntryFragment(entry), Is.False);
    }

    [Test]
    public void The_benign_entries_are_ones_the_host_accepts()
    {
        var entry = File.ReadAllBytes(Path.Combine(ShellStylesheets.Absolute(Root), "flood", "index.html"));
        Assert.That(AppFrameBundleRules.IsValidEntryFragment(entry), Is.True, Encoding.ASCII.GetString(entry));
    }
}
