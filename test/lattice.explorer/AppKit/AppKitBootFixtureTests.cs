using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.AppKit;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// The browser-lane fixture page that U1 (#3832) runs against the real
/// bootstrap. These tests keep it honest without a browser: it speaks the
/// protocol's names, frames the bootstrap with only <c>allow-scripts</c>, covers
/// the ordering and every failure path it claims to, and decides no outcome by
/// timer.
/// </summary>
[TestFixture]
public sealed class AppKitBootFixtureTests
{
    private static readonly string Page = AppKitPaths.Read(AppKitPaths.Fixtures + "/boot-fixture.html");
    private static readonly string Script = AppKitPaths.Read(AppKitPaths.Fixtures + "/boot-fixture.js");

    [Test]
    public void The_fixture_page_loads_only_its_script()
    {
        var html = Regex.Replace(Page, "<!--.*?-->", string.Empty, RegexOptions.Singleline);

        Assert.Multiple(() =>
        {
            Assert.That(Regex.Matches(html, "<script\\b[^>]*>").Select(m => m.Value), Is.EqualTo(new[] { "<script src=\"boot-fixture.js\" defer>" }));
            Assert.That(Regex.IsMatch(html, @"\son[a-z]+\s*=|<style\b"), Is.False);
        });
    }

    [Test]
    public void The_fixture_frames_the_bootstrap_with_only_allow_scripts()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Script, Does.Contain("frame.setAttribute(\"sandbox\", \"allow-scripts\");"));
            Assert.That(Regex.Matches(Script, "setAttribute\\(\"sandbox\""), Has.Count.EqualTo(1));
            Assert.That(Script, Does.Not.Contain("allow-same-origin"));
            Assert.That(Script, Does.Contain("\"/_apps/frame/v1/" + AppKitProtocol.FrameDocument + "\""), "the default is the route the Explorer maps");
        });
    }

    [Test]
    public void The_fixture_covers_the_ordering_and_every_failure_it_can_provoke()
    {
        var scenarios = Regex.Matches(Script, @"^\s*""(?<name>[a-z-]+)"": \{ outcome:", RegexOptions.Multiline)
            .Select(m => m.Groups["name"].Value)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(scenarios, Is.EquivalentTo(new[]
            {
                "order", "second-hello", "revoked", "digest-mismatch", "bundle-digest-mismatch", "asset-missing", "malformed", "protocol",
            }));
            foreach (var code in new[]
            {
                AppKitProtocol.FailureCodes.DigestMismatch, AppKitProtocol.FailureCodes.BundleDigestMismatch,
                AppKitProtocol.FailureCodes.AssetMissing, AppKitProtocol.FailureCodes.BundleMalformed,
                AppKitProtocol.FailureCodes.ProtocolUnsupported,
            })
            {
                Assert.That(Script, Does.Contain("code: \"" + code + "\""), code);
            }
        });
    }

    [Test]
    public void The_fixture_ordering_scenario_mixes_classic_and_module_scripts()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Script, Does.Contain("scripts: [{ path: \"one.js\", module: false }, { path: \"two.mjs\", module: true }, { path: \"three.js\", module: false }]"));
            Assert.That(Script, Does.Contain("notifications: [\"classic-1:styled:true\", \"module-2:function\", \"classic-3\"]"),
                "styles apply before the first script, lattice exists in a module, and the order is the manifest's");
        });
    }

    [Test]
    public void The_fixture_speaks_the_protocol_names()
    {
        Assert.Multiple(() =>
        {
            foreach (var name in new[]
            {
                AppKitProtocol.Messages.Ready, AppKitProtocol.Messages.Hello, AppKitProtocol.Messages.Bundle,
                AppKitProtocol.Messages.Loaded, AppKitProtocol.Messages.Failed, AppKitProtocol.Events.Revoked,
                AppKitProtocol.Operations.UiNotify, AppKitProtocol.Operations.ContextRead,
            })
            {
                Assert.That(Script, Does.Contain("\"" + name + "\""), name);
            }
        });
    }

    [Test]
    public void The_fixture_decides_no_outcome_by_timer()
    {
        Assert.That(Regex.IsMatch(Script, @"setTimeout|setInterval|requestAnimationFrame"), Is.False);
    }

    [Test]
    public void The_fixture_files_are_plain_ascii()
    {
        Assert.Multiple(() =>
        {
            foreach (var file in new[] { "boot-fixture.html", "boot-fixture.js" })
            {
                var bytes = File.ReadAllBytes(AppKitPaths.Absolute(AppKitPaths.Fixtures + "/" + file));
                Assert.That(bytes.All(b => b is 0x09 or 0x0a or 0x0d or (>= 0x20 and < 0x7f)), Is.True, file);
            }
        });
    }
}
