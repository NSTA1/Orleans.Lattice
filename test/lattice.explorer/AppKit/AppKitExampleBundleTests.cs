using System.Security.Cryptography;
using System.Text.Json;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// The example bundle is a real, valid app manifest whose UI section pins the
/// exact bytes of its entry fragment, one stylesheet and one module - the
/// starting point for an app author (P1) and for the browser lane (U1).
/// </summary>
[TestFixture]
public sealed class AppKitExampleBundleTests
{
    private static AppManifest Manifest()
    {
        var result = AppManifestParser.Parse(AppKitPaths.Read(AppKitPaths.Example + "/manifest.json"));
        Assert.That(result.Errors, Is.Empty, string.Join("; ", result.Errors.Select(e => e.Path + ": " + e.Message)));
        return result.Manifest!;
    }

    [Test]
    public void The_example_manifest_is_valid()
    {
        Assert.That(AppManifestParser.Parse(AppKitPaths.Read(AppKitPaths.Example + "/manifest.json")).IsValid, Is.True);
    }

    [Test]
    public void The_example_is_a_fragment_one_stylesheet_and_one_module()
    {
        var ui = Manifest().Ui!;

        Assert.Multiple(() =>
        {
            Assert.That(ui.Entry, Is.EqualTo("index.html"));
            Assert.That(ui.Styles, Is.EqualTo(new[] { "app.css" }));
            Assert.That(ui.Scripts, Has.Length.EqualTo(1));
            Assert.That(ui.Scripts![0].Path, Is.EqualTo("app.mjs"));
            Assert.That(ui.Scripts[0].Module, Is.True);
            Assert.That(ui.Assets.Select(a => a.Path), Is.EquivalentTo(new[] { "index.html", "app.css", "app.mjs" }));
            Assert.That(ui.MinProtocol, Is.EqualTo(AppUiProtocol.Current));
        });
    }

    [Test]
    public void Every_example_asset_digest_pins_the_file_bytes()
    {
        Assert.Multiple(() =>
        {
            foreach (var asset in Manifest().Ui!.Assets)
            {
                var bytes = File.ReadAllBytes(AppKitPaths.Absolute(AppKitPaths.Example + "/" + asset.Path));
                Assert.That(Convert.ToHexStringLower(SHA256.HashData(bytes)), Is.EqualTo(asset.Digest), asset.Path);
                Assert.That(bytes, Does.Not.Contain((byte)'\r'), asset.Path + " is LF-only, as example/.gitattributes pins");
            }
        });
    }

    [Test]
    public void The_example_bundle_digest_is_the_f1_digest_of_its_assets()
    {
        var ui = Manifest().Ui!;

        Assert.That(AppUiBundle.ComputeBundleDigest(ui.Assets), Is.EqualTo(ui.BundleDigest));
    }

    [Test]
    public void The_example_entry_is_a_valid_fragment()
    {
        var bytes = File.ReadAllBytes(AppKitPaths.Absolute(AppKitPaths.Example + "/index.html"));

        Assert.That(AppManifestValidator.ValidateUiEntryFragment(bytes), Is.Empty);
    }

    [Test]
    public void The_example_requests_only_bridge_operations_it_uses()
    {
        var bridge = Manifest().Ui!.Bridge!;
        var module = AppKitPaths.Read(AppKitPaths.Example + "/app.mjs");

        Assert.Multiple(() =>
        {
            Assert.That(bridge.Select(b => b.Operation), Is.All.Matches<string>(AppUiBridgeOperations.IsKnown));
            foreach (var declaration in bridge)
            {
                Assert.That(module, Does.Contain("\"" + declaration.Operation + "\""), declaration.Operation + " is used");
            }

            Assert.That(bridge.Single(b => b.Operation == AppUiBridgeOperations.DataRead).Trees, Is.EqualTo(new[] { "notes" }));
        });
    }

    [Test]
    public void The_example_files_are_plain_ascii()
    {
        Assert.Multiple(() =>
        {
            foreach (var file in new[] { "manifest.json", "index.html", "app.css", "app.mjs", ".gitattributes" })
            {
                var bytes = File.ReadAllBytes(AppKitPaths.Absolute(AppKitPaths.Example + "/" + file));
                Assert.That(bytes.All(b => b is 0x09 or 0x0a or 0x0d or (>= 0x20 and < 0x7f)), Is.True, file);
            }
        });
    }

    [Test]
    public void The_example_line_endings_are_pinned()
    {
        Assert.That(AppKitPaths.Read(AppKitPaths.Example + "/.gitattributes"), Does.Contain("* text eol=lf"));
    }

    [Test]
    public void The_example_manifest_round_trips_as_json()
    {
        using var document = JsonDocument.Parse(AppKitPaths.Read(AppKitPaths.Example + "/manifest.json"));

        Assert.That(document.RootElement.GetProperty("ui").GetProperty("bundleDigest").GetString(), Is.EqualTo(Manifest().Ui!.BundleDigest));
    }
}
