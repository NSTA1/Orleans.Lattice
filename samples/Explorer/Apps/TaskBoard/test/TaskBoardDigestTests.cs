using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Samples.Explorer.TaskBoard.Tests;

/// <summary>
/// The manifest pins the exact bytes of every bundle asset. There is no build step: these tests
/// compute the digests and, on drift, fail with the values to paste into manifest.json.
/// </summary>
[TestFixture]
public sealed class TaskBoardDigestTests
{
    private static string Sha256(byte[] bytes) => Convert.ToHexStringLower(SHA256.HashData(bytes));

    private static IReadOnlyList<AppUiAsset> ExpectedAssets()
    {
        using var document = JsonDocument.Parse(TaskBoardFiles.ReadBytes("manifest.json"));
        return document.RootElement.GetProperty("ui").GetProperty("assets").EnumerateArray()
            .Select(a =>
            {
                var path = a.GetProperty("path").GetString()!;
                return new AppUiAsset
                {
                    Path = path,
                    MediaType = a.GetProperty("mediaType").GetString()!,
                    Digest = Sha256(TaskBoardFiles.ReadAsset(path)),
                };
            })
            .ToArray();
    }

    [Test]
    public void The_manifest_digests_match_the_bundle_files()
    {
        using var document = JsonDocument.Parse(TaskBoardFiles.ReadBytes("manifest.json"));
        var root = document.RootElement;
        var ui = root.GetProperty("ui");
        var expected = ExpectedAssets();
        var expectedBundle = AppUiBundle.ComputeBundleDigest(expected.ToArray());
        var expectedIcon = expected.Single(a => a.Path == "icon.svg").Digest;

        var drift = new StringBuilder();
        foreach (var (declared, asset) in ui.GetProperty("assets").EnumerateArray().Zip(expected))
        {
            if (declared.GetProperty("digest").GetString() != asset.Digest)
                drift.AppendLine($"  ui.assets[{asset.Path}].digest = \"{asset.Digest}\"");
        }
        if (ui.GetProperty("bundleDigest").GetString() != expectedBundle)
            drift.AppendLine($"  ui.bundleDigest = \"{expectedBundle}\"");
        if (root.GetProperty("presentation").GetProperty("icon").GetProperty("digest").GetString() != expectedIcon)
            drift.AppendLine($"  presentation.icon.digest = \"{expectedIcon}\"");

        Assert.That(drift.ToString(), Is.Empty, "manifest.json has drifted from the bundle files. Set:" + Environment.NewLine + drift);
    }

    [Test]
    public void The_manifest_lists_exactly_the_bundle_files()
    {
        var onDisk = Directory.EnumerateFiles(Path.Combine(TaskBoardFiles.SourceDirectory, "ui"))
            .Select(Path.GetFileName)
            .Where(name => name != ".gitattributes");

        Assert.That(TaskBoardFiles.Manifest().Ui!.Assets.Select(a => a.Path), Is.EquivalentTo(onDisk));
        Assert.That(TaskBoardFiles.Manifest().Ui!.Assets.Select(a => a.Path), Is.EqualTo(TaskBoardFiles.AssetPaths));
    }

    [Test]
    public void The_bundle_digest_is_the_f1_digest_of_the_assets()
    {
        var ui = TaskBoardFiles.Manifest().Ui!;

        Assert.That(AppUiBundle.ComputeBundleDigest(ui.Assets), Is.EqualTo(ui.BundleDigest));
    }

    [Test]
    public void The_icon_digest_is_its_asset_digest()
    {
        var manifest = TaskBoardFiles.Manifest();

        Assert.That(manifest.Presentation!.Icon!.Digest, Is.EqualTo(manifest.Ui!.Assets.Single(a => a.Path == "icon.svg").Digest));
    }

    [Test]
    public void Every_embedded_resource_is_the_file_on_disk()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TaskBoardFiles.ReadEmbedded(TaskBoardApp.ManifestResourceName), Is.EqualTo(TaskBoardFiles.ReadBytes("manifest.json")), "manifest.json");
            foreach (var path in TaskBoardFiles.AssetPaths)
            {
                Assert.That(TaskBoardFiles.ReadEmbedded(TaskBoardApp.AssetResourcePrefix + path), Is.EqualTo(TaskBoardFiles.ReadAsset(path)), path);
            }
        });
    }
}
