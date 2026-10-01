using System.Security.Cryptography;
using System.Text;
using NUnit.Framework;
using Orleans.Lattice.Api.Mcp.RepoContext;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Pins the operation id's exact bytes. The id is the idempotency key a chunk's
/// atomic saga is keyed on, so a change to it is not a refactor - it detaches every
/// in-flight retry from its original saga. Each case therefore computes the
/// expectation with the staged <see cref="StringBuilder"/> shape the three
/// reconcilers used before they shared this builder, and asserts the folded
/// implementation reproduces it character for character.
/// </summary>
[TestFixture]
public sealed class RepoContextOperationIdTests
{
    /// <summary>The three reconciler tags that reach the shared builder.</summary>
    private static readonly string[] Prefixes = ["rcb-", "rcc-", "rcs-"];

    [Test]
    public void BuildMatchesStagedShapeForEveryPrefix()
    {
        var upserts = new List<KeyValuePair<string, byte[]>>
        {
            new("repo/lattice/file/src/a.cs", "alpha"u8.ToArray()),
            new("repo/lattice/file/src/b.cs", "beta"u8.ToArray()),
        };
        var deletes = new List<string> { "repo/lattice/file/src/gone.cs" };

        foreach (var prefix in Prefixes)
        {
            Assert.That(
                RepoContextOperationId.Build(prefix, operationScope: null, "lattice", 7, upserts, deletes),
                Is.EqualTo(Staged(prefix, operationScope: null, "lattice", 7, upserts, deletes)),
                $"prefix {prefix}");
        }
    }

    [Test]
    public void BuildMatchesStagedShapeWithOperationScope()
    {
        var upserts = new List<KeyValuePair<string, byte[]>>
        {
            new("repo/lattice/symbol/Foo.Bar", "body"u8.ToArray()),
        };

        Assert.That(
            RepoContextOperationId.Build("rcs-", "symbols:pass-3", "lattice", 0, upserts, []),
            Is.EqualTo(Staged("rcs-", "symbols:pass-3", "lattice", 0, upserts, [])));
    }

    [Test]
    public void BuildMatchesStagedShapeForEmptyChunk()
    {
        Assert.That(
            RepoContextOperationId.Build("rcb-", operationScope: null, "lattice", 0, [], []),
            Is.EqualTo(Staged("rcb-", operationScope: null, "lattice", 0, [], [])));
    }

    [Test]
    public void BuildMatchesStagedShapeForNonAsciiAndOversizedParts()
    {
        // Deliberately crosses the builder's stack transcode budget and carries
        // multi-byte and surrogate-pair characters, so the per-part transcode is
        // exercised on both the rented path and the non-ASCII path.
        var longKey = "repo/lattice/file/" + new string('k', 900) + "/\u00e9\ud83d\ude00.cs";
        var upserts = new List<KeyValuePair<string, byte[]>>
        {
            new(longKey, new byte[4096]),
        };
        var deletes = new List<string> { "repo/lattice/file/\u00fc\u00f1\u00ee.cs" };

        Assert.That(
            RepoContextOperationId.Build("rcc-", "\u00e9scope", "lattice-\u00e9", 1234, upserts, deletes),
            Is.EqualTo(Staged("rcc-", "\u00e9scope", "lattice-\u00e9", 1234, upserts, deletes)));
    }

    [Test]
    public void BuildIsSensitiveToEveryPart()
    {
        var upserts = new List<KeyValuePair<string, byte[]>>
        {
            new("key-a", "one"u8.ToArray()),
        };
        var baseline = RepoContextOperationId.Build("rcb-", null, "lattice", 1, upserts, ["d"]);

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextOperationId.Build("rcb-", "scope", "lattice", 1, upserts, ["d"]), Is.Not.EqualTo(baseline));
            Assert.That(RepoContextOperationId.Build("rcb-", null, "other", 1, upserts, ["d"]), Is.Not.EqualTo(baseline));
            Assert.That(RepoContextOperationId.Build("rcb-", null, "lattice", 2, upserts, ["d"]), Is.Not.EqualTo(baseline));
            Assert.That(
                RepoContextOperationId.Build(
                    "rcb-",
                    null,
                    "lattice",
                    1,
                    [new KeyValuePair<string, byte[]>("key-a", "two"u8.ToArray())],
                    ["d"]),
                Is.Not.EqualTo(baseline));
            Assert.That(RepoContextOperationId.Build("rcb-", null, "lattice", 1, upserts, ["e"]), Is.Not.EqualTo(baseline));
            Assert.That(RepoContextOperationId.Build("rcc-", null, "lattice", 1, upserts, ["d"]), Is.Not.EqualTo(baseline));
        });
    }

    /// <summary>
    /// The staged implementation every reconciler carried before the shared builder
    /// replaced it, reproduced verbatim as the expectation.
    /// </summary>
    private static string Staged(
        string prefix,
        string? operationScope,
        string repoId,
        int chunkIndex,
        IReadOnlyList<KeyValuePair<string, byte[]>> upserts,
        IReadOnlyList<string> deletes)
    {
        var builder = new StringBuilder();
        if (operationScope is not null)
        {
            builder.Append(operationScope).Append('\n');
        }

        builder.Append(repoId).Append('\n').Append(chunkIndex);
        foreach (var upsert in upserts)
        {
            builder.Append("\nU").Append(upsert.Key).Append('=').Append(FileDigest.Compute(upsert.Value));
        }

        foreach (var delete in deletes)
        {
            builder.Append("\nD").Append(delete);
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString()));
        return prefix + Convert.ToHexStringLower(hash.AsSpan(0, 16));
    }
}
