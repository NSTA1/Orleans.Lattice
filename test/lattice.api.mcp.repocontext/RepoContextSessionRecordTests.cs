using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextSessionRecord"/>, the grow-only CRDT that persists
/// one caller session's reuse bookkeeping. The load-bearing property is convergence:
/// two concurrent bundle calls sharing a session id must reach the same record under
/// merge regardless of order, so the merge is commutative, associative, and idempotent.
/// </summary>
[TestFixture]
public sealed class RepoContextSessionRecordTests
{
    private static RepoContextSessionRecord WithReceipts(string sessionId, params string[] receipts)
    {
        var record = new RepoContextSessionRecord { SessionId = sessionId, RepoId = "acme" };
        foreach (var receipt in receipts)
        {
            record.Receipts.Add(Encoding.UTF8.GetBytes(receipt));
        }

        return record;
    }

    private static IReadOnlyList<string> ReceiptStrings(RepoContextSessionRecord record)
        => record.Receipts.Values().Select(Encoding.UTF8.GetString).OrderBy(static s => s, StringComparer.Ordinal).ToArray();

    private static IReadOnlyList<string> PossessionStrings(RepoContextSessionRecord record)
        => record.Possession.Values().Select(Encoding.UTF8.GetString).OrderBy(static s => s, StringComparer.Ordinal).ToArray();

    [Test]
    public void Merge_unions_receipts_and_is_order_independent()
    {
        var left = WithReceipts("s", "a", "b");
        var right = WithReceipts("s", "b", "c");

        var leftFirst = RepoContextSessionRecord.Merge(left, right);
        var rightFirst = RepoContextSessionRecord.Merge(right, left);

        Assert.Multiple(() =>
        {
            Assert.That(ReceiptStrings(leftFirst), Is.EqualTo(new[] { "a", "b", "c" }));
            Assert.That(ReceiptStrings(rightFirst), Is.EqualTo(ReceiptStrings(leftFirst)),
                "Merge is commutative: order cannot change the converged set.");
        });
    }

    [Test]
    public void Merge_unions_possession_tokens()
    {
        var left = new RepoContextSessionRecord { SessionId = "s", RepoId = "acme" };
        left.Possession.Add(Encoding.UTF8.GetBytes("src/A.cs\u0000h1"));
        var right = new RepoContextSessionRecord { SessionId = "s", RepoId = "acme" };
        right.Possession.Add(Encoding.UTF8.GetBytes("src/B.cs\u0000h2"));

        var merged = RepoContextSessionRecord.Merge(left, right);

        Assert.That(PossessionStrings(merged), Is.EqualTo(new[] { "src/A.cs\u0000h1", "src/B.cs\u0000h2" }));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var record = WithReceipts("s", "a", "b");

        var once = RepoContextSessionRecord.Merge(record, record);
        var twice = RepoContextSessionRecord.Merge(once, record);

        Assert.That(ReceiptStrings(twice), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = WithReceipts("s", "a");
        var b = WithReceipts("s", "b");
        var c = WithReceipts("s", "c");

        var leftAssoc = RepoContextSessionRecord.Merge(RepoContextSessionRecord.Merge(a, b), c);
        var rightAssoc = RepoContextSessionRecord.Merge(a, RepoContextSessionRecord.Merge(b, c));

        Assert.That(ReceiptStrings(leftAssoc), Is.EqualTo(ReceiptStrings(rightAssoc)));
    }

    [Test]
    public void Merge_preserves_identity_from_the_populated_side()
    {
        var populated = new RepoContextSessionRecord { SessionId = "s1", RepoId = "acme" };
        var blank = new RepoContextSessionRecord();

        var merged = RepoContextSessionRecord.Merge(blank, populated);

        Assert.Multiple(() =>
        {
            Assert.That(merged.SessionId, Is.EqualTo("s1"), "Identity falls back to the populated replica.");
            Assert.That(merged.RepoId, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void Merge_rejects_null_arguments()
    {
        var record = WithReceipts("s", "a");

        Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextSessionRecord.Merge(null!, record), Throws.ArgumentNullException);
            Assert.That(() => RepoContextSessionRecord.Merge(record, null!), Throws.ArgumentNullException);
        });
    }
}
