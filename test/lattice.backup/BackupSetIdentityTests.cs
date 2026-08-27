using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="BackupSetIdentity"/>: the single definition of a
/// backup set's content address and of the rule deciding whether a set records
/// durable membership at all. Both halves are load-bearing - the capture path
/// mints an id only for a set it also stamps, and the restore path re-derives a
/// single-member address to rescue an id persisted from a build that minted one
/// unconditionally - so they are pinned here as pure functions.
/// </summary>
public sealed class BackupSetIdentityTests
{
    // ---- RecordsMembership ----------------------------------------------

    [TestCase(0, false)]
    [TestCase(1, false)]
    [TestCase(2, true)]
    [TestCase(3, true)]
    [TestCase(64, true)]
    public void RecordsMembership_is_true_only_from_two_members_up(int memberCount, bool expected)
    {
        Assert.That(BackupSetIdentity.RecordsMembership(memberCount), Is.EqualTo(expected));
    }

    [Test]
    public void The_recorded_member_threshold_is_two()
    {
        // The threshold is the contract a one-member set hangs off: it is left
        // unstamped because it is indistinguishable from a plain backup, so it is
        // given no id either.
        Assert.That(BackupSetIdentity.MinimumRecordedMembers, Is.EqualTo(2));
    }

    // ---- Compute ---------------------------------------------------------

    [Test]
    public void Compute_rejects_a_null_member_list()
    {
        Assert.That(() => BackupSetIdentity.Compute(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Compute_is_the_lowercase_hex_sha256_of_the_newline_terminated_member_ids()
    {
        // Pinned against an independent computation so a refactor cannot silently
        // change the content address and orphan every previously-issued set id.
        var expected = Convert.ToHexStringLower(
            SHA256.HashData(Encoding.UTF8.GetBytes("m1\nm2\n")));

        Assert.That(BackupSetIdentity.Compute(new[] { "m1", "m2" }), Is.EqualTo(expected));
    }

    [Test]
    public void Compute_is_deterministic_for_the_same_members_in_the_same_order()
    {
        Assert.That(
            BackupSetIdentity.Compute(new[] { "a", "b", "c" }),
            Is.EqualTo(BackupSetIdentity.Compute(new[] { "a", "b", "c" })));
    }

    [Test]
    public void Compute_is_order_sensitive()
    {
        // Members are ordered by scope, and the set restore fences and commits them
        // in that order, so a reordering is a different set.
        Assert.That(
            BackupSetIdentity.Compute(new[] { "a", "b" }),
            Is.Not.EqualTo(BackupSetIdentity.Compute(new[] { "b", "a" })));
    }

    [Test]
    public void Compute_separates_members_so_a_concatenation_is_not_ambiguous()
    {
        // The newline terminator is what stops {"ab"} and {"a","b"} colliding.
        Assert.That(
            BackupSetIdentity.Compute(new[] { "ab" }),
            Is.Not.EqualTo(BackupSetIdentity.Compute(new[] { "a", "b" })));
    }

    [Test]
    public void Compute_emits_a_64_character_lowercase_hex_digest()
    {
        var id = BackupSetIdentity.Compute(new[] { "m1", "m2" });

        Assert.Multiple(() =>
        {
            Assert.That(id, Has.Length.EqualTo(64));
            Assert.That(id, Does.Match("^[0-9a-f]{64}$"));
        });
    }

    [Test]
    public void Compute_of_an_empty_member_list_is_the_empty_sha256()
    {
        Assert.That(
            BackupSetIdentity.Compute(Array.Empty<string>()),
            Is.EqualTo(Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>()))));
    }
}
