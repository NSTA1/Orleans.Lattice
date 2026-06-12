using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage of the pure anti-entropy digest comparison helper.
/// </summary>
[TestFixture]
public class DigestProbeComparerTests
{
    private static LeafProjectionDigest Digest(byte[] hash, int version = 0)
        => new() { Hash = hash, EntryCount = hash.Length, Version = version };

    [Test]
    public void Compare_returns_remote_unavailable_when_digest_not_available()
    {
        var outcome = DigestProbeComparer.Compare(
            Digest(new byte[] { 1 }),
            new DigestProbeResponse { DigestAvailable = false });

        Assert.That(outcome, Is.EqualTo(DigestProbeOutcome.RemoteUnavailable));
    }

    [Test]
    public void Compare_returns_version_skew_when_versions_differ()
    {
        var outcome = DigestProbeComparer.Compare(
            Digest(new byte[] { 1 }, version: 0),
            new DigestProbeResponse { DigestAvailable = true, Digest = Digest(new byte[] { 1 }, version: 1) });

        Assert.That(outcome, Is.EqualTo(DigestProbeOutcome.VersionSkew));
    }

    [Test]
    public void Compare_returns_match_when_versions_and_hashes_equal()
    {
        var outcome = DigestProbeComparer.Compare(
            Digest(new byte[] { 1, 2, 3 }),
            new DigestProbeResponse { DigestAvailable = true, Digest = Digest(new byte[] { 1, 2, 3 }) });

        Assert.That(outcome, Is.EqualTo(DigestProbeOutcome.Match));
    }

    [Test]
    public void Compare_returns_mismatch_when_versions_match_but_hashes_differ()
    {
        var outcome = DigestProbeComparer.Compare(
            Digest(new byte[] { 1, 2, 3 }),
            new DigestProbeResponse { DigestAvailable = true, Digest = Digest(new byte[] { 9, 9, 9 }) });

        Assert.That(outcome, Is.EqualTo(DigestProbeOutcome.Mismatch));
    }

    [Test]
    public void Compare_treats_null_hashes_as_empty_and_equal()
    {
        var local = new LeafProjectionDigest { Hash = null!, Version = 0 };
        var remote = new DigestProbeResponse
        {
            DigestAvailable = true,
            Digest = new LeafProjectionDigest { Hash = null!, Version = 0 },
        };

        Assert.That(DigestProbeComparer.Compare(local, remote), Is.EqualTo(DigestProbeOutcome.Match));
    }
}
