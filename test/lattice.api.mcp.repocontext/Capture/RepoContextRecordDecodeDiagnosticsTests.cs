using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Unit tests for the decode diagnostics on <see cref="RepoContextMemoryCodec"/>.
/// <para>
/// A malformed stored value used to surface as a bare positional decode error - the
/// offending byte and <c>Path: $</c>, the document root, which is where every
/// whole-value decode begins and so distinguishes nothing. Neither the key nor the
/// tree appeared anywhere in it, so a caller holding one bad record among thousands
/// could not tell which record had failed. These tests pin that the failure now
/// names the record, and that the byte window it reports to identify the payload
/// shape is bounded and anchored rather than an open read over the stored bytes.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextRecordDecodeDiagnosticsTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly string Key = RepoContextKeys.Memory("acme", "gotchas", "entry-1");

    /// <summary>
    /// Extracts the anchored byte window from a decode failure message, so a test can
    /// assert on the offset and on the exact bytes rendered rather than on prose.
    /// </summary>
    private static (int Offset, string[] Bytes) Window(string message)
    {
        var match = Regex.Match(message, @"bytes at offset (\d+): ([0-9A-F]{2}(?: [0-9A-F]{2})*)\)");
        Assert.That(match.Success, Is.True, $"The message carries an anchored byte window. Message was: {message}");
        return (int.Parse(match.Groups[1].Value), match.Groups[2].Value.Split(' '));
    }

    [Test]
    public void A_malformed_register_names_the_key_the_stage_and_the_stored_length()
    {
        // Not JSON at any offset, so the register decode fails at byte 0.
        var malformed = new byte[] { 0xFE, 0x01, 0x00, 0x00, 0x00, 0x02 };

        var ex = Assert.Throws<RepoContextRecordDecodeException>(
            () => RepoContextMemoryCodec.Fold(malformed, Serializer, Key));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Key, Is.EqualTo(Key));
            Assert.That(ex.Stage, Is.EqualTo("register envelope"));
            Assert.That(ex.StoredLength, Is.EqualTo(6));
            Assert.That(ex.Message, Does.Contain(Key),
                "The key is in the message text, not only on the exception, because the message is what reaches a caller.");
            Assert.That(ex.Message, Does.Contain("stored length: 6 bytes"));
            Assert.That(ex.InnerException, Is.Not.Null, "The underlying positional failure is preserved.");
        });
    }

    [Test]
    public void The_byte_window_is_anchored_at_the_offset_the_decoder_failed_at()
    {
        // Two spaces are legal JSON leading whitespace, so the decoder consumes them
        // and reports the failure at byte 2. An unanchored preview would report byte 0
        // and render the whitespace - the two bytes that are not the problem.
        var malformed = new byte[] { 0x20, 0x20, 0xFE, 0xAB, 0xCD };

        var ex = Assert.Throws<RepoContextRecordDecodeException>(
            () => RepoContextMemoryCodec.Fold(malformed, Serializer, Key));
        var (offset, bytes) = Window(ex!.Message);

        Assert.Multiple(() =>
        {
            Assert.That(offset, Is.EqualTo(2), "The window is anchored at the decoder's reported failure offset.");
            Assert.That(bytes, Is.EqualTo(new[] { "FE", "AB", "CD" }),
                "The window starts at the offending byte, so the first rendered byte is the one the decoder rejected.");
        });
    }

    [Test]
    public void The_byte_window_is_bounded_however_large_the_stored_value_is()
    {
        var malformed = Enumerable.Repeat((byte)0xFE, 4096).ToArray();

        var ex = Assert.Throws<RepoContextRecordDecodeException>(
            () => RepoContextMemoryCodec.Fold(malformed, Serializer, Key));
        var (_, bytes) = Window(ex!.Message);

        Assert.That(bytes, Has.Length.EqualTo(RepoContextRecordDecodeException.WindowByteCount),
            "A four-kilobyte malformed value still renders exactly the bounded window, never a prefix dump.");
    }

    [Test]
    public void The_message_cannot_carry_a_credential_out_of_the_stored_bytes()
    {
        // This is the security clause, and it is the reason the window is bounded at
        // all. The population this diagnostic fires on is records whose bodies carry
        // credential-bearing URLs, and an exception message travels into logs,
        // telemetry, CI transcripts and pull requests. A diagnostic that rendered the
        // stored bytes freely would be an exfiltration path opened by the fix for the
        // credential problem.
        //
        // The secret is assembled from fragments rather than written as a literal: a
        // fixture that reproduces the offending shape becomes an instance of it, which
        // is the self-reference trap RepoContextBodyFraming already documents. The
        // fragments below spell no recognisable token format on any single line.
        var secret = string.Concat("QQQQ", new string('Z', 32), "9999");
        var malformed = Encoding.UTF8.GetBytes(string.Concat("\u00fe", "user:", secret, "@host/path"));

        var ex = Assert.Throws<RepoContextRecordDecodeException>(
            () => RepoContextMemoryCodec.Fold(malformed, Serializer, Key));

        // Asserted before the window is extracted, deliberately. Were this ordered
        // the other way, a change that rendered decoded text would fail the window
        // parse first and redden this test for a reason that has nothing to do with
        // the secret, which is a green-looking arm asserting the wrong clause.
        Assert.That(ex!.Message, Does.Not.Contain(secret),
            "The secret is never echoed as text: the window is rendered as hex and never as decoded content.");

        var (_, bytes) = Window(ex.Message);

        Assert.That(bytes, Has.Length.LessThanOrEqualTo(RepoContextRecordDecodeException.WindowByteCount),
            "At most the bounded window is exposed, which is far too few bytes to reconstruct a credential.");
    }

    [Test]
    public void A_malformed_record_inside_a_well_formed_register_still_names_the_key()
    {
        // The register envelope decodes perfectly; one concurrent value inside it does
        // not. Without the key this failure is indistinguishable from the envelope
        // case, and neither points at a record.
        var register = new MvRegister();
        register.Set("r", Enumerable.Repeat((byte)0xFF, 32).ToArray());
        var stored = JsonLatticeSerializer<MvRegister>.Default.Serialize(register);

        var ex = Assert.Throws<RepoContextRecordDecodeException>(
            () => RepoContextMemoryCodec.Fold(stored, Serializer, Key));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Key, Is.EqualTo(Key));
            Assert.That(ex.Stage, Is.EqualTo("memory record"),
                "The stage separates a malformed envelope from a malformed record inside a sound envelope.");
            Assert.That(ex.StoredLength, Is.EqualTo(32),
                "The report sizes the concurrent value the decoder rejected, not the enclosing envelope that decoded cleanly.");
        });
    }

    [Test]
    public void A_decode_failure_from_a_call_site_with_no_key_says_so_rather_than_going_silent()
    {
        var malformed = new byte[] { 0xFE };

        var ex = Assert.Throws<RepoContextRecordDecodeException>(
            () => RepoContextMemoryCodec.Fold(malformed, Serializer));

        Assert.That(ex!.Key, Does.Contain("not supplied"),
            "A call site that threaded no key is named as such, which is still more actionable than a bare positional error.");
    }

    [Test]
    public void TryFold_reports_a_malformed_value_instead_of_throwing()
    {
        var malformed = new byte[] { 0xFE, 0x01, 0x02 };

        var decoded = RepoContextMemoryCodec.TryFold(malformed, Serializer, Key, out var folded);

        Assert.Multiple(() =>
        {
            Assert.That(decoded, Is.False);
            Assert.That(folded, Is.Null);
        });
    }

    [Test]
    public void TryFold_returns_the_record_for_a_healthy_value()
    {
        var record = new MemoryRecord { RepoId = "acme", Topic = "gotchas", Id = "entry-1" };
        var stored = MemoryRegisterTestEncoding.EncodeSingle(Serializer, "r", record);

        var decoded = RepoContextMemoryCodec.TryFold(stored, Serializer, Key, out var folded);

        Assert.Multiple(() =>
        {
            Assert.That(decoded, Is.True);
            Assert.That(folded, Is.Not.Null);
            Assert.That(folded!.Id, Is.EqualTo("entry-1"));
        });
    }

    [Test]
    public void TryFold_treats_an_absent_value_as_decoded_rather_than_malformed()
    {
        // An absent key and a corrupt one must not collapse into the same answer: the
        // first is ordinary, the second is a fault worth reporting.
        var decoded = RepoContextMemoryCodec.TryFold(null, Serializer, Key, out var folded);

        Assert.Multiple(() =>
        {
            Assert.That(decoded, Is.True);
            Assert.That(folded, Is.Null);
        });
    }
}
