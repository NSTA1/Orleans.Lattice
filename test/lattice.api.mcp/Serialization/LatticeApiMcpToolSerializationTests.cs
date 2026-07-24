using System.Text.Json;
using Orleans.Lattice;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Regression coverage for the MCP tool-result serializer
/// (<see cref="LatticeApiMcpToolSerialization"/>) and its
/// <see cref="Int64JsonStringConverter"/>. Proves that every <c>int64</c> field a
/// state tool emits is serialized as a JSON <b>string</b> so it round-trips
/// byte-exact through a host that re-parses the structured-content block, that
/// 32-bit fields stay JSON numbers, and that the SDK's enum-as-string behaviour
/// survives deriving a fresh options instance from the SDK defaults (issue #1339).
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpToolSerializationTests
{
    // The exact values recorded in issue #1339: both are wider than the 2^53
    // integer range an IEEE-754 double can represent without loss.
    private const long WallClockTicks = 639204850338743759L;
    private const long ValueHash = -1169626215086674468L;
    private const long ExpiresAtTicks = 639204850338799991L;

    private static readonly JsonSerializerOptions Options = LatticeApiMcpToolSerialization.Options;

    [Test]
    public void Int64_fields_that_exceed_the_double_mantissa_cannot_survive_a_double()
    {
        // The premise of the defect: a 64-bit value beyond 2^53 loses its
        // low-order digits when a JSON number token is re-parsed through a double,
        // which is exactly how many MCP hosts read the structured block. This is
        // why the value must not be emitted as a JSON number.
        Assert.Multiple(() =>
        {
            Assert.That((long)(double)WallClockTicks, Is.Not.EqualTo(WallClockTicks));
            Assert.That((long)(double)ValueHash, Is.Not.EqualTo(ValueHash));
        });
    }

    [Test]
    public void Entry_detail_serializes_every_int64_as_a_string_and_round_trips_byte_exact()
    {
        var result = new EntryDetailResult
        {
            Status = StateQueryStatus.Found,
            TreeId = "mcp-test",
            Key = "alpha",
            Entry = new EntryRecord
            {
                Key = "alpha",
                ValueLength = 11,
                Hlc = new HybridLogicalClock { WallClockTicks = WallClockTicks, Counter = 7 },
                ExpiresAtTicks = ExpiresAtTicks,
            },
        };

        var json = JsonSerializer.Serialize(result, Options);

        using (var doc = JsonDocument.Parse(json))
        {
            var entry = doc.RootElement.GetProperty("entry");
            var hlc = entry.GetProperty("hlc");
            var wallClock = hlc.GetProperty("wallClockTicks");
            var expiresAt = entry.GetProperty("expiresAtTicks");

            Assert.Multiple(() =>
            {
                Assert.That(wallClock.ValueKind, Is.EqualTo(JsonValueKind.String),
                    "wallClockTicks must be emitted as a JSON string, not a number");
                Assert.That(wallClock.GetString(), Is.EqualTo(WallClockTicks.ToString()),
                    "the string keeps every digit of the 64-bit timestamp");
                Assert.That(expiresAt.ValueKind, Is.EqualTo(JsonValueKind.String));
                Assert.That(expiresAt.GetString(), Is.EqualTo(ExpiresAtTicks.ToString()));

                // 32-bit fields cannot exceed the mantissa, so they stay numbers.
                Assert.That(hlc.GetProperty("counter").ValueKind, Is.EqualTo(JsonValueKind.Number));
                Assert.That(entry.GetProperty("valueLength").ValueKind, Is.EqualTo(JsonValueKind.Number));

                // Enum handling inherited from the SDK defaults: string name, not ordinal.
                Assert.That(doc.RootElement.GetProperty("status").ValueKind, Is.EqualTo(JsonValueKind.String));
                Assert.That(doc.RootElement.GetProperty("status").GetString(), Is.EqualTo(nameof(StateQueryStatus.Found)));
            });
        }

        var back = JsonSerializer.Deserialize<EntryDetailResult>(json, Options);
        Assert.That(back, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(back!.Entry!.Hlc.WallClockTicks, Is.EqualTo(WallClockTicks),
                "the 64-bit timestamp round-trips with no trailing-digit divergence");
            Assert.That(back.Entry.Hlc.Counter, Is.EqualTo(7));
            Assert.That(back.Entry.ExpiresAtTicks, Is.EqualTo(ExpiresAtTicks));
            Assert.That(back.Entry.ValueLength, Is.EqualTo(11));
        });
    }

    [Test]
    public void Revision_value_hash_serializes_as_a_string_and_round_trips_byte_exact()
    {
        var revision = new EntryRevisionRecord
        {
            Hlc = new HybridLogicalClock { WallClockTicks = WallClockTicks, Counter = 0 },
            SourceKey = "alpha",
            ValueHash = ValueHash,
            ValueLength = 11,
        };

        var json = JsonSerializer.Serialize(revision, Options);

        using (var doc = JsonDocument.Parse(json))
        {
            var hash = doc.RootElement.GetProperty("valueHash");
            Assert.Multiple(() =>
            {
                Assert.That(hash.ValueKind, Is.EqualTo(JsonValueKind.String),
                    "a negative 64-bit value hash must also serialize as a string");
                Assert.That(hash.GetString(), Is.EqualTo(ValueHash.ToString()));
            });
        }

        var back = JsonSerializer.Deserialize<EntryRevisionRecord>(json, Options);
        Assert.That(back!.ValueHash, Is.EqualTo(ValueHash));
    }

    [Test]
    public void Converter_reads_an_int64_back_from_either_a_string_or_a_number_token()
    {
        // Forward compatibility: a payload that still encodes the value as a JSON
        // number (an older writer, or a hand-authored request) must deserialize.
        var fromNumber = JsonSerializer.Deserialize<Holder>(
            $"{{\"value\":{WallClockTicks}}}", Options);
        var fromString = JsonSerializer.Deserialize<Holder>(
            $"{{\"value\":\"{WallClockTicks}\"}}", Options);

        Assert.Multiple(() =>
        {
            Assert.That(fromNumber!.Value, Is.EqualTo(WallClockTicks));
            Assert.That(fromString!.Value, Is.EqualTo(WallClockTicks));
        });
    }

    private sealed record Holder
    {
        public long Value { get; init; }
    }
}
