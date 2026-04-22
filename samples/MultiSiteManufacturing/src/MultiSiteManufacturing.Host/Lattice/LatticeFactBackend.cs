using System.Globalization;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// Lattice backend — appends every fact to a single Orleans.Lattice B+ tree
/// (keyed by <c>{serial}/{hlc}/{factId}</c> so a range scan over one prefix
/// returns all facts for a part in HLC order) and computes state via
/// <see cref="ComplianceFold"/> over the scan result.
/// </summary>
/// <remarks>
/// <para>Keys are structured:</para>
/// <code>
///   {serial}/{wallTicks:D20}/{counter:D10}/{factId}
/// </code>
/// <para>
/// Zero-padding the HLC components keeps lexicographic order identical to
/// HLC order, so the tree's native sorted-scan cost is the same as a fold
/// over a pre-sorted log. Delivery order is irrelevant: a late-arriving
/// fact with an earlier HLC slots into the correct position on read.
/// </para>
/// </remarks>
public sealed class LatticeFactBackend(
    IGrainFactory grainFactory,
    ILogger<LatticeFactBackend> logger,
    string treeId = LatticeFactBackend.FactTreeId) : IFactBackend
{
    /// <summary>Default Lattice tree id that holds every fact across every part.</summary>
    public const string FactTreeId = "mfg-facts";

    /// <inheritdoc />
    public string Name => "lattice";

    private ILattice Tree => grainFactory.GetGrain<ILattice>(treeId);

    /// <inheritdoc />
    public async Task EmitAsync(Fact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);

        var key = KeyFor(fact);
        var payload = FactJsonCodec.Encode(fact);

        await Tree.SetAsync(key, payload, cancellationToken);

        logger.LogDebug("Lattice backend wrote {Key} ({Bytes} bytes)", key, payload.Length);
    }

    /// <inheritdoc />
    public async Task<ComplianceState> GetStateAsync(PartSerialNumber serial, CancellationToken cancellationToken = default)
    {
        var facts = await GetFactsAsync(serial, cancellationToken);
        return ComplianceFold.Fold(facts);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<Fact>> GetFactsAsync(PartSerialNumber serial, CancellationToken cancellationToken = default)
    {
        var (start, endExclusive) = PrefixRange(serial);
        var facts = new List<Fact>();

        await foreach (var kvp in Tree.ScanEntriesAsync(start, endExclusive, cancellationToken: cancellationToken))
        {
            facts.Add(FactJsonCodec.Decode(kvp.Value));
        }

        return facts;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PartSerialNumber>> ListPartsAsync(CancellationToken cancellationToken = default)
    {
        // The tree is the source of truth (strongly consistent), and keys
        // are of the form "{serial}/...", so the distinct set of serials
        // is exactly the distinct prefix-before-'/' of every tree key.
        // Walk once, dedupe with a HashSet.
        var serials = new HashSet<PartSerialNumber>();
        await foreach (var key in Tree.ScanKeysAsync(cancellationToken: cancellationToken))
        {
            var slash = key.IndexOf('/');
            if (slash > 0)
            {
                serials.Add(new PartSerialNumber(key[..slash]));
            }
        }
        return [.. serials];
    }

    private static string KeyFor(Fact fact) =>
        string.Create(CultureInfo.InvariantCulture,
            $"{fact.Serial.Value}/{fact.Hlc.WallClockTicks:D20}/{fact.Hlc.Counter:D10}/{fact.FactId:N}");

    private static (string Start, string EndExclusive) PrefixRange(PartSerialNumber serial)
    {
        // '/' = 0x2F, '0' = 0x30 — so {serial}0 is strictly greater than every
        // key of the form {serial}/<anything>, giving a tight half-open range.
        var prefix = serial.Value + "/";
        var end = serial.Value + "0";
        return (prefix, end);
    }
}
