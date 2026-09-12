using System.IO;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Records the issue #2735 audit of test fixtures that invoke a private
/// production member by reflection, and keeps it from decaying into a snapshot.
/// <para>
/// <b>The defect being guarded.</b> A fixture that reaches its subject through
/// <c>BindingFlags.NonPublic</c> and <c>Invoke</c> proves the member behaves
/// correctly <i>when called</i>, and nothing about whether anything calls it.
/// Those are two different claims and only the first is tested. This shipped a
/// real defect: a leaf-split helper was green under a reflected fixture while
/// its single production call site was gated behind a predicate that declined
/// on every tree large enough to need the split, and a deployment sat on an
/// unsplit 21.3 GB tree with no test anywhere contradicting it.
/// </para>
/// <para>
/// <b>Why a registry and not an inline comment convention.</b> The audit's
/// value is that somebody checked the production call sites of each reflected
/// member. That finding is worth keeping, but it expires silently: the next
/// fixture to reach past the seam inherits the clean bill of health of the ones
/// audited before it, because nothing distinguishes it from them. Listing the
/// audited population here inverts that. A new reflection site is not in the
/// registry, so it fails, and the author has to make the same judgement
/// deliberately instead of by default. This is the idiom issue #2735 itself
/// cites as precedent - the same shape as the metrics doc-coverage exemption
/// lists.
/// </para>
/// <para>
/// <b>Being in this registry is not a permanent exemption.</b> Each entry is a
/// recorded judgement that the exemption was acceptable <i>at audit time</i>,
/// under the rubric in <c>.github/instructions/testing.instructions.md</c>
/// ("Reflection past the public seam proves the unit and exempts the wiring").
/// The discriminator there is not "is the member non-public" but "who calls it
/// in production, and can that caller regress?". If you change the production
/// wiring around one of these members, re-check its row rather than trusting
/// it.
/// </para>
/// </summary>
[TestFixture]
public sealed class ReflectionSeamAuditTests
{
    // Split so this file, which necessarily names the tokens it searches for,
    // is not matched by its own scan. Skipping by path alone would be enough,
    // and the path skip below is still the real defence - this just keeps the
    // file from being a confusing grep hit for anyone auditing the auditor.
    private const string GetMethodToken = "GetMethod" + "(";
    private const string NonPublicToken = "NonPublic";
    private const string InvokeToken = ".Invoke" + "(";

    /// <summary>
    /// The audited population: every test file that invokes a private member by
    /// reflection, with the reason its exemption was judged acceptable.
    /// </summary>
    private static readonly Dictionary<string, string> AuditedReflectionSites = new(StringComparer.Ordinal)
    {
        ["test/lattice.api.mcp.repocontext/Bootstrap/RepoContextBootstrapServicePassTests.cs"] =
            "Progress pump is gated on a non-null progress sink; the same fixture drives the public bootstrap pass seam.",
        ["test/lattice.api.state/LatticeStateApiEdgeCaseTests.cs"] =
            "Arrange-only: reflection seeds edge-case state, and the State API integration fixtures drive the public seam.",
        ["test/lattice.backup/RawEntryCollectorTests.cs"] =
            "Carries the prescribed comment naming its two unconditional call sites in LatticeBackupCaptureService.",
        ["test/lattice.explorer.entra/MsalEntraInteractiveTokenAcquirerTests.cs"] =
            "Arrange-only: reflection seeds the MSAL app cache and device-code callback; the public acquire methods are asserted.",
        ["test/lattice.replication/ReplicationDriverActivationServiceTests.RuntimeRetry.cs"] =
            "Retry arm is also reached through ExecuteAsync in the sibling fixture, which drives the hosted-service seam.",
        ["test/lattice.replication/ReplicationDriverActivationServiceTests.cs"] =
            "Framework-owned caller: BackgroundService.StartAsync invokes ExecuteAsync, so no call site of ours can regress.",
        ["test/lattice.storage.azuretable/AzureTableWalStorageProviderReadTrimIntegrationTests.cs"] =
            "DecompressPayload has two unconditional call sites on the read path, which this fixture also exercises publicly.",
        ["test/lattice.storage.azuretable/AzureTableWalStorageProviderReconcileWhiteboxTests.cs"] =
            "Pins the 404-swallow arm of orphan rollback; the reconcile path itself has public integration coverage.",
        ["test/lattice.storage.azuretable/AzureTableWalStorageProviderTests.DecompressBounds.cs"] =
            "Bounds check on DecompressPayload, whose production call sites are unconditional rather than gated.",
        ["test/lattice/BPlusTree/Grains/BPlusLeafGrainTests.BoundedSplitTransfer.cs"] =
            "SplitAsync is gated by SplitIfNeededUnderGateAsync; SplitGate's Split_proceeds_normally_when_gate_is_free drives that wiring.",
        ["test/lattice/BPlusTree/Grains/BPlusLeafGrainTests.MultiPartitionMaterialiser.cs"] =
            "Split-recovery helper; sibling BPlusLeafGrain fixtures drive the public split path.",
        ["test/lattice/BPlusTree/Grains/BPlusLeafGrainTests.SplitByteBound.cs"] =
            "Known instance of this defect and the one that motivated issue #2735. Owned by issue #2733, not by this audit.",
        ["test/lattice/BPlusTree/Grains/LatticeLockGrainTests.RemindersAndFaultArms.cs"] =
            "ResolveTtl override has no production call site by design; the fixture says so inline and guards the opt-out.",
        ["test/lattice/BPlusTree/Grains/WalCommitLogWriterWedgeDiagnosticsTests.cs"] =
            "Arrange-only: wedges private tracker state by reflection, then asserts through the public AppendAsync.",
        ["test/lattice/BPlusTree/Grains/WalMaterialiserPinGrainFaultArmsTests.cs"] =
            "Private flush arm is also driven from the public report path in the same fixture.",
        ["test/lattice/Views/ViewActivationServiceTests.cs"] =
            "Framework-owned caller: BackgroundService.StartAsync invokes ExecuteAsync, so no call site of ours can regress.",
        ["test/shared/Orleans.Lattice.Testing/CrdtBufferOwnershipContractTestsBase.cs"] =
            "Census: a contract sweep whose subject is the population of members, so there is no single wiring to exempt.",
    };

    /// <summary>
    /// Fails when a test file invokes a private member by reflection without
    /// having been audited, so the judgement is made deliberately rather than
    /// inherited.
    /// </summary>
    [Test]
    public void Every_reflection_invocation_site_in_test_code_has_been_audited()
    {
        var (found, examined) = ScanForReflectionSites();

        HygieneDenominator.RequireExamined(
            examined,
            nameof(ReflectionSeamAuditTests),
            "test source files",
            "test/");

        var unaudited = found.Where(f => !AuditedReflectionSites.ContainsKey(f)).OrderBy(f => f, StringComparer.Ordinal).ToList();

        Assert.That(unaudited, Is.Empty,
            "A test fixture invokes a private production member by reflection and has not been audited. That proves the "
            + "member works when called and nothing about whether anything calls it - see 'Reflection past the public seam' "
            + "in .github/instructions/testing.instructions.md. Decide who calls it in production and whether that caller "
            + "can regress; then either add a fixture that drives the public seam, or add a row to "
            + nameof(AuditedReflectionSites) + " recording why the exemption is acceptable."
            + Environment.NewLine
            + string.Join(Environment.NewLine, unaudited));
    }

    /// <summary>
    /// Fails when a registry row no longer corresponds to a reflection site.
    /// <para>
    /// Without this, the registry only ever grows. A row whose fixture was
    /// deleted or rewritten to use the public seam would sit here indefinitely,
    /// asserting that an exemption is still needed for a fixture that no longer
    /// takes one - and, worse, silently pre-authorising the next fixture that
    /// happens to be created at that path. This is the half that will fire when
    /// issue #2733 lands, which is the intended behaviour rather than a nuisance:
    /// removing the reflection from <c>SplitByteBound</c> should require deleting
    /// its row.
    /// </para>
    /// </summary>
    [Test]
    public void Every_audited_entry_still_corresponds_to_a_reflection_site()
    {
        var (found, _) = ScanForReflectionSites();

        var stale = AuditedReflectionSites.Keys
            .Where(k => !found.Contains(k))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.That(stale, Is.Empty,
            "A row in " + nameof(AuditedReflectionSites) + " no longer matches a reflection site. The fixture was either "
            + "deleted or converted to drive the public seam, which is the outcome this audit wants. Delete the row so the "
            + "registry cannot pre-authorise a future fixture at the same path."
            + Environment.NewLine
            + string.Join(Environment.NewLine, stale));
    }

    /// <summary>
    /// Scans the repository's test tree for files that invoke a private member
    /// by reflection.
    /// </summary>
    /// <returns>The matching repo-relative paths, and the number of files examined.</returns>
    private static (HashSet<string> Found, int Examined) ScanForReflectionSites()
    {
        var root = HygieneRepository.FindRepoRoot();
        var testRoot = Path.Combine(root, "test");

        Assert.That(Directory.Exists(testRoot), Is.True,
            $"The test root was not found at '{testRoot}'. This guard's scan root has moved.");

        var found = new HashSet<string>(StringComparer.Ordinal);
        var examined = 0;

        foreach (var file in Directory.EnumerateFiles(testRoot, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(root, file).Replace('\\', '/');

            if (relative.Contains("/bin/", StringComparison.Ordinal)
                || relative.Contains("/obj/", StringComparison.Ordinal))
            {
                continue;
            }

            // This file names the tokens in order to search for them.
            if (relative.EndsWith(nameof(ReflectionSeamAuditTests) + ".cs", StringComparison.Ordinal))
            {
                continue;
            }

            examined++;
            var text = File.ReadAllText(file);

            if (text.Contains(GetMethodToken, StringComparison.Ordinal)
                && text.Contains(NonPublicToken, StringComparison.Ordinal)
                && text.Contains(InvokeToken, StringComparison.Ordinal))
            {
                found.Add(relative);
            }
        }

        return (found, examined);
    }
}
