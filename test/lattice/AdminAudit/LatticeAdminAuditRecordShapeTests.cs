using System.Reflection;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.AdminAudit;

/// <summary>
/// Pins what an admin audit record can carry (#3638, #6056). The secret-exclusion
/// rule is an allow-list: the record holds identifiers and scalars only, and the
/// request a facade hands the trail has no member a payload (a rule body, a
/// manifest, a credential) or a caller-asserted actor could travel in. Only the
/// administration facades emit records: a replicated or restored record is
/// applied as data and never re-audited.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeAdminAuditRecordShapeTests
{
    private static readonly Regex Emission = new(
        @"\bLatticeAdminAudit\s*\.\s*(RunAsync|AuthorizeAsync|RecordFailureAsync)\b|\bILatticeAdminAuditTrail\b|\bAdminAuditStore\s*\.\s*WriteFinalAsync\b",
        RegexOptions.CultureInvariant);

    private static readonly Dictionary<string, Type> RecordFields = new(StringComparer.Ordinal)
    {
        ["OperationId"] = typeof(string),
        ["Outcome"] = typeof(LatticeAdminAuditOutcome),
        ["Action"] = typeof(LatticeAdminAuditAction),
        ["ActorSubjectId"] = typeof(string),
        ["ActorTenant"] = typeof(string),
        ["ActorKind"] = typeof(LatticeAdminAuditActorKind),
        ["TargetScope"] = typeof(string),
        ["TargetKind"] = typeof(LatticeAdminAuditTargetKind),
        ["TargetId"] = typeof(string),
        ["AnchorTreeId"] = typeof(string),
        ["AnchorKey"] = typeof(string),
        ["AnchorBaseVersion"] = typeof(HybridLogicalClock?),
        ["AnchorBaseHash"] = typeof(long),
        ["AnchorValueHash"] = typeof(long),
        ["CommittedRevisionHlc"] = typeof(HybridLogicalClock?),
        ["ReasonCode"] = typeof(string),
        ["Detail"] = typeof(IReadOnlyDictionary<string, string>),
        ["OriginRegion"] = typeof(string),
        ["TimestampUtc"] = typeof(DateTimeOffset),
        ["CompletedUtc"] = typeof(DateTimeOffset?),
    };

    private static readonly Dictionary<string, Type> RequestFields = new(StringComparer.Ordinal)
    {
        ["Action"] = typeof(LatticeAdminAuditAction),
        ["TargetScope"] = typeof(string),
        ["TargetKind"] = typeof(LatticeAdminAuditTargetKind),
        ["TargetId"] = typeof(string),
        ["Anchor"] = typeof(LatticeAdminAuditAnchor?),
        ["Detail"] = typeof(IReadOnlyDictionary<string, string>),
        ["ActorTenant"] = typeof(string),
    };

    [Test]
    public void Record_carries_only_the_allow_listed_identifier_and_scalar_fields()
    {
        Assert.That(Fields(typeof(LatticeAdminAuditRecord)), Is.EquivalentTo(RecordFields));
    }

    [Test]
    public void Request_carries_no_payload_and_no_actor()
    {
        Assert.That(Fields(typeof(LatticeAdminAuditRequest)), Is.EquivalentTo(RequestFields));
    }

    [Test]
    public void Only_the_administration_facades_and_the_trail_emit_admin_audit_records()
    {
        var src = Path.Combine(HygieneRepository.FindRepoRoot(), "src");
        var emitters = HygieneRepository.EnumerateFiles(src, "*.cs")
            .Where(path => Emission.IsMatch(File.ReadAllText(path)))
            .Select(path => Path.GetRelativePath(src, path).Replace('\\', '/'))
            .ToList();

        Assert.That(emitters, Is.Not.Empty, "the scan found no facade that emits an admin audit record");
        Assert.That(
            emitters.Where(path => !IsAdministrationSurface(path)),
            Is.Empty,
            "a replicated or restored record is applied as data and must never be re-audited");
    }

    private static bool IsAdministrationSurface(string path) =>
        path.StartsWith("lattice/AdminAudit/", StringComparison.Ordinal)
        || path.StartsWith("lattice.auth/", StringComparison.Ordinal)
        || path.StartsWith("lattice.api.", StringComparison.Ordinal);

    private static Dictionary<string, Type> Fields(Type type) =>
        type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.Name != "EqualityContract")
            .ToDictionary(p => p.Name, p => p.PropertyType, StringComparer.Ordinal);
}
