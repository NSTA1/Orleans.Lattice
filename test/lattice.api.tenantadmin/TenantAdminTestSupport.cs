using Orleans.Lattice;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Shared deterministic test doubles for the tenant-admin facade unit tests: an
/// in-memory <see cref="ITenantRegistry"/>, hand-written access gates (avoiding
/// NSubstitute's awkward <c>in</c>-parameter mocking), a strictly increasing
/// clock, and a stub tenant-tree cascade. All are side-effect free and hold no
/// timing or ordering assumptions.
/// </summary>
internal static class TenantAdminTestSupport
{
    /// <summary>A hand-written gate that uniformly allows or denies.</summary>
    internal sealed class FixedGate : ILatticeAccessGate
    {
        private readonly bool _allow;

        public FixedGate(bool allow) => _allow = allow;

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    /// <summary>A gate that allows but narrows the allow with a key filter, to prove the fail-closed partial-allow rejection.</summary>
    internal sealed class FilteredGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Filtered(static _ => true, "narrowed by test"));
    }

    /// <summary>A gate that records the single request it saw, so a test can assert the operation and scope authorized.</summary>
    internal sealed class RecordingGate : ILatticeAccessGate
    {
        public LatticeOperation LastOperation { get; private set; }

        public string? LastScope { get; private set; }

        public int Calls { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            Calls++;
            LastOperation = request.Operation;
            LastScope = request.TreeId;
            return new(LatticeAccessDecision.Allow());
        }
    }

    /// <summary>
    /// A gate that allows only the one configured cluster-administrator subject id
    /// and denies every other subject, modelling how the real access gate
    /// distinguishes a genuine tenant administrator from a non-admin or a
    /// tenant-scoped ("wrong tenant") subject that lacks cluster-wide admin
    /// authority. Uses an ordinal subject-id comparison.
    /// </summary>
    internal sealed class AdminSubjectGate : ILatticeAccessGate
    {
        private readonly string _adminSubjectId;

        public AdminSubjectGate(string adminSubjectId) => _adminSubjectId = adminSubjectId;

        public string? LastSubjectId { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            LastSubjectId = request.Subject.SubjectId;
            return new(string.Equals(request.Subject.SubjectId, _adminSubjectId, StringComparison.Ordinal)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("subject is not the cluster tenant administrator"));
        }
    }

    /// <summary>
    /// A membership context that resolves a single configured subject on the warm
    /// synchronous path, so a test can drive the authorizer with a specific caller
    /// identity without a cluster or directory read.
    /// </summary>
    internal sealed class FixedMembershipContext : ILatticeMembershipContext
    {
        private readonly LatticeSubject _subject;

        public FixedMembershipContext(LatticeSubject subject) => _subject = subject;

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => new(_subject);

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = _subject;
            return true;
        }
    }

    /// <summary>A strictly increasing <see cref="HybridLogicalClock"/> source.</summary>
    internal sealed class IncrementingClock : ITenantAdminClock
    {
        private HybridLogicalClock _previous = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        public int Ticks { get; private set; }

        public HybridLogicalClock Next()
        {
            _previous = HybridLogicalClock.Tick(_previous);
            Ticks++;
            return _previous;
        }
    }

    /// <summary>A stub cascade returning a configured tree count and recording the tenant it was asked to cascade.</summary>
    internal sealed class StubCascade : ITenantTreeCascade
    {
        private readonly int _count;

        public StubCascade(int count) => _count = count;

        public TenantId? LastTenant { get; private set; }

        public int Calls { get; private set; }

        public Task<int> DeleteTenantTreesAsync(TenantId tenant, CancellationToken cancellationToken = default)
        {
            Calls++;
            LastTenant = tenant;
            return Task.FromResult(_count);
        }
    }

    /// <summary>
    /// A cascade that snapshots the registry record's status at the moment it is
    /// invoked, so a test can prove the tenant was already suspended (admissions
    /// blocked) before any tree was enumerated or deleted.
    /// </summary>
    internal sealed class StatusObservingCascade : ITenantTreeCascade
    {
        private readonly FakeTenantRegistry _registry;
        private readonly int _count;

        public StatusObservingCascade(FakeTenantRegistry registry, int count)
        {
            _registry = registry;
            _count = count;
        }

        public TenantStatus? ObservedStatusAtCascade { get; private set; }

        public int Calls { get; private set; }

        public Task<int> DeleteTenantTreesAsync(TenantId tenant, CancellationToken cancellationToken = default)
        {
            Calls++;
            ObservedStatusAtCascade = _registry.Peek(tenant.Value)?.Status;
            return Task.FromResult(_count);
        }
    }

    /// <summary>A minimal in-memory <see cref="ITenantRegistry"/> backed by a dictionary, with call counters.</summary>
    internal sealed class FakeTenantRegistry : ITenantRegistry
    {
        private readonly Dictionary<string, TenantRecord> _records = new(StringComparer.Ordinal);

        public int Puts { get; private set; }

        public int Deletes { get; private set; }

        public void Seed(TenantRecord record) => _records[record.Id.Value] = record;

        public bool Contains(string tenantId) => _records.ContainsKey(tenantId);

        public TenantRecord? Peek(string tenantId) => _records.TryGetValue(tenantId, out var r) ? r : null;

        public Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)
            => Task.FromResult(_records.TryGetValue(tenant.Value, out var record) ? record : null);

        public Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default)
            => Task.FromResult(_records.ContainsKey(tenant.Value));

        public async IAsyncEnumerable<TenantRecord> ListAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var record in _records.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return record;
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(record);
            Puts++;
            _records[record.Id.Value] = record;
            return Task.FromResult(record);
        }

        public Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default)
        {
            Deletes++;
            return Task.FromResult(_records.Remove(tenant.Value));
        }
    }
}
