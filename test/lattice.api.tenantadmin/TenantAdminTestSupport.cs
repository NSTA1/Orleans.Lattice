using Microsoft.Extensions.Options;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
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

        /// <summary>How many times the gate was consulted, so a test can prove the
        /// authorizer reached it (or, for the system-origin bypass, did not).</summary>
        public int Calls { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            Calls++;
            return new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
        }
    }

    /// <summary>A gate that allows but narrows the allow with a key filter, to prove the fail-closed partial-allow rejection.</summary>
    internal sealed class FilteredGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Filtered(static _ => true, "narrowed by test"));
    }

    /// <summary>
    /// A gate that allows but narrows with a key filter and carries <b>no reason</b>,
    /// so a facade that reports a partial allow as a fail-closed denial must fall back
    /// to its own default message rather than surface a (null) gate reason.
    /// </summary>
    internal sealed class FilteredNoReasonGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Filtered(static _ => true));
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

    /// <summary>
    /// A gate double that <b>faithfully models the real core
    /// <c>PolicyAccessGate</c> under <c>LatticeAuthOptions.DefaultEffect = Allow</c>
    /// with no policy rules authored</b>. It reproduces the gate's routing exactly:
    /// a request on a control-plane id - the reserved authorization namespace
    /// (<see cref="LatticeAuthReservedTrees.IsReserved"/>) or the tenant-admin
    /// capability namespace (<see cref="LatticeTenantAdminScope.TenantScopePrefix"/>)
    /// - is governed by control-plane isolation and, absent an explicit grant, is
    /// denied regardless of the default effect; every other (data-plane) scope
    /// takes the ordinary path where an unmatched request inherits the
    /// <c>DefaultEffect = Allow</c> and is allowed. An optional grant admits a named
    /// subject on the reserved policy tree, modelling a real platform operator.
    /// This lets a unit test prove that authorizing tenant administration over a
    /// data-plane <c>"*"</c> scope fails open, while the reserved policy tree stays
    /// fail-closed - without spinning up a cluster.
    /// </summary>
    internal sealed class DefaultEffectAllowGate : ILatticeAccessGate
    {
        private readonly string? _policyTreeAdminSubjectId;

        public DefaultEffectAllowGate(string? policyTreeAdminSubjectId = null) =>
            _policyTreeAdminSubjectId = policyTreeAdminSubjectId;

        public int Calls { get; private set; }

        public string? LastScope { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            Calls++;
            LastScope = request.TreeId;

            var isControlPlane =
                LatticeAuthReservedTrees.IsReserved(request.TreeId)
                || request.TreeId.StartsWith(LatticeTenantAdminScope.TenantScopePrefix, StringComparison.Ordinal);

            if (isControlPlane)
            {
                // Control-plane isolation: denied unless an explicit matched grant
                // names this subject on the policy tree. Never inherits DefaultEffect.
                var granted = _policyTreeAdminSubjectId is not null
                    && string.Equals(request.TreeId, LatticeAuthReservedTrees.PolicyTreeId, StringComparison.Ordinal)
                    && string.Equals(request.Subject.SubjectId, _policyTreeAdminSubjectId, StringComparison.Ordinal);
                return new(granted
                    ? LatticeAccessDecision.Allow()
                    : LatticeAccessDecision.Deny("Control-plane isolation: unmatched request denied."));
            }

            // Data-plane scope: an unmatched request inherits DefaultEffect = Allow.
            return new(LatticeAccessDecision.Allow());
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

    /// <summary>
    /// A cascade that throws on its first invocation (simulating a partial failure
    /// mid-delete) and succeeds on every subsequent call, so a test can prove a
    /// re-run after a partial failure completes. Records the registry status it
    /// observed on each call.
    /// </summary>
    internal sealed class FlakyCascade : ITenantTreeCascade
    {
        private readonly FakeTenantRegistry _registry;
        private readonly int _countOnSuccess;

        public FlakyCascade(FakeTenantRegistry registry, int countOnSuccess)
        {
            _registry = registry;
            _countOnSuccess = countOnSuccess;
        }

        public int Calls { get; private set; }

        public TenantStatus? ObservedStatusOnFirstCall { get; private set; }

        public Task<int> DeleteTenantTreesAsync(TenantId tenant, CancellationToken cancellationToken = default)
        {
            Calls++;
            if (Calls == 1)
            {
                ObservedStatusOnFirstCall = _registry.Peek(tenant.Value)?.Status;
                throw new InvalidOperationException("simulated partial cascade failure");
            }

            return Task.FromResult(_countOnSuccess);
        }
    }

    /// <summary>
    /// A substitutable <see cref="ITenantUsageReader"/> serving a fixed
    /// tenant-to-reading map and a fixed enforcement scope. Every figure is a
    /// hand-authored sample rather than a live sampler reading, so a usage test is
    /// exact and never depends on timing, ordering, or the wall clock.
    /// </summary>
    internal sealed class FakeTenantUsageReader : ITenantUsageReader
    {
        private readonly Dictionary<string, TenantUsageReading> _readings = new(StringComparer.Ordinal);
        private readonly TenantEnforcementScope _scope;

        public FakeTenantUsageReader(TenantEnforcementScope scope = TenantEnforcementScope.GlobalConverged) =>
            _scope = scope;

        /// <summary>The tenant ids <see cref="ReadAsync"/> was called for, in order.</summary>
        public List<string?> Reads { get; } = [];

        /// <summary>Seeds a tenant's reading, built from fixed usage, quota, and overage samples.</summary>
        public FakeTenantUsageReader With(
            TenantId tenant,
            LocalUsageSample usage,
            TenantQuotas quotas,
            TenantOverageSample meteredOverage = default,
            TenantEnforcementScope? scope = null)
        {
            _readings[tenant.Value!] = new TenantUsageReading(
                new TenantObservabilitySnapshot(tenant, usage, quotas, meteredOverage),
                scope ?? _scope);
            return this;
        }

        public TenantEnforcementScope ResolveScope(TenantId tenant) => _scope;

        public Task<TenantUsageReading?> ReadAsync(TenantId tenant, CancellationToken cancellationToken = default)
        {
            Reads.Add(tenant.Value);
            return Task.FromResult(_readings.TryGetValue(tenant.Value ?? string.Empty, out var reading)
                ? (TenantUsageReading?)reading
                : null);
        }
    }

    /// <summary>
    /// An <see cref="ITenantRegistry"/> double that models the real
    /// <c>LatticeTenantRegistry.PutMergeAsync</c> CRDT join rather than a
    /// last-writer-wins overwrite: a read hands out an independent
    /// <see cref="TenantRecord.Clone"/>, and a write folds the caller's record
    /// into the stored one with <see cref="TenantRecord.MergeFrom"/> and returns
    /// the stored, merged result. That is what makes a read-check-write guard
    /// testable: the caller's pre-merge view and the committed record are
    /// genuinely different objects.
    /// </summary>
    internal class MergingTenantRegistry : ITenantRegistry
    {
        private readonly Dictionary<string, TenantRecord> _records = new(StringComparer.Ordinal);

        public int Puts { get; private set; }

        public int Deletes { get; private set; }

        public void Seed(TenantRecord record) => _records[record.Id.Value] = record;

        /// <summary>The committed record, without cloning, for assertions.</summary>
        public TenantRecord? Peek(string tenantId) => _records.TryGetValue(tenantId, out var r) ? r : null;

        public Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)
            => Task.FromResult(_records.TryGetValue(tenant.Value, out var record) ? record.Clone() : null);

        public Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default)
            => Task.FromResult(_records.ContainsKey(tenant.Value));

        public async IAsyncEnumerable<TenantRecord> ListAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var record in _records.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return record.Clone();
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(record);
            Puts++;
            OnBeforeMerge(Puts);

            if (!_records.TryGetValue(record.Id.Value, out var stored))
            {
                stored = record.Clone();
                _records[record.Id.Value] = stored;
                return Task.FromResult(stored.Clone());
            }

            stored.MergeFrom(record);

            // The real PutMergeAsync returns the freshly-read stored record it
            // merged into, which is never the same object as the caller's copy.
            // Clone so a caller that mutates the returned record and writes it
            // back cannot alias the store.
            return Task.FromResult(stored.Clone());
        }

        public Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default)
        {
            Deletes++;
            return Task.FromResult(_records.Remove(tenant.Value));
        }

        /// <summary>
        /// Hook invoked immediately before the <paramref name="putNumber"/>-th
        /// write is folded into the stored record - i.e. exactly inside the
        /// caller's read-to-write window. A subclass injects a competing write
        /// here; the base does nothing.
        /// </summary>
        protected virtual void OnBeforeMerge(int putNumber)
        {
        }
    }

    /// <summary>
    /// A <see cref="MergingTenantRegistry"/> that lands one competing residency
    /// removal inside the read-to-write window of the very first write, at an
    /// explicitly supplied stamp. This is the deterministic stand-in for a second
    /// caller draining a <i>different</i> region concurrently: no threads, no
    /// clock, and no ordering assumption.
    /// </summary>
    internal sealed class RacingResidencyRemovalRegistry : MergingTenantRegistry
    {
        private readonly TenantId _tenant;
        private readonly string _regionId;
        private readonly HybridLogicalClock _stamp;

        public RacingResidencyRemovalRegistry(TenantId tenant, string regionId, HybridLogicalClock stamp)
        {
            _tenant = tenant;
            _regionId = regionId;
            _stamp = stamp;
        }

        protected override void OnBeforeMerge(int putNumber)
        {
            if (putNumber != 1)
            {
                return;
            }

            Peek(_tenant.Value)?.SetRegionStatus(
                _regionId, TenantRegionStatus.Draining, _stamp, "racing-writer");
        }
    }

    /// <summary>
    /// A <see cref="MergingTenantRegistry"/> that brings one region online inside
    /// the read-to-write window of the first write, modelling a concurrent
    /// residency change from another replica on a key the caller never touched.
    /// </summary>
    internal sealed class RacingResidencyGrantRegistry : MergingTenantRegistry
    {
        private readonly TenantId _tenant;
        private readonly string _regionId;
        private readonly HybridLogicalClock _stamp;

        public RacingResidencyGrantRegistry(TenantId tenant, string regionId, HybridLogicalClock stamp)
        {
            _tenant = tenant;
            _regionId = regionId;
            _stamp = stamp;
        }

        protected override void OnBeforeMerge(int putNumber)
        {
            if (putNumber != 1)
            {
                return;
            }

            Peek(_tenant.Value)?.SetRegionStatus(
                _regionId, TenantRegionStatus.Online, _stamp, "racing-writer");
        }
    }

    /// <summary>
    /// A <see cref="MergingTenantRegistry"/> that authorizes one extra region
    /// inside the read-to-write window of the first write, modelling a concurrent
    /// operator change to the allowed set.
    /// </summary>
    internal sealed class RacingAllowedRegionRegistry : MergingTenantRegistry
    {
        private readonly TenantId _tenant;
        private readonly string _regionId;
        private readonly HybridLogicalClock _stamp;

        public RacingAllowedRegionRegistry(TenantId tenant, string regionId, HybridLogicalClock stamp)
        {
            _tenant = tenant;
            _regionId = regionId;
            _stamp = stamp;
        }

        protected override void OnBeforeMerge(int putNumber)
        {
            if (putNumber != 1)
            {
                return;
            }

            Peek(_tenant.Value)?.AuthorizeRegion(_regionId, _stamp, "racing-writer");
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

    /// <summary>
    /// A membership context that models a <b>cache miss</b>: the warm synchronous
    /// <see cref="TryResolveCurrent"/> path fails, forcing the facade onto the
    /// asynchronous <see cref="ResolveCurrentAsync"/> directory-read path. It
    /// records whether that read ran under a <see cref="LatticeSystemOrigin"/>
    /// scope, so a test can pin the invariant that the uncached resolution bypasses
    /// the gate (rather than re-entering it) exactly as the production seam requires.
    /// </summary>
    internal sealed class CacheMissMembershipContext : ILatticeMembershipContext
    {
        private readonly LatticeSubject _subject;

        public CacheMissMembershipContext(LatticeSubject subject) => _subject = subject;

        /// <summary>Whether the asynchronous resolve path was taken at all.</summary>
        public bool ResolveCurrentCalled { get; private set; }

        /// <summary>Whether the asynchronous resolve ran inside a system-origin scope.</summary>
        public bool ResolvedUnderSystemOrigin { get; private set; }

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
        {
            ResolveCurrentCalled = true;
            ResolvedUnderSystemOrigin = LatticeSystemOrigin.IsActive;
            return new(_subject);
        }

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = LatticeSubject.Anonymous;
            return false;
        }
    }

    /// <summary>
    /// A configurable <see cref="ILatticeIdentityDirectory"/> that is not the
    /// default <see cref="NullIdentityDirectory"/> (so the facade treats it as a
    /// real, available provider) and resolves every id to a single configured
    /// principal, or to <c>null</c> to model a typo'd, retired, or
    /// not-yet-provisioned id. Records the ids it was asked to resolve.
    /// </summary>
    internal sealed class FakeIdentityDirectory : ILatticeIdentityDirectory
    {
        private readonly DirectoryPrincipal? _principal;

        public FakeIdentityDirectory(DirectoryPrincipal? principal) => _principal = principal;

        /// <summary>The ids <see cref="ResolveAsync"/> was called for, in order.</summary>
        public List<string> Resolved { get; } = [];

        public string ProviderId => "fake";

        public string DescribeEntry(DirectoryPrincipalKind? kind) => "A fake identity directory for tests.";

        public Task<DirectorySearchPage> SearchAsync(
            DirectorySearchQuery query, CancellationToken cancellationToken = default)
            => Task.FromResult(DirectorySearchPage.Empty);

        public Task<DirectoryPrincipal?> ResolveAsync(
            string principalId, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(principalId);
            Resolved.Add(principalId);
            return Task.FromResult(_principal);
        }
    }

    /// <summary>
    /// A minimal <see cref="IOptionsMonitor{TOptions}"/> serving one fixed value,
    /// so a facade that reads <c>CurrentValue</c> / <c>Get</c> can be driven without
    /// the options infrastructure. Change notification is a no-op.
    /// </summary>
    internal sealed class FixedOptionsMonitor<T> : IOptionsMonitor<T>
    {
        public FixedOptionsMonitor(T value) => CurrentValue = value;

        public T CurrentValue { get; }

        public T Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}
