# ReplayPermits mutation catalogue

The 36 anchored mutations cover every checked property, including TypeOK, and
every behavioural Next action, including environment actions. Read the
[refinement](../ReplayPermitsRefinement.md) for the approved cold-lifetime
correction and the explicit temporal premises. All 29 original one-target pairs
passed against source SHA-256
`657769D201A472F5EFFF54C85EC17A2F56D0CB81F6F4844F0B91E49426BB10D0`,
as did the complete base and both enrolled larger-bound configurations. After
work-conserving refusal (#5992 A1), all 30 pairs, the base and both variants
passed again against the LF-normalised source SHA-256
`3DCD5C946280F348954F8B89502703D6B74ADBC3032042074199BC9E3DE7A710`.
After retained arrivals (#5992), all 36 pairs, the base and both variants
passed against the LF-normalised source SHA-256
`5FACE20F886D2B46C41E1C18FA7961191493392BEC9C2EFCDE61F2CCDF1A4385`.
Each file is an anchored edit against the current base, never a copied model.
The filename and `MODULE` begin with the target property. The harness generates
one-target configurations and carries TypeOK beside every non-TypeOK target.
`BOUNDS` leaves the full-bound clean control unchanged. When the generated
mutant cfg differs, the clean source must additionally hold under that exact
cfg and the same TLC options before the targeted mutant runs. These controls
check TypeOK plus the named target, not the complete property set.

## Classification

There are no added-transition splices. `PERTURBS` is present only
for edits to actual `Next` action definitions. Shared-helper and read-definition
edits do not claim direct action coverage.

| Mutation | Classification |
|----------|----------------|
| `BankingMonotoneReturnDiscardsPrefix` | Return-helper perturbation; affects both completed release and budget abandonment, not abandonment alone. |
| `DriveResidencyBoundedBudgetNeverAbandons` | Anchored `BudgetOpportunity` perturbation; only the drive budget is suppressed. |
| `PriorityReservationBulkFloorRemoved` | Bound read-definition perturbation. |
| `ClassNonStarvationCapacityOpportunityNeverGrants` | Anchored `CapacityOpportunity` action perturbation. |
| `TypeOKCloseArrivalsOutsideDomain` | Anchored environment `CloseArrivals` action perturbation. |
| `CoverageSoundColdCoverageUsesCheckpoint` | Anchored `ReplaySlice` action perturbation, larger gap in mutant. |
| `QueueOwnershipCancellationKeepsQueueEntry` | CancelQueued-helper perturbation after actual lifetime cancellation. |
| `DeadlineBoundedRetainsExpiredWait` | Anchored `ClientDeadline` perturbation leaves an expired client active. |
| `UnresolvedWorkDurableDeferWithoutLedger` | Anchored `DeferTerminal` action perturbation. |
| `PermitConservationDriveDuplicatesCapacity` | Anchored `DriveAdmission` action perturbation. |
| `QueueBoundedEnqueueIgnoresRefusal` | Anchored `Enqueue` action perturbation. |
| `FloorOneClampRemoved` | Return-helper perturbation; independently removes the withholding floor. |
| `TypeOKHangOutsideDomain` | Anchored environment `Hang` action perturbation. |
| `MemoryWithholdingIgnoredUnderPressure` | Return-helper perturbation; ignoring withholding preserves FloorOne. |
| `TypeOKObserveUnknownDrain` | Anchored environment `ObserveDrain` action perturbation. |
| `TypeOKPressureOffOutsideDomain` | Anchored environment `MemoryPressureOff` action perturbation. |
| `TypeOKPressureOnOutsideDomain` | Anchored environment `MemoryPressureOn` action perturbation. |
| `QueueBoundedRefuseDepthQueues` | Anchored `RefuseDepth` action perturbation. |
| `SmoothedWaitRefusalAdmits` | Anchored `RefuseWait` action perturbation. |
| `PermitConservationReleaseKeepsOwnership` | Anchored `Release` action perturbation. |
| `NoSelfSustainingBacklogReplacementQueuesAgain` | Anchored `ClientReplacement` perturbation duplicates the existing owner in the queue. |
| `ReplayConvergesDiscardsBanking` | Anchored `ReplaySlice` action perturbation. |
| `ClassPriorityRequestIgnoresClass` | Anchored `Request` action perturbation. |
| `ClassPriorityDriveCountsColdOwners` | Anchored `DriveAdmission` action perturbation; ordinary owners must not consume GC reservations. |
| `ColdCancellationReleasesIgnoresCancelledHolder` | Anchored `BudgetOpportunity` perturbation retains a genuinely cancelled owner. |
| `ColdDeadlineIsolationCancelsOwner` | Anchored `ClientDeadline` perturbation cancels an independent cold lifetime. |
| `ColdDeadlineIsolationAbandonsOwner` | Anchored `ClientDeadline` perturbation directly retires/releases the shared owner without changing its cancellation membership. |
| `TypeOKLifetimeCancellationOutsideDomain` | Anchored environment `CancelColdLifetime` perturbation introduces an unknown owner. |
| `RestoreOwnershipStealsServiceCapacity` | Anchored `Restore` action perturbation with a service-owned baseline in mutant. |
| `WorkConservingRefusalWaitIgnoresFreePermit` | Anchored `RefuseWait` action perturbation; the wait arm drops its decision-time re-check. |
| `RetainedBoundedRetainIgnoresCirculating` | Anchored `RetainOnRefusal` action perturbation with a one-permit ceiling in mutant; retention ignores the retained count. |
| `RetentionGuardedAdmitsAtTheBound` | `Retainable` read-definition perturbation; retention admits at, not below, the circulating permits. |
| `RetentionExpiresExpiryKeepsEntry` | `ExpireRetained`-helper perturbation reached from `BudgetOpportunity`; the maximum age leaves the entry parked. |
| `AdmissionNoOvertakingAnyWaiter` | `Admit`-helper perturbation reached from `CapacityOpportunity`; the gate grants any parked waiter. |
| `QueueProgressDriveIgnoresParkedWaiter` | Anchored `DriveAdmission` action perturbation; the drive probe takes a released permit while a waiter is parked (G1). |
| `QueueBoundedBulkIgnoresTotalDepth` | `RefuseForDepth` read-definition perturbation; the current Interactive bound is dropped. |

All actual environment actions have perturbations; only `Stutter` represents
no production behavior. The refinement records the completed pairs, scoped
harness controls and retained historical failures separately. The
shared census classifier's false positive on legitimate gap prose remains an
integration dependency, not a reason to alter this model or its obligations.
