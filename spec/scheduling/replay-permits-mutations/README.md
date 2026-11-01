# ReplayPermits mutation catalogue

The 32 anchored mutations cover every checked property, including TypeOK, and
every behavioural Next action, including environment actions. Read the
[refinement](../ReplayPermitsRefinement.md) for the approved cold-lifetime
correction and the explicit temporal premises. All 29 one-target pairs passed
against source SHA-256
`657769D201A472F5EFFF54C85EC17A2F56D0CB81F6F4844F0B91E49426BB10D0`,
as did the complete base and both enrolled larger-bound configurations.
The three reserved-system-lane mutations (issue #5569) were added after that
certification and are checked by CI against the current source.
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
| `SystemLaneProgressLaneWaitsForSharedPool` | Anchored `LaneOpportunity` perturbation; the lane is granted only with free shared capacity, so a hung shared holder starves a system waiter. |
| `LaneExclusiveAdmitsAnyWaiter` | Anchored `LaneOpportunity` perturbation; any queued replay may take the reserved lane. |
| `PermitConservationSystemAdmissionDuplicatesCapacity` | Anchored `SystemAdmission` perturbation; a system replay takes a shared permit without consuming it. |

All actual environment actions have perturbations; only `Stutter` represents
no production behavior. The refinement records the completed pairs, scoped
harness controls and retained historical failures separately. The
shared census classifier's false positive on legitimate gap prose remains an
integration dependency, not a reason to alter this model or its obligations.
