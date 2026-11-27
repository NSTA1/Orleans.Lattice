# Refinement note: ReplicationOriginBinding

This maps the admitted-peer seam of the replication gRPC host (issue #6053)
onto its production seams: the shared-secret interceptor, the seam's method
registration and admission classes, and the checks each class calls. The
applier the admitted calls feed is `ReplicationAdmission` (see
[RefinementAdmission.md](RefinementAdmission.md)).

It is a design check with detectors, not a machine-checked refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `binding` | Whether the presented credential is bound to the stamped origin | `LatticeReplicationSecurityOptions.BindCredentialToOriginCluster` |
| `obs` | What the last call returned and the identity it acted for | No production state: the call's status and acting identity, which the property judges. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `Call` | A peer calls a method bound on the replication gRPC host: the interceptor refuses an unregistered method, checks the credential and binds it to the stamped origin, then the method's admission class binds its body-declared identity to the caller | `LatticeReplicationGrpcAuthInterceptor`, `ReplicationAdmittedPeer.TryClassify`, `ReplicationAdmittedPeer.EnsureDeclaredOriginIsCaller`, `ReplicationAdmittedPeer.EnsureEntriesFromCaller`, `ReplicationAdmittedPeer.CredentialBoundOrigin`, `LatticeSagaGrpcService` | Yes: `ReplicationAdmittedPeerTests.The_interceptor_refuses_a_replication_method_the_seam_does_not_register`, `LatticeReplicationGrpcAuthInterceptorOriginBindingTests.Binding_enabled_refuses_an_accepted_credential_claiming_another_cluster`, `LatticeReplicationGrpcServiceEnrollmentTests.Push_refuses_a_call_with_no_stamped_origin_header` and `LatticeSagaGrpcServiceTests.Saga_control_without_credential_binding_denies_claimed_origin`. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `OriginAuthenticated` | Every method bound on the host passes the admitted-peer seam; an origin-bound verb requires a present stamped origin equal to the body, and a push refuses a batch any entry of which names another origin; a control verb acts only for the credential-bound origin of an authorized peer, with the body coordinator bound to it and the requester overwritten by it; the binding refusal is one for every failure shape | Yes: `ReplicationAdmittedPeerTests.Every_method_bound_on_the_replication_host_passes_through_the_admitted_peer_seam`, `LatticeReplicationGrpcServiceEnrollmentTests.Push_refuses_an_origin_that_disagrees_with_the_stamped_header`, `LatticeReplicationGrpcServiceEnrollmentTests.Push_refuses_a_batch_carrying_an_entry_of_another_origin`, `LatticeReplicationGrpcAuthInterceptorOriginBindingTests.Binding_enabled_reports_one_indistinguishable_refusal_for_every_failure_shape`, `LatticeSagaGrpcServiceTests.Commit_rejects_unauthorized_origin_before_handler_runs`, `LatticeSagaGrpcServiceTests.Authorization_refuses_when_body_coordinator_disagrees_with_stamped_origin` and `LatticeSagaGrpcServiceTests.GetDecision_stamps_the_authenticated_origin_as_the_requester`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Deliberate abstraction gaps

- **The data plane does not check the topology.** The seam's control class requires the credential-bound origin to be an authorized peer of the current topology, but the origin-bound data-plane verbs require only that the stamped origin equals the body and, with binding on, is bound to the credential. A cluster whose secret is still accepted but which left the topology can push, exchange a manifest and read its own cursor, so the model's origin-bound class carries no topology condition.
  Production symbols: `LatticeReplicationGrpcService.Push`, `LatticeReplicationGrpcService.ExchangeContentManifest`, `LatticeReplicationGrpcService.GetPeerHighWaterMark`, `ReplicationAdmittedPeer.EnsureDeclaredOriginIsCaller`, `IReplicationTopology.CurrentPeers`.
- **A cluster-wide secret binds every origin.** The model gives each peer its own secret. With one cluster-wide secret the outbound secret resolves the same value for any origin, so the binding proves possession of the mesh secret rather than a cluster identity, and an unknown origin is bound. The receiver warns about that mode once at startup and does not refuse it.
  Production symbols: `ConfigurationBindingSecretSource.GetOutboundSecretAsync`, `EnvironmentVariableSecretSource.GetOutboundSecretAsync`, `ReplicationOriginBindingStartupCheck`.
- **Authentication disabled.** The model always requires a credential. A host that turns authentication off admits every registered method without one; only the seam's registration check still applies.
  Production symbols: `LatticeReplicationSecurityOptions.RequireAuthentication`.
- **Peer-read verbs.** The digest, Merkle-walk, dictionary and snapshot verbs consume no body-declared origin; their per-tree enrollment and tenant-read gates decide what they answer and are not modelled.
  Production symbols: `LatticeRemoteSnapshotService.GetMetadataAsync`, `IReplicationTenantIsolationGate`.
