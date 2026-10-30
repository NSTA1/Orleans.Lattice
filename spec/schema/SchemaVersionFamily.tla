---- MODULE SchemaVersionFamily ----
EXTENDS Naturals

\* One versioned tree's schema family: the upcaster catalog built from startup
\* declarations and the durable, append-only runtime registry; the version
\* config's target and how the facade lets it move; the read-time decode of
\* every stored value; and the eager migration that re-stamps every value at
\* the migration's own target, or aborts naming a value it cannot migrate.

CONSTANTS KeyCount, MaxVersion, HostDeclared, PolicyInstalled

Keys == 1..KeyCount
Versions == 1..MaxVersion
Names == {"a", "b"}
\* The tree's configured schema family, and another family's stamp.
Fam == 1
Foreign == 2
\* No version config: target 0 is the unversioned sentinel.
NoCfg == 0

\* A stored value. Abs is an absent (deleted or never-written) key; Raw an
\* un-stamped legacy or unversioned value; Env a value stamped with family fam
\* and version ver whose body has shape `shape` (0 for a legacy body) and
\* satisfies the tree's policy when `ok`.
Val(t, f, v, s, o) == [tag |-> t, fam |-> f, ver |-> v, shape |-> s, ok |-> o]
Abs == Val("Abs", 0, 0, 0, TRUE)
Raw(o) == Val("Raw", 0, 0, 0, o)
Env(f, v, s, o) == Val("Env", f, v, s, o)
Pending == Val("Pending", 0, 0, 0, TRUE)
Stuck == Val("Stuck", 0, 0, 0, TRUE)
Values == {Abs} \cup {Raw(o) : o \in BOOLEAN}
          \cup {Env(f, v, s, o) : f \in {Fam, Foreign}, v \in Versions,
                                  s \in {0} \cup Versions, o \in BOOLEAN}

\* What can land under a key by any path: a local write stamped at some silo's
\* target, a legacy unversioned write, a stamp replicated, restored or forged
\* past the check (newer than the target, or with no path to it), a body the
\* policy rejects, another family's stamp, or a delete.
Landing == {Abs, Raw(TRUE)} \cup {Env(Fam, v, v, TRUE) : v \in Versions}
           \cup {Env(Fam, 1, 1, FALSE), Env(Foreign, 1, 1, TRUE)}

\* Startup declarations: descriptors <<version, name>> and hops <<from, to>>.
HostD == IF HostDeclared THEN {<<1, "a">>, <<MaxVersion, "a">>} ELSE {}
HostH == IF HostDeclared THEN {<<1, MaxVersion>>} ELSE {}

\* Runtime registrations: single descriptors (a replay of a declaration, a
\* rename of one, a new version under either name), single hops (a replay, an
\* extension, one that collides with a declared hop, a descending one), and
\* two registrations carrying a descriptor and a hop together.
Additions ==
    {[d |-> {x}, h |-> {}] :
        x \in {<<1, "a">>, <<1, "b">>, <<2, "a">>, <<2, "b">>, <<MaxVersion, "a">>}}
    \cup {[d |-> {}, h |-> {y}] :
        y \in {<<1, MaxVersion>>, <<2, MaxVersion>>, <<1, 2>>, <<MaxVersion, 2>>}}
    \cup {[d |-> {<<2, "a">>}, h |-> {<<2, MaxVersion>>}],
          [d |-> {<<2, "b">>}, h |-> {<<1, 2>>}]}

VARIABLES rtD, rtH, cfg, data, mig, migT, shadow, reserved, out
vars == <<rtD, rtH, cfg, data, mig, migT, shadow, reserved, out>>

\* What the last step returned to its caller: a registration or config verdict,
\* or a migration's completed or aborted report naming the value.
Out(o) == [o |-> o, key |-> 0]
Aborted(k) == [o |-> "Aborted", key |-> k]
Outcomes == {Out(o) : o \in {"None", "RegAccepted", "RegRefused", "CfgAccepted",
                              "CfgRefused", "Completed"}}
            \cup {Aborted(k) : k \in Keys}

EffD == HostD \cup rtD
EffH == HostH \cup rtH
DescReg(v) == \E n \in Names : <<v, n>> \in EffD
\* A non-empty catalog governs the facade's target checks.
Governed == EffD # {} \/ EffH # {}

\* The registry's walk: follow the unique hop from each version, refusing one
\* that overshoots the target.
RECURSIVE Walk(_, _, _)
Walk(c, t, n) ==
    IF c = t THEN TRUE
    ELSE IF c > t \/ n = 0 THEN FALSE
    ELSE IF \E h \in EffH : h[1] = c /\ h[2] <= t
         THEN Walk((CHOOSE h \in EffH : h[1] = c /\ h[2] <= t)[2], t, n - 1)
         ELSE FALSE
Reach(v, t) == Walk(v, t, MaxVersion)

\* An independent statement of reachability: some chain of hops ends at t.
RECURSIVE ChainN(_, _, _)
ChainN(v, t, n) == v = t \/ (n > 0 /\ \E h \in EffH : h[1] = v /\ ChainN(h[2], t, n - 1))
Chain(v, t) == ChainN(v, t, MaxVersion)

\* A registration merges whole or not at all: no descriptor renamed, every hop
\* ascending between registered versions, at most one hop from any version.
Admissible(a) ==
    LET D == EffD \cup a.d IN
    /\ \A x \in a.d, y \in D : x[1] = y[1] => x[2] = y[2]
    /\ \A h \in a.h :
        /\ h[2] > h[1]
        /\ \E n \in Names : <<h[1], n>> \in D
        /\ \E n \in Names : <<h[2], n>> \in D
        /\ \A g \in EffH \cup a.h : g[1] = h[1] => g = h
Contained(a) == a.d \subseteq EffD /\ a.h \subseteq EffH

\* The facade's check on a new target.
TargetValid(t) ==
    ~Governed \/ (DescReg(t) /\ (cfg = NoCfg \/ (t >= cfg /\ Reach(cfg, t))))

\* The client-facing read of a stored value under target c.
Res(r, s, o) == [r |-> r, shape |-> s, ok |-> o]
Decode(v, c) ==
    IF v.tag = "Abs" THEN Res("Absent", 0, TRUE)
    ELSE IF v.tag = "Raw" \/ c = NoCfg \/ v.fam # Fam \/ v.ver = c
         THEN Res("Body", v.shape, v.ok)
    ELSE IF v.ver > c THEN Res("Newer", 0, TRUE)
    ELSE IF Reach(v.ver, c) THEN Res("Body", c, v.ok)
    ELSE Res("NoPath", 0, TRUE)

\* The per-value migration step: re-stamp at t from the value's own stamp.
MigrateValue(v, t) ==
    IF v.tag = "Abs" THEN Abs
    ELSE IF v.tag = "Raw" THEN Env(Fam, t, 0, v.ok)
    ELSE IF v.ver = t THEN v
    ELSE IF v.fam = Fam /\ Reach(v.ver, t) THEN Env(v.fam, t, t, v.ok)
    ELSE Stuck
\* The tree's policy judges the migrated body, which is at the target's shape.
Rejected(m, t) == PolicyInstalled /\ m.tag = "Env" /\ ~(m.ok /\ m.shape \in {0, t})

\* An independent statement of which values a migration to t must abort on.
NonMigratable(v, t) ==
    \/ v.tag = "Env" /\ v.ver # t /\ ~(v.fam = Fam /\ Chain(v.ver, t))
    \/ PolicyInstalled /\ v.tag # "Abs" /\ ~v.ok

Blank == [k \in Keys |-> Pending]

TypeOK ==
    /\ rtD \subseteq Versions \X Names
    /\ rtH \subseteq Versions \X Versions
    /\ cfg \in {NoCfg} \cup Versions
    /\ data \in [Keys -> Values]
    /\ mig \in {"Idle", "Running"}
    /\ migT \in {0} \cup Versions
    /\ shadow \in [Keys -> Values \cup {Pending}]
    /\ reserved \in BOOLEAN
    /\ out \in Outcomes

\* The effective catalog names each version once, has at most one hop from any
\* version, and every hop ascends between registered versions.
CatalogConsistent ==
    /\ \A x, y \in EffD : x[1] = y[1] => x = y
    /\ \A g, h \in EffH : g[1] = h[1] => g = h
    /\ \A h \in EffH : h[2] > h[1] /\ DescReg(h[1]) /\ DescReg(h[2])

\* The alias reservation is held exactly while a migration runs.
ReservationHeldWhileRunning == reserved <=> mig = "Running"

\* Every value a running migration has built is stamped at its own target.
ShadowAtOwnTarget ==
    \A k \in Keys : shadow[k].tag = "Env" => shadow[k].ver = migT

\* A read of a stamped value of the tree's family returns the body at the
\* target when some chain of hops reaches it, and otherwise an explicit error.
UpcastOrExplicitFailure ==
    cfg # NoCfg =>
      \A k \in Keys :
        LET v == data[k]
            r == Decode(v, cfg)
        IN (v.tag = "Env" /\ v.fam = Fam) =>
             IF Chain(v.ver, cfg)
             THEN /\ r.r = "Body" /\ r.ok = v.ok
                  /\ (r.shape = cfg \/ (v.ver = cfg /\ r.shape = v.shape))
             ELSE /\ r.r \in {"Newer", "NoPath"}
                  /\ (r.r = "Newer" <=> v.ver > cfg)

\* An un-stamped value, or a stamp with no matching config, reads back as the
\* body it was stored with.
UnversionedReadsStoredBody ==
    \A k \in Keys :
      LET v == data[k] IN
        (v.tag = "Raw" \/ (v.tag = "Env" /\ (cfg = NoCfg \/ v.fam # Fam)))
          => Decode(v, cfg) = Res("Body", v.shape, v.ok)

\* No registration removes or redefines an entry.
CatalogAppendOnly == [][rtD \subseteq rtD' /\ rtH \subseteq rtH']_vars

Register(a) ==
    LET ok == Admissible(a) IN
    /\ rtD' = IF ok THEN rtD \cup (a.d \ HostD) ELSE rtD
    /\ rtH' = IF ok THEN rtH \cup (a.h \ HostH) ELSE rtH
    /\ out' = IF ok THEN Out("RegAccepted") ELSE Out("RegRefused")
    /\ UNCHANGED <<cfg, data, mig, migT, shadow, reserved>>

\* A registration lands whole when admissible and changes nothing otherwise.
RegistrationAllOrNothing ==
    [][out'.o \in {"RegAccepted", "RegRefused"} =>
       \A a \in Additions :
         Register(a) =>
           IF Admissible(a)
           THEN /\ out' = Out("RegAccepted")
                /\ rtD' = rtD \cup (a.d \ HostD) /\ rtH' = rtH \cup (a.h \ HostH)
           ELSE out' = Out("RegRefused") /\ rtD' = rtD /\ rtH' = rtH]_vars

\* Replaying entries the catalog already holds succeeds and changes nothing.
ReplayIdempotent ==
    [][out'.o \in {"RegAccepted", "RegRefused"} =>
       \A a \in Additions :
         (Register(a) /\ Contained(a)) =>
           out' = Out("RegAccepted") /\ rtD' = rtD /\ rtH' = rtH]_vars

\* A target the facade accepts while the catalog governs it is registered, not
\* lower than the current target, and reachable from it.
ValidatedTarget ==
    [][(out' = Out("CfgAccepted") /\ cfg' # NoCfg /\ Governed)
         => /\ DescReg(cfg')
            /\ (cfg = NoCfg \/ (cfg' >= cfg /\ Chain(cfg, cfg')))]_vars

\* The facade's replace: validated against the catalog when it governs.
SetConfig(t) ==
    /\ IF TargetValid(t)
       THEN cfg' = t /\ out' = Out("CfgAccepted")
       ELSE cfg' = cfg /\ out' = Out("CfgRefused")
    /\ UNCHANGED <<rtD, rtH, data, mig, migT, shadow, reserved>>

\* The facade's advance: validated, then strictly monotonic in the admin.
Advance(t) ==
    /\ IF cfg # NoCfg /\ TargetValid(t) /\ t > cfg
       THEN cfg' = t /\ out' = Out("CfgAccepted")
       ELSE cfg' = cfg /\ out' = Out("CfgRefused")
    /\ UNCHANGED <<rtD, rtH, data, mig, migT, shadow, reserved>>

\* An accepted advance moves an existing target strictly forward.
AdvanceStrict ==
    [][out' = Out("CfgAccepted") =>
       \A t \in Versions : Advance(t) => cfg # NoCfg /\ cfg' > cfg]_vars

ClearConfig ==
    /\ cfg' = NoCfg /\ out' = Out("CfgAccepted")
    /\ UNCHANGED <<rtD, rtH, data, mig, migT, shadow, reserved>>

\* Changing the target, clearing the config or registering a hop rewrites no
\* stored value and nothing a running migration has built.
ConfigNeverRewritesData ==
    [][(cfg' # cfg \/ rtD' # rtD \/ rtH' # rtH)
         => data' = data /\ shadow' = shadow]_vars

\* Environment: a value lands under key k by any path; the tree is quiescent
\* while a migration runs.
Put(k, v) ==
    /\ mig = "Idle"
    /\ data' = [data EXCEPT ![k] = v]
    /\ out' = Out("None")
    /\ UNCHANGED <<rtD, rtH, cfg, mig, migT, shadow, reserved>>

\* A migration starts at the current target and holds the alias reservation.
MigrateBegin ==
    /\ mig = "Idle" /\ cfg # NoCfg
    /\ mig' = "Running" /\ migT' = cfg /\ reserved' = TRUE
    /\ shadow' = Blank /\ out' = Out("None")
    /\ UNCHANGED <<rtD, rtH, cfg, data>>

\* One value is migrated into the build, or the migration aborts naming it:
\* nothing published, the build discarded, the reservation released.
MigrateStep(k) ==
    /\ mig = "Running" /\ shadow[k] = Pending
    /\ LET m == MigrateValue(data[k], migT) IN
       IF m = Stuck \/ Rejected(m, migT)
       THEN /\ mig' = "Idle" /\ migT' = 0 /\ reserved' = FALSE
            /\ shadow' = Blank /\ out' = Aborted(k)
       ELSE /\ shadow' = [shadow EXCEPT ![k] = m] /\ out' = Out("None")
            /\ UNCHANGED <<mig, migT, reserved>>
    /\ UNCHANGED <<rtD, rtH, cfg, data>>

\* Every value is built: the tree cuts over to it and the reservation goes.
MigrateCutover ==
    /\ mig = "Running" /\ \A k \in Keys : shadow[k] # Pending
    /\ data' = shadow /\ shadow' = Blank
    /\ mig' = "Idle" /\ migT' = 0 /\ reserved' = FALSE
    /\ out' = Out("Completed")
    /\ UNCHANGED <<rtD, rtH, cfg>>

\* An abort publishes nothing and releases the reservation, and the value it
\* names is one the migration genuinely could not migrate.
AbortLeavesDataUntouched ==
    [][(mig = "Running" /\ mig' = "Idle" /\ out'.o = "Aborted")
         => /\ data' = data /\ ~reserved'
            /\ shadow[out'.key] = Pending
            /\ NonMigratable(data[out'.key], migT)]_vars

\* A completed migration publishes, under every key, a value that reads back at
\* the migration's target exactly as the read-time upcast of the old one did.
MigrationReadEquivalent ==
    [][(mig = "Running" /\ mig' = "Idle" /\ out' = Out("Completed"))
         => \A k \in Keys : Decode(data'[k], migT) = Decode(data[k], migT)]_vars

\* Every migration ends, completed or aborted.
MigrationTerminates == mig = "Running" ~> mig = "Idle"

Init ==
    /\ rtD = {} /\ rtH = {} /\ cfg = NoCfg
    /\ data = [k \in Keys |-> Abs]
    /\ mig = "Idle" /\ migT = 0 /\ shadow = Blank /\ reserved = FALSE
    /\ out = Out("None")

Stutter == UNCHANGED vars

Next ==
    \/ \E a \in Additions : Register(a)
    \/ \E t \in Versions : SetConfig(t)
    \/ \E t \in Versions : Advance(t)
    \/ ClearConfig
    \/ \E k \in Keys, v \in Landing : Put(k, v)
    \/ MigrateBegin
    \/ \E k \in Keys : MigrateStep(k)
    \/ MigrateCutover
    \/ Stutter

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(\E k \in Keys : MigrateStep(k))
    /\ WF_vars(MigrateCutover)
====
