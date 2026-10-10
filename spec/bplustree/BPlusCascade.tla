---- MODULE BPlusCascade ----
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Keys, Fanout, MaxHeight, MaxLeaves, MaxNodes, MaxCrashes

NoNode == 0
NoKey == 0
NodePool == 1..MaxNodes

VARIABLES active, kind, parent, children, nodeKeys, nodeLevel,
          previousLeaf, nextLeaf, root, firstLeaf, rightmost, treeHeight,
          acked, pending, ready, crashes

vars ==
    <<active, kind, parent, children, nodeKeys, nodeLevel,
      previousLeaf, nextLeaf, root, firstLeaf, rightmost, treeHeight,
      acked, pending, ready, crashes>>

NoPending ==
    [kind |-> "none",
     donor |-> NoNode,
     sibling |-> NoNode,
     parent |-> NoNode,
     separator |-> NoKey]

MinSet(S) == CHOOSE x \in S : \A y \in S : x <= y
MaxSet(S) == CHOOSE x \in S : \A y \in S : y <= x

LeftHalf(S) ==
    LET middle == Cardinality(S) \div 2
        pivot == CHOOSE k \in S :
            Cardinality({x \in S : x <= k}) = middle
    IN {k \in S : k <= pivot}

RightHalf(S) == S \ LeftHalf(S)

ActiveLeaves == {n \in active : kind[n] = "leaf"}
ActiveInternals == {n \in active : kind[n] = "internal"}

ChildNodes(n) ==
    {children[n][i].node : i \in 1..Len(children[n])}

PendingDetached ==
    IF pending.kind \in {"leaf-link", "internal-link", "root-promotion"}
    THEN {pending.sibling}
    ELSE {}

RECURSIVE Ancestors(_)

AddToAncestors(values, node, keys) ==
    [n \in NodePool |->
        IF n \in Ancestors(node) THEN values[n] \cup keys ELSE values[n]]

RemoveFromAncestors(values, node, keys) ==
    [n \in NodePool |->
        IF n \in Ancestors(node) THEN values[n] \ keys ELSE values[n]]

Ancestors(node) ==
    IF parent[node] = NoNode
    THEN {node}
    ELSE {node} \cup Ancestors(parent[node])

RECURSIVE ReachableFrom(_)
ReachableFrom(node) ==
    {node} \cup
        (IF kind[node] = "internal"
         THEN UNION {
             ReachableFrom(children[node][i].node) :
                 i \in 1..Len(children[node])}
         ELSE {})

RECURSIVE ChainFrom(_)
ChainFrom(node) ==
    IF node = NoNode
    THEN <<>>
    ELSE <<node>> \o ChainFrom(nextLeaf[node])

RECURSIVE RouteToLeaf(_, _)
RouteToLeaf(node, key) ==
    IF kind[node] = "leaf"
    THEN node
    ELSE
        LET candidates ==
                {i \in 1..Len(children[node]) :
                    children[node][i].separator = NoKey
                    \/ children[node][i].separator <= key}
            selected == MaxSet(candidates)
        IN RouteToLeaf(children[node][selected].node, key)

LeafChain == ChainFrom(firstLeaf)

FreshNode == MinSet(NodePool \ active)

TypeOK ==
    /\ active \subseteq NodePool
    /\ kind \in [NodePool -> {"unused", "leaf", "internal"}]
    /\ active = {n \in NodePool : kind[n] # "unused"}
    /\ parent \in [NodePool -> (NodePool \cup {NoNode})]
    /\ children \in
        [NodePool -> Seq([node : NodePool, separator : (Keys \cup {NoKey})])]
    /\ nodeKeys \in [NodePool -> SUBSET Keys]
    /\ nodeLevel \in [NodePool -> 0..(MaxHeight + 1)]
    /\ previousLeaf \in [NodePool -> (NodePool \cup {NoNode})]
    /\ nextLeaf \in [NodePool -> (NodePool \cup {NoNode})]
    /\ root \in active
    /\ firstLeaf \in ActiveLeaves
    /\ rightmost \in ActiveLeaves
    /\ treeHeight \in 3..(MaxHeight + 1)
    /\ acked \subseteq Keys
    /\ pending.kind \in
        {"none", "leaf-birth", "leaf-link", "internal-split",
         "internal-link", "root-promotion"}
    /\ pending.donor \in (NodePool \cup {NoNode})
    /\ pending.sibling \in (NodePool \cup {NoNode})
    /\ pending.parent \in (NodePool \cup {NoNode})
    /\ pending.separator \in (Keys \cup {NoKey})
    /\ ready \in BOOLEAN
    /\ crashes \in 0..MaxCrashes
    /\ \A n \in active :
        /\ kind[n] \in {"leaf", "internal"}
        /\ nodeKeys[n] # {}
        /\ nodeLevel[n] \in 1..treeHeight
    /\ \A n \in (NodePool \ active) :
        /\ kind[n] = "unused"
        /\ parent[n] = NoNode
        /\ children[n] = <<>>
        /\ nodeKeys[n] = {}
        /\ nodeLevel[n] = 0
        /\ previousLeaf[n] = NoNode
        /\ nextLeaf[n] = NoNode
    /\ \A n \in NodePool :
        \A i \in 1..Len(children[n]) :
            children[n][i].node \in NodePool
            /\ children[n][i].separator \in (Keys \cup {NoKey})

NoKeyLostOrDuplicated ==
    /\ UNION {nodeKeys[n] : n \in ActiveLeaves} = acked
    /\ \A left, right \in ActiveLeaves :
        left # right => nodeKeys[left] \cap nodeKeys[right] = {}

SortedLeafChain ==
    LET chain == LeafChain
        last == Len(chain)
    IN
        /\ last = Cardinality(ActiveLeaves)
        /\ {chain[i] : i \in 1..last} = ActiveLeaves
        /\ chain[1] = firstLeaf
        /\ chain[last] = rightmost
        /\ previousLeaf[firstLeaf] = NoNode
        /\ nextLeaf[chain[last]] = NoNode
        /\ \A i \in 1..(last - 1) :
            /\ nextLeaf[chain[i]] = chain[i + 1]
            /\ previousLeaf[chain[i + 1]] = chain[i]
            /\ MaxSet(nodeKeys[chain[i]]) < MinSet(nodeKeys[chain[i + 1]])

ParentChildAccounting ==
    /\ \A p \in ActiveInternals :
        Len(children[p]) = Cardinality(ChildNodes(p))
    /\ \A p \in ActiveInternals :
        \A i \in 1..Len(children[p]) :
            parent[children[p][i].node] = p
    /\ \A n \in active :
        /\ Cardinality({p \in ActiveInternals : n \in ChildNodes(p)}) =
            IF n = root \/ n \in PendingDetached THEN 0 ELSE 1
        /\ IF n = root \/ n \in PendingDetached
           THEN parent[n] = NoNode
           ELSE parent[n] \in ActiveInternals
    /\ active =
        ReachableFrom(root)
        \cup
            (IF PendingDetached = {}
             THEN {}
             ELSE ReachableFrom(pending.sibling))
    /\ \A p \in ActiveInternals :
        \A i \in 1..Len(children[p]) :
            nodeLevel[children[p][i].node] = nodeLevel[p] + 1
    /\ nodeLevel[root] = 1
    /\ \A n \in ActiveLeaves : nodeLevel[n] = treeHeight
    /\ \A n \in PendingDetached : nodeLevel[n] = nodeLevel[pending.donor]

SeparatorRanges ==
    \A p \in ActiveInternals :
        /\ Len(children[p]) >= 2
        /\ children[p][1].separator = NoKey
        /\ \A i \in 2..Len(children[p]) :
            LET left == children[p][i - 1].node
                right == children[p][i].node
                separator == children[p][i].separator
            IN
                /\ separator = MinSet(nodeKeys[right])
                /\ MaxSet(nodeKeys[left]) < separator

FanoutBound ==
    /\ Cardinality(ActiveLeaves) <= MaxLeaves
    /\ \A n \in ActiveLeaves :
        IF pending.kind = "leaf-birth" /\ pending.donor = n
        THEN Cardinality(nodeKeys[n]) = Fanout + 1
        ELSE Cardinality(nodeKeys[n]) \in 1..Fanout
    /\ \A n \in ActiveInternals :
        IF pending.kind = "internal-split" /\ pending.donor = n
        THEN Len(children[n]) = Fanout + 1
        ELSE Len(children[n]) \in 2..Fanout

RootHasOneRoot ==
    /\ kind[root] = "internal"
    /\ parent[root] = NoNode
    /\ IF pending.kind = "internal-split" /\ pending.donor = root
       THEN Len(children[root]) = Fanout + 1
       ELSE Len(children[root]) \in 2..Fanout
    /\ (pending.kind = "root-promotion" => pending.donor = root)

HeightBound ==
    /\ treeHeight <= MaxHeight
    /\ \A n \in active : nodeLevel[n] \in 1..treeHeight

RecoverablePending ==
    CASE pending.kind = "none" ->
            /\ active = ReachableFrom(root)
            /\ nodeKeys[root] = acked
        [] pending.kind = "leaf-birth" ->
            /\ kind[pending.donor] = "leaf"
            /\ pending.sibling \notin active
            /\ Cardinality(nodeKeys[pending.donor]) = Fanout + 1
            /\ pending.separator =
                MinSet(RightHalf(nodeKeys[pending.donor]))
            /\ parent[pending.donor] = pending.parent
        [] pending.kind = "leaf-link" ->
            /\ kind[pending.donor] = "leaf"
            /\ kind[pending.sibling] = "leaf"
            /\ parent[pending.sibling] = NoNode
            /\ nextLeaf[pending.donor] = pending.sibling
            /\ pending.separator = MinSet(nodeKeys[pending.sibling])
            /\ nodeKeys[root] \cup nodeKeys[pending.sibling] = acked
        [] pending.kind = "internal-split" ->
            /\ kind[pending.donor] = "internal"
            /\ Len(children[pending.donor]) = Fanout + 1
            /\ nodeKeys[root] = acked
        [] pending.kind = "internal-link" ->
            /\ kind[pending.donor] = "internal"
            /\ kind[pending.sibling] = "internal"
            /\ parent[pending.sibling] = NoNode
            /\ parent[pending.donor] = pending.parent
            /\ pending.separator = MinSet(nodeKeys[pending.sibling])
            /\ nodeKeys[root] \cup nodeKeys[pending.sibling] = acked
        [] pending.kind = "root-promotion" ->
            /\ pending.donor = root
            /\ kind[pending.sibling] = "internal"
            /\ parent[pending.sibling] = NoNode
            /\ nodeKeys[root] \cup nodeKeys[pending.sibling] = acked

RoutedKeysOwned ==
    pending.kind = "none" =>
        \A key \in acked :
            key \in nodeKeys[RouteToLeaf(root, key)]

Init ==
    /\ active = {1, 2, 3, 4, 5, 6, 7}
    /\ kind =
        [n \in NodePool |->
            IF n = 1 \/ n \in {2, 3} THEN "internal"
            ELSE IF n \in {4, 5, 6, 7} THEN "leaf"
            ELSE "unused"]
    /\ parent =
        [n \in NodePool |->
            IF n = 1 THEN NoNode
            ELSE IF n \in {2, 3} THEN 1
            ELSE IF n \in {4, 5} THEN 2
            ELSE IF n \in {6, 7} THEN 3
            ELSE NoNode]
    /\ children =
        [n \in NodePool |->
            IF n = 1
            THEN <<[node |-> 2, separator |-> NoKey],
                    [node |-> 3, separator |-> 3]>>
            ELSE IF n = 2
            THEN <<[node |-> 4, separator |-> NoKey],
                    [node |-> 5, separator |-> 2]>>
            ELSE IF n = 3
            THEN <<[node |-> 6, separator |-> NoKey],
                    [node |-> 7, separator |-> 4]>>
            ELSE <<>>]
    /\ nodeKeys =
        [n \in NodePool |->
            IF n = 1 THEN {1, 2, 3, 4}
            ELSE IF n = 2 THEN {1, 2}
            ELSE IF n = 3 THEN {3, 4}
            ELSE IF n = 4 THEN {1}
            ELSE IF n = 5 THEN {2}
            ELSE IF n = 6 THEN {3}
            ELSE IF n = 7 THEN {4}
            ELSE {}]
    /\ nodeLevel =
        [n \in NodePool |->
            IF n = 1 THEN 1
            ELSE IF n \in {2, 3} THEN 2
            ELSE IF n \in {4, 5, 6, 7} THEN 3
            ELSE 0]
    /\ previousLeaf =
        [n \in NodePool |->
            IF n = 5 THEN 4
            ELSE IF n = 6 THEN 5
            ELSE IF n = 7 THEN 6
            ELSE NoNode]
    /\ nextLeaf =
        [n \in NodePool |->
            IF n = 4 THEN 5
            ELSE IF n = 5 THEN 6
            ELSE IF n = 6 THEN 7
            ELSE NoNode]
    /\ root = 1
    /\ firstLeaf = 4
    /\ rightmost = 7
    /\ treeHeight = 3
    /\ acked = {1, 2, 3, 4}
    /\ pending = NoPending
    /\ ready = TRUE
    /\ crashes = 0

Write(key) ==
    /\ ready
    /\ key \in Keys \ acked
    /\ key = MinSet(Keys \ acked)
    /\ pending.kind \notin
        {"leaf-birth", "leaf-link", "internal-link", "root-promotion"}
    /\ LET updatedLeafKeys == nodeKeys[rightmost] \cup {key}
       IN
            /\ IF Cardinality(updatedLeafKeys) <= Fanout
               THEN TRUE
               ELSE
                    /\ pending.kind = "none"
                    /\ Cardinality(ActiveLeaves) < MaxLeaves
                    /\ NodePool \ active # {}
            /\ nodeKeys' = AddToAncestors(nodeKeys, rightmost, {key})
            /\ acked' = acked \cup {key}
            /\ pending' =
                IF Cardinality(updatedLeafKeys) > Fanout
                THEN [kind |-> "leaf-birth",
                      donor |-> rightmost,
                      sibling |-> FreshNode,
                      parent |-> parent[rightmost],
                      separator |-> MinSet(RightHalf(updatedLeafKeys))]
                ELSE pending
    /\ UNCHANGED
        <<active, kind, parent, children, nodeLevel, previousLeaf, nextLeaf,
          root, firstLeaf, rightmost, treeHeight, ready, crashes>>

WriteNext == \E key \in Keys : Write(key)

BirthLeaf ==
    /\ ready
    /\ pending.kind = "leaf-birth"
    /\ LET donor == pending.donor
           sibling == pending.sibling
           leftKeys == LeftHalf(nodeKeys[donor])
           rightKeys == RightHalf(nodeKeys[donor])
       IN
            /\ active' = active \cup {sibling}
            /\ kind' =
                [n \in NodePool |->
                    IF n = sibling THEN "leaf" ELSE kind[n]]
            /\ nodeKeys' =
                [n \in NodePool |->
                    IF n = donor THEN leftKeys
                    ELSE IF n = sibling THEN rightKeys
                    ELSE IF parent[donor] # NoNode
                            /\ n \in Ancestors(parent[donor])
                         THEN nodeKeys[n] \ rightKeys
                         ELSE nodeKeys[n]]
            /\ nodeLevel' =
                [n \in NodePool |->
                    IF n = sibling THEN nodeLevel[donor] ELSE nodeLevel[n]]
            /\ previousLeaf' =
                [n \in NodePool |->
                    IF n = sibling THEN donor ELSE previousLeaf[n]]
            /\ nextLeaf' =
                [n \in NodePool |->
                    IF n = donor THEN sibling ELSE nextLeaf[n]]
            /\ rightmost' = sibling
            /\ pending' =
                [pending EXCEPT !.kind = "leaf-link"]
    /\ UNCHANGED
        <<parent, children, root, firstLeaf, treeHeight, acked, ready, crashes>>

LinkLeaf ==
    /\ ready
    /\ pending.kind = "leaf-link"
    /\ LET donor == pending.donor
           sibling == pending.sibling
           parentNode == pending.parent
           newEntries == Append(
               children[parentNode],
               [node |-> sibling, separator |-> pending.separator])
       IN
            /\ parent[donor] = parentNode
            /\ children[parentNode][Len(children[parentNode])].node = donor
            /\ children' =
                [n \in NodePool |->
                    IF n = parentNode THEN newEntries ELSE children[n]]
            /\ parent' =
                [n \in NodePool |->
                    IF n = sibling THEN parentNode ELSE parent[n]]
            /\ nodeKeys' =
                AddToAncestors(nodeKeys, parentNode, nodeKeys[sibling])
            /\ pending' =
                IF Len(newEntries) > Fanout
                THEN [kind |-> "internal-split",
                      donor |-> parentNode,
                      sibling |-> NoNode,
                      parent |-> parent[parentNode],
                      separator |-> NoKey]
                ELSE NoPending
    /\ UNCHANGED
        <<active, kind, nodeLevel, previousLeaf, nextLeaf, root, firstLeaf,
          rightmost, treeHeight, acked, ready, crashes>>

SplitInternal ==
    /\ ready
    /\ pending.kind = "internal-split"
    /\ NodePool \ active # {}
    /\ LET donor == pending.donor
           sibling == FreshNode
           oldParent == parent[donor]
           middle == Len(children[donor]) \div 2
           leftEntries == SubSeq(children[donor], 1, middle)
           rightEntries == SubSeq(children[donor], middle + 1, Len(children[donor]))
           promoted == rightEntries[1].separator
           siblingEntries ==
                [i \in 1..Len(rightEntries) |->
                    IF i = 1
                    THEN [rightEntries[i] EXCEPT !.separator = NoKey]
                    ELSE rightEntries[i]]
           moved == {rightEntries[i].node : i \in 1..Len(rightEntries)}
           leftKeys ==
                UNION {nodeKeys[leftEntries[i].node] : i \in 1..Len(leftEntries)}
           rightKeys ==
                UNION {nodeKeys[rightEntries[i].node] : i \in 1..Len(rightEntries)}
       IN
            /\ active' = active \cup {sibling}
            /\ kind' =
                [n \in NodePool |->
                    IF n = sibling THEN "internal" ELSE kind[n]]
            /\ parent' =
                [n \in NodePool |->
                    IF n \in moved THEN sibling ELSE parent[n]]
            /\ children' =
                [n \in NodePool |->
                    IF n = donor THEN leftEntries
                    ELSE IF n = sibling THEN siblingEntries
                    ELSE children[n]]
            /\ nodeKeys' =
                [n \in NodePool |->
                    IF n = donor THEN leftKeys
                    ELSE IF n = sibling THEN rightKeys
                    ELSE IF oldParent # NoNode /\ n \in Ancestors(oldParent)
                         THEN nodeKeys[n] \ rightKeys
                         ELSE nodeKeys[n]]
            /\ nodeLevel' =
                [n \in NodePool |->
                    IF n = sibling THEN nodeLevel[donor] ELSE nodeLevel[n]]
            /\ pending' =
                IF donor = root
                THEN [kind |-> "root-promotion",
                      donor |-> donor,
                      sibling |-> sibling,
                      parent |-> NoNode,
                      separator |-> promoted]
                ELSE [kind |-> "internal-link",
                      donor |-> donor,
                      sibling |-> sibling,
                      parent |-> oldParent,
                      separator |-> promoted]
    /\ UNCHANGED
        <<previousLeaf, nextLeaf, root, firstLeaf, rightmost, treeHeight,
          acked, ready, crashes>>

LinkInternal ==
    /\ ready
    /\ pending.kind = "internal-link"
    /\ LET donor == pending.donor
           sibling == pending.sibling
           parentNode == pending.parent
           newEntries == Append(
               children[parentNode],
               [node |-> sibling, separator |-> pending.separator])
       IN
            /\ parent[donor] = parentNode
            /\ children[parentNode][Len(children[parentNode])].node = donor
            /\ children' =
                [n \in NodePool |->
                    IF n = parentNode THEN newEntries ELSE children[n]]
            /\ parent' =
                [n \in NodePool |->
                    IF n = sibling THEN parentNode ELSE parent[n]]
            /\ nodeKeys' =
                AddToAncestors(nodeKeys, parentNode, nodeKeys[sibling])
            /\ pending' =
                IF Len(newEntries) > Fanout
                THEN [kind |-> "internal-split",
                      donor |-> parentNode,
                      sibling |-> NoNode,
                      parent |-> parent[parentNode],
                      separator |-> NoKey]
                ELSE NoPending
    /\ UNCHANGED
        <<active, kind, nodeLevel, previousLeaf, nextLeaf, root, firstLeaf,
          rightmost, treeHeight, acked, ready, crashes>>

PromoteRoot ==
    /\ ready
    /\ pending.kind = "root-promotion"
    /\ treeHeight < MaxHeight
    /\ NodePool \ active # {}
    /\ LET oldRoot == root
           sibling == pending.sibling
           newRoot == FreshNode
           newRootEntries ==
               <<[node |-> oldRoot, separator |-> NoKey],
                 [node |-> sibling, separator |-> pending.separator]>>
       IN
            /\ active' = active \cup {newRoot}
            /\ kind' =
                [n \in NodePool |->
                    IF n = newRoot THEN "internal" ELSE kind[n]]
            /\ parent' =
                [n \in NodePool |->
                    IF n \in {oldRoot, sibling} THEN newRoot ELSE parent[n]]
            /\ children' =
                [n \in NodePool |->
                    IF n = newRoot THEN newRootEntries ELSE children[n]]
            /\ nodeKeys' =
                [n \in NodePool |->
                    IF n = newRoot
                    THEN nodeKeys[oldRoot] \cup nodeKeys[sibling]
                    ELSE nodeKeys[n]]
            /\ nodeLevel' =
                [n \in NodePool |->
                    IF n = newRoot THEN 1
                    ELSE IF n \in active THEN nodeLevel[n] + 1
                    ELSE nodeLevel[n]]
            /\ root' = newRoot
            /\ treeHeight' = treeHeight + 1
            /\ pending' = NoPending
    /\ UNCHANGED
        <<previousLeaf, nextLeaf, firstLeaf, rightmost, acked, ready, crashes>>

Crash ==
    /\ ready
    /\ pending.kind # "none"
    /\ crashes < MaxCrashes
    /\ ready' = FALSE
    /\ crashes' = crashes + 1
    /\ UNCHANGED
        <<active, kind, parent, children, nodeKeys, nodeLevel,
          previousLeaf, nextLeaf, root, firstLeaf, rightmost, treeHeight,
          acked, pending>>

Recover ==
    /\ ~ready
    /\ pending.kind # "none"
    /\ ready' = TRUE
    /\ UNCHANGED
        <<active, kind, parent, children, nodeKeys, nodeLevel,
          previousLeaf, nextLeaf, root, firstLeaf, rightmost, treeHeight,
          acked, pending, crashes>>

Stutter == UNCHANGED vars

ReachMaxHeight == <> (treeHeight = MaxHeight)

Next ==
    \/ \E key \in Keys : Write(key)
    \/ BirthLeaf
    \/ LinkLeaf
    \/ SplitInternal
    \/ LinkInternal
    \/ PromoteRoot
    \/ Crash
    \/ Recover
    \/ Stutter

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(WriteNext)
    /\ WF_vars(BirthLeaf)
    /\ WF_vars(LinkLeaf)
    /\ WF_vars(SplitInternal)
    /\ WF_vars(LinkInternal)
    /\ WF_vars(PromoteRoot)
    /\ WF_vars(Recover)
====
