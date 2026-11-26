---------------------------- MODULE StompClient ----------------------------
(***************************************************************************)
(* The stomp Client's concurrency: application calls, the reader, the       *)
(* writer and the supervisor, against a broker with a queue, a credit       *)
(* window and redelivery of unacknowledged messages on connection loss and  *)
(* on UNSUBSCRIBE. One subscription id is modelled; each Subscribe with it  *)
(* is a new incarnation. Each action is one critical section of client.go   *)
(* or subscription.go; specs/decisions/0010-tla-client-model.md maps them.  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Msgs,        \* messages on the queue at the start
    SendMsgs,    \* messages the application sends
    MaxGen,      \* sessions, so MaxGen - 1 connection losses
    MaxInc,      \* incarnations of the subscription id
    Cap,         \* SubscribeRequest.Capacity
    EndOnOverflow, \* SubscribeRequest.EndOnOverflow
    EarlyDeliver, \* the broker may deliver before the SUBSCRIBE receipt
    Window,      \* broker credit: unacknowledged deliveries per subscription
    Auto,        \* AckAuto when TRUE, AckClientIndividual otherwise
    EnableClose, \* the Client may end: the application calls Close, or reconnecting gives up
    CtxCancel,   \* contexts may end while a call is in progress
    Mutant       \* "none", or a seeded defect the checks must catch

ASSUME /\ MaxGen \in Nat \ {0} /\ MaxInc \in Nat \ {0}
       /\ Cap \in Nat \ {0} /\ Window \in Nat \ {0}
       /\ Auto \in BOOLEAN /\ EndOnOverflow \in BOOLEAN /\ EarlyDeliver \in BOOLEAN /\ EnableClose \in BOOLEAN /\ CtxCancel \in BOOLEAN
       /\ Mutant \in {"none", "nogen", "noseq", "ackonrecv", "ctxack", "nowaiting", "nobeginguard", "overflowdefault", "discardlive", "dropconfirmed", "dialafterclose"}

Incs == 1..MaxInc
NoD == [m |-> "none", g |-> 0, inc |-> 0]
Fr(t, inc, d, r, k) == [t |-> t, inc |-> inc, d |-> d, r |-> r, k |-> k]
Pending == "pending"
Resolve(w) == IF w = Pending THEN "unknown" ELSE w

VARIABLES
    \* Client: c.mu state and the session core for the one id. life is
    \* c.life; state is the State the Client reports; handshook is a
    \* completed handshake that supervise has not yet passed to beginSession.
    life, state, handshook, lateBegin, gen, hasSess, linkUp, sessSub, disconnecting, wbuf,
    subW, unsubW, discW, csub, confirmed, subq, ended, nextInc, curInc,
    \* Transport of the current connection
    conn, c2b, b2c,
    \* Broker
    bq, bsub, unacked, bdisc, bpend,
    \* Application
    appPhase, appInc, handles, autoPend, held, sendSt,
    \* Observations for the properties
    accepted, returned, backed, badAck, afterDisc, misroute, overflowed, taken, permitted, windowAppend

client == <<life, state, handshook, lateBegin, gen, hasSess, linkUp, sessSub, disconnecting, wbuf,
            subW, unsubW, discW, csub, confirmed, subq, ended, nextInc, curInc>>
net == <<conn, c2b, b2c>>
broker == <<bq, bsub, unacked, bdisc, bpend>>
app == <<appPhase, appInc, handles, autoPend, held, sendSt>>
ghost == <<accepted, returned, backed, badAck, afterDisc, misroute, overflowed, taken, permitted, windowAppend>>
vars == <<client, net, broker, app, ghost>>

\* appendLocked; windowAppend records a frame buffered after the session's
\* loops stopped and before runSession cleared c.sess.
Buffer(f) == /\ wbuf' = Append(wbuf, f)
             /\ windowAppend' = (windowAppend \/ ~linkUp)

\* Session.Ack's checks (ackHeaders), reached through enqueue with noSession ErrStaleAck.
Running == life = "running"

CanAck(d) == /\ Running /\ hasSess /\ ~disconnecting
             /\ (d.g = gen \/ Mutant = "nogen")
             /\ sessSub = "active"
             /\ (d.inc = curInc \/ Mutant = "noseq")

\* unsubscribeLocked(i): UNSUBSCRIBE when the session may write one, else End.
UnsubLocked(i, waiter) ==
    IF ~hasSess
    THEN UNCHANGED <<wbuf, sessSub, unsubW, windowAppend, curInc>>
    ELSE IF disconnecting \/ sessSub # "active"
    THEN /\ sessSub' = IF sessSub = "active" THEN "draining" ELSE sessSub
         /\ UNCHANGED <<wbuf, unsubW, windowAppend, curInc>>
    ELSE /\ sessSub' = "draining"
         /\ Buffer(Fr("UNSUB", i, NoD, TRUE, "unsub"))
         /\ unsubW' = IF waiter THEN [unsubW EXCEPT ![i] = Pending] ELSE unsubW
         /\ UNCHANGED curInc

Init ==
    /\ life = "running" /\ state = "connected" /\ handshook = FALSE /\ lateBegin = FALSE
    /\ gen = 1 /\ hasSess = TRUE /\ linkUp = TRUE
    /\ sessSub = "none" /\ disconnecting = FALSE /\ wbuf = <<>>
    /\ subW = [i \in Incs |-> "none"] /\ unsubW = [i \in Incs |-> "none"] /\ discW = "none"
    /\ csub = 0 /\ confirmed = {} /\ subq = [i \in Incs |-> <<>>]
    /\ ended = [i \in Incs |-> FALSE] /\ nextInc = 0 /\ curInc = 0
    /\ conn = TRUE /\ c2b = <<>> /\ b2c = <<>>
    /\ bq = Msgs /\ bsub = 0 /\ unacked = {} /\ bdisc = FALSE /\ bpend = {}
    /\ appPhase = "idle" /\ appInc = 0 /\ handles = {} /\ autoPend = {} /\ held = {}
    /\ sendSt = [m \in SendMsgs |-> "idle"]
    /\ accepted = Msgs /\ returned = {} /\ backed = {}
    /\ badAck = FALSE /\ afterDisc = FALSE /\ misroute = FALSE
    /\ overflowed = FALSE /\ taken = {} /\ permitted = {} /\ windowAppend = FALSE

-----------------------------------------------------------------------------
(* Application calls *)

\* Client.Subscribe: enqueue builds SUBSCRIBE once a session exists.
AppSubscribe ==
    /\ appPhase = "idle" /\ appInc = 0 /\ nextInc < MaxInc
    /\ Running /\ hasSess /\ ~disconnecting /\ sessSub = "none"
    /\ LET i == nextInc + 1 IN
        /\ nextInc' = i /\ csub' = i /\ curInc' = i /\ sessSub' = "active"
        /\ Buffer(Fr("SUB", i, NoD, TRUE, "sub"))
        /\ subW' = [subW EXCEPT ![i] = Pending]
        /\ appPhase' = "subscribing" /\ appInc' = i
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, linkUp, disconnecting, unsubW, discW,
                   confirmed, subq, ended, net, broker, handles, autoPend, held, sendSt,
                   accepted, returned, backed, badAck, afterDisc, misroute, overflowed, taken, permitted>>

\* unsubscribeLocked(s) from Subscribe: a Subscribe that did not get its receipt.
Drop(i) ==
    IF csub = i
    THEN /\ csub' = 0
         /\ ended' = [ended EXCEPT ![i] = TRUE]
         /\ subq' = [subq EXCEPT ![i] = <<>>]
         /\ UnsubLocked(i, FALSE)
    ELSE UNCHANGED <<csub, ended, subq, wbuf, sessSub, unsubW, windowAppend, curInc>>

\* Subscribe's await returns: the receipt, or ErrOutcomeUnknown at session end.
AppSubscribeDone ==
    /\ appPhase = "subscribing" /\ subW[appInc] \in {"ok", "unknown"}
    /\ appPhase' = "idle"
    /\ IF subW[appInc] = "ok"
       THEN /\ handles' = handles \cup {appInc}
            /\ UNCHANGED <<appInc, csub, ended, subq, wbuf, sessSub, unsubW, windowAppend, curInc>>
       ELSE IF appInc \in confirmed /\ csub = appInc /\ Mutant # "dropconfirmed"
       THEN /\ handles' = handles \cup {appInc}
            /\ UNCHANGED <<appInc, csub, ended, subq, wbuf, sessSub, unsubW, windowAppend, curInc>>
       ELSE /\ appInc' = 0 /\ Drop(appInc) /\ UNCHANGED handles
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, linkUp, disconnecting, subW, discW, confirmed,
                   nextInc, net, broker, autoPend, held, sendSt,
                   accepted, returned, backed, badAck, afterDisc, misroute, overflowed, taken, permitted>>

\* Subscribe's ctx ends before the receipt.
AppSubscribeTimeout ==
    /\ CtxCancel /\ appPhase = "subscribing" /\ subW[appInc] = Pending
    /\ appPhase' = "idle"
    /\ IF appInc \in confirmed /\ csub = appInc
       THEN /\ handles' = handles \cup {appInc}
            /\ UNCHANGED <<appInc, csub, ended, subq, wbuf, sessSub, unsubW, windowAppend, curInc, permitted>>
       ELSE /\ appInc' = 0 /\ Drop(appInc)
            /\ permitted' = permitted \cup {appInc}
            /\ UNCHANGED handles
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, linkUp, disconnecting, subW, discW, confirmed,
                   nextInc, net, broker, autoPend, held, sendSt,
                   accepted, returned, backed, badAck, afterDisc, misroute, overflowed, taken>>

\* Subscription.Unsubscribe: queued messages stay for ReadMessage (SUB-14).
AppUnsubscribe ==
    /\ appPhase = "idle" /\ appInc # 0 /\ Running
    /\ appInc' = 0
    /\ IF csub = appInc
       THEN /\ csub' = 0
            /\ ended' = [ended EXCEPT ![appInc] = TRUE]
            /\ UnsubLocked(appInc, TRUE)
       ELSE UNCHANGED <<csub, ended, wbuf, sessSub, unsubW, windowAppend, curInc>>
    /\ permitted' = permitted \cup {appInc}
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, linkUp, disconnecting, subW, discW, confirmed,
                   subq, nextInc, net, broker, appPhase, handles, autoPend, held, sendSt,
                   accepted, returned, backed, badAck, afterDisc, misroute, overflowed, taken>>

\* Subscription.ReadMessage pops under s.mu; AckAuto's acknowledgement is a later step.
AppNext(i) ==
    /\ i \in handles /\ Len(subq[i]) > 0
    /\ LET d == Head(subq[i]) IN
        /\ subq' = [subq EXCEPT ![i] = Tail(@)]
        /\ returned' = returned \cup {d.m}
        /\ IF Auto THEN autoPend' = autoPend \cup {d} /\ UNCHANGED held
                   ELSE held' = held \cup {d} /\ UNCHANGED autoPend
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, linkUp, sessSub, disconnecting, wbuf, subW, unsubW,
                   discW, csub, confirmed, ended, nextInc, curInc, net, broker,
                   appPhase, appInc, handles, sendSt,
                   accepted, backed, badAck, afterDisc, misroute, overflowed, taken, permitted, windowAppend>>

\* ReadMessage returns the end reason once the queue is empty; the handle is let go.
AppNextEnded(i) ==
    /\ i \in handles /\ Len(subq[i]) = 0 /\ ended[i]
    /\ handles' = handles \ {i}
    /\ appInc' = IF appInc = i THEN 0 ELSE appInc
    /\ UNCHANGED <<client, net, broker, appPhase, autoPend, held, sendSt, ghost>>

AckStep(d) ==
    IF CanAck(d)
    THEN Buffer(Fr("ACK", 0, d, FALSE, "none"))
    ELSE UNCHANGED <<wbuf, windowAppend>>

\* Subscription.autoAck.
AutoAck(d) ==
    /\ d \in autoPend
    /\ autoPend' = autoPend \ {d}
    /\ AckStep(d)
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, linkUp, sessSub, disconnecting, subW, unsubW, discW,
                   csub, confirmed, subq, ended, nextInc, curInc, net, broker,
                   appPhase, appInc, handles, held, sendSt,
                   accepted, returned, backed, badAck, afterDisc, misroute, overflowed, taken, permitted>>

\* autoAck ignores the caller's ctx; the "ctxack" mutant restores the
\* defect where an ended ctx dropped the acknowledgement (SUB-32).
AutoAckCtxDone(d) ==
    /\ Mutant = "ctxack" /\ d \in autoPend
    /\ autoPend' = autoPend \ {d}
    /\ UNCHANGED <<client, net, broker, appPhase, appInc, handles, held, sendSt, ghost>>

\* Subscription.Ack in AckClientIndividual mode.
AppAck(d) ==
    /\ d \in held
    /\ held' = held \ {d}
    /\ AckStep(d)
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, linkUp, sessSub, disconnecting, subW, unsubW, discW,
                   csub, confirmed, subq, ended, nextInc, curInc, net, broker,
                   appPhase, appInc, handles, autoPend, sendSt,
                   accepted, returned, backed, badAck, afterDisc, misroute, overflowed, taken, permitted>>

\* Client.Send.
AppSend(m) ==
    /\ sendSt[m] = "idle" /\ Running /\ hasSess /\ ~disconnecting
    /\ Buffer(Fr("SEND", 0, [m |-> m, g |-> 0, inc |-> 0], TRUE, "send"))
    /\ sendSt' = [sendSt EXCEPT ![m] = Pending]
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, linkUp, sessSub, disconnecting, subW, unsubW, discW,
                   csub, confirmed, subq, ended, nextInc, curInc, net, broker,
                   appPhase, appInc, handles, autoPend, held,
                   accepted, returned, backed, badAck, afterDisc, misroute, overflowed, taken, permitted>>

\* Client.Close: DISCONNECT is the last frame the session builds (CON-25).
\* Close then stops the session, which ConnDrop models.
Close ==
    /\ EnableClose /\ Running
    /\ life' = "closing"
    /\ IF hasSess
       THEN /\ disconnecting' = TRUE
            /\ Buffer(Fr("DISC", 0, NoD, TRUE, "disc"))
            /\ discW' = Pending
       ELSE UNCHANGED <<disconnecting, wbuf, windowAppend, discW>>
    /\ permitted' = Incs
    /\ UNCHANGED <<state, handshook, lateBegin, gen, hasSess, linkUp, sessSub, subW, unsubW,
                   csub, confirmed, subq, ended, nextInc, curInc, net, broker, app,
                   accepted, returned, backed, badAck, afterDisc, misroute, overflowed, taken>>

-----------------------------------------------------------------------------
(* Client goroutines *)

\* writeLoop.
Writer ==
    /\ linkUp /\ conn /\ wbuf # <<>>
    /\ c2b' = Append(c2b, Head(wbuf))
    /\ wbuf' = Tail(wbuf)
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, linkUp, sessSub, disconnecting, subW, unsubW, discW,
                   csub, confirmed, subq, ended, nextInc, curInc, conn, b2c, broker, app, ghost>>

Deliver(f) ==
    LET i == csub
        d == [m |-> f.d.m, g |-> gen, inc |-> i]
    IN /\ misroute' = (misroute \/ f.d.inc # i)
       /\ taken' = taken \cup {d}
       /\ confirmed' = confirmed \cup {i}
       /\ UNCHANGED permitted
       /\ IF (EndOnOverflow \/ Mutant = "overflowdefault") /\ Len(subq[i]) >= Cap
          THEN /\ csub' = 0
               /\ ended' = [ended EXCEPT ![i] = TRUE]
               /\ overflowed' = TRUE
               /\ UnsubLocked(i, FALSE)
               /\ UNCHANGED subq
          ELSE /\ subq' = [subq EXCEPT ![i] = Append(@, d)]
               /\ IF Mutant = "ackonrecv" /\ Auto
                  THEN Buffer(Fr("ACK", 0, d, FALSE, "none"))
                  ELSE UNCHANGED <<wbuf, windowAppend>>
               /\ UNCHANGED <<csub, ended, overflowed, sessSub, unsubW, curInc>>

\* readLoop and receive: one frame under c.mu.
ClientRecv ==
    /\ linkUp /\ b2c # <<>>
    /\ b2c' = Tail(b2c)
    /\ LET f == Head(b2c) IN
       IF f.t = "MSG"
       THEN IF sessSub = "active" /\ csub # 0 /\ Mutant # "discardlive"
            THEN /\ Deliver(f)
                 /\ UNCHANGED <<subW, discW, sendSt>>
            ELSE /\ misroute' = (misroute \/ sessSub = "none")
                 /\ taken' = taken \cup {[m |-> f.d.m, g |-> gen, inc |-> f.d.inc]}
                 /\ UNCHANGED <<wbuf, windowAppend, csub, ended, overflowed, permitted, sessSub, unsubW,
                                curInc, subq, subW, discW, confirmed, sendSt>>
       ELSE \* RECEIPT
            /\ confirmed' = IF f.k \in {"sub", "replay"} /\ csub # 0
                            THEN confirmed \cup {csub} ELSE confirmed
            /\ subW' = IF f.k = "sub" /\ subW[f.inc] = Pending
                       THEN [subW EXCEPT ![f.inc] = "ok"] ELSE subW
            /\ sessSub' = IF f.k = "unsub" /\ sessSub = "draining" THEN "none" ELSE sessSub
            /\ unsubW' = IF f.k = "unsub" /\ unsubW[f.inc] = Pending
                         THEN [unsubW EXCEPT ![f.inc] = "ok"] ELSE unsubW
            /\ sendSt' = IF f.k = "send" /\ sendSt[f.d.m] = Pending
                         THEN [sendSt EXCEPT ![f.d.m] = "ok"] ELSE sendSt
            /\ discW' = IF f.k = "disc" /\ discW = Pending THEN "ok" ELSE discW
            /\ UNCHANGED <<wbuf, windowAppend, csub, ended, overflowed, taken, permitted, curInc, subq, misroute>>
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, linkUp, disconnecting, nextInc, conn, c2b, broker,
                   appPhase, appInc, handles, autoPend, held,
                   accepted, returned, backed, badAck, afterDisc>>

\* The connection fails, or Close stops the session: the loops stop, the
\* broker requeues what it delivered and was not acknowledged.
ConnDrop ==
    /\ conn /\ linkUp /\ (gen < MaxGen \/ ~Running)
    /\ conn' = FALSE /\ linkUp' = FALSE /\ c2b' = <<>> /\ b2c' = <<>>
    /\ bq' = bq \cup {d.m : d \in unacked}
    /\ unacked' = {} /\ bsub' = 0 /\ bdisc' = FALSE /\ bpend' = {}
    /\ UNCHANGED <<life, state, handshook, lateBegin, gen, hasSess, sessSub, disconnecting, wbuf, subW, unsubW, discW,
                   csub, confirmed, subq, ended, nextInc, curInc, app, ghost>>

\* runSession's locked cleanup after both loops stopped.
SessionEnd ==
    /\ hasSess /\ ~linkUp
    /\ hasSess' = FALSE
    /\ subW' = [i \in Incs |-> Resolve(subW[i])]
    /\ unsubW' = [i \in Incs |-> Resolve(unsubW[i])]
    /\ sendSt' = [m \in SendMsgs |-> Resolve(sendSt[m])]
    /\ discW' = Resolve(discW)
    /\ IF csub # 0 /\ csub \notin confirmed
       THEN /\ csub' = 0
            /\ ended' = [ended EXCEPT ![csub] = TRUE]
            /\ subq' = [subq EXCEPT ![csub] = <<>>]
       ELSE UNCHANGED <<csub, ended, subq>>
    /\ state' = IF Running /\ Mutant # "nowaiting" THEN "waiting" ELSE state
    /\ UNCHANGED <<life, handshook, lateBegin, gen, linkUp, sessSub, disconnecting, wbuf, confirmed, nextInc,
                   curInc, net, broker, appPhase, appInc, handles, autoPend, held, ghost>>

\* establish: a dial and handshake complete while the Client runs.
\* establish, which reports Connecting only while running (RCN-16). The
\* "dialafterclose" mutant dials after Close began.
Handshake ==
    /\ ~hasSess /\ ~handshook /\ (Running \/ Mutant = "dialafterclose") /\ gen < MaxGen
    /\ handshook' = TRUE /\ state' = "connecting"
    /\ UNCHANGED <<life, lateBegin, gen, hasSess, linkUp, sessSub, disconnecting, wbuf,
                   subW, unsubW, discW, csub, confirmed, subq, ended, nextInc, curInc,
                   net, broker, app, ghost>>

\* beginSession: replays the live subscription, or closes the new connection
\* when Close began during the handshake (CON-30). The "nobeginguard" mutant
\* starts the session regardless.
BeginSession ==
    /\ handshook
    /\ handshook' = FALSE
    /\ IF Running \/ Mutant = "nobeginguard"
       THEN /\ lateBegin' = (lateBegin \/ ~Running)
            /\ gen' = gen + 1 /\ hasSess' = TRUE /\ linkUp' = TRUE /\ conn' = TRUE
            /\ disconnecting' = FALSE
            /\ wbuf' = IF csub # 0 THEN <<Fr("SUB", csub, NoD, TRUE, "replay")>> ELSE <<>>
            /\ sessSub' = IF csub # 0 THEN "active" ELSE "none"
            /\ curInc' = csub
            /\ state' = "connected"
       ELSE UNCHANGED <<lateBegin, gen, hasSess, linkUp, conn, disconnecting, wbuf, sessSub,
                        curInc, state>>
    /\ UNCHANGED <<life, subW, unsubW, discW, csub, confirmed, subq, ended, nextInc,
                   c2b, b2c, broker, app, ghost>>

\* finish: the supervisor ends the Client once no session runs, after Close
\* or when reconnecting gives up (MaxAttempts, a fatal broker error).
Finish ==
    /\ ~hasSess /\ ~handshook /\ (life = "closing" \/ (Running /\ EnableClose))
    /\ life' = "closed" /\ state' = "closed"
    /\ IF csub # 0
       THEN /\ csub' = 0 /\ ended' = [ended EXCEPT ![csub] = TRUE]
       ELSE UNCHANGED <<csub, ended>>
    /\ UNCHANGED <<handshook, lateBegin, gen, hasSess, linkUp, sessSub, disconnecting, wbuf,
                   subW, unsubW, discW, confirmed, subq, nextInc, curInc, net, broker, app, ghost>>

-----------------------------------------------------------------------------
(* Broker *)

Reply(f) == b2c' = Append(b2c, f)

BrokerRecv ==
    /\ conn /\ c2b # <<>> /\ bpend = {}
    /\ c2b' = Tail(c2b)
    /\ LET f == Head(c2b) IN
        /\ afterDisc' = (afterDisc \/ bdisc)
        /\ CASE f.t = "SUB" ->
                  /\ misroute' = (misroute \/ bsub # 0)
                  /\ bsub' = f.inc
                  /\ IF EarlyDeliver
                     THEN /\ bpend' = {Fr("RCPT", f.inc, NoD, FALSE, f.k)}
                          /\ UNCHANGED b2c
                     ELSE /\ Reply(Fr("RCPT", f.inc, NoD, FALSE, f.k))
                          /\ UNCHANGED bpend
                  /\ UNCHANGED <<bq, unacked, bdisc, accepted, backed, badAck>>
             [] f.t = "UNSUB" ->
                  /\ bq' = bq \cup {d.m : d \in unacked}
                  /\ unacked' = {} /\ bsub' = 0
                  /\ Reply(Fr("RCPT", f.inc, NoD, FALSE, "unsub"))
                  /\ UNCHANGED <<bdisc, bpend, accepted, backed, badAck, misroute>>
             [] f.t = "ACK" ->
                  /\ IF f.d \in unacked
                     THEN /\ unacked' = unacked \ {f.d}
                          /\ backed' = backed \cup {f.d.m}
                          /\ UNCHANGED badAck
                     ELSE /\ badAck' = TRUE
                          /\ UNCHANGED <<unacked, backed>>
                  /\ UNCHANGED <<b2c, bq, bsub, bdisc, bpend, accepted, misroute>>
             [] f.t = "SEND" ->
                  /\ accepted' = accepted \cup {f.d.m}
                  /\ bq' = bq \cup {f.d.m}
                  /\ Reply(Fr("RCPT", 0, f.d, FALSE, "send"))
                  /\ UNCHANGED <<bsub, unacked, bdisc, bpend, backed, badAck, misroute>>
             [] f.t = "DISC" ->
                  /\ bdisc' = TRUE
                  /\ Reply(Fr("RCPT", 0, NoD, FALSE, "disc"))
                  /\ UNCHANGED <<bq, bsub, unacked, bpend, accepted, backed, badAck, misroute>>
    /\ UNCHANGED <<client, conn, app, returned, overflowed, taken, permitted, windowAppend>>

\* With EarlyDeliver, the broker sends the SUBSCRIBE receipt after it may
\* already have delivered.
BrokerSubReceipt ==
    /\ conn /\ bpend # {}
    /\ b2c' = Append(b2c, CHOOSE r \in bpend : TRUE)
    /\ bpend' = {}
    /\ UNCHANGED <<client, conn, c2b, bq, bsub, unacked, bdisc, app, ghost>>

BrokerDeliver(m) ==
    /\ conn /\ bsub # 0 /\ ~bdisc /\ m \in bq /\ Cardinality(unacked) < Window
    /\ LET d == [m |-> m, g |-> gen, inc |-> bsub] IN
        /\ bq' = bq \ {m}
        /\ unacked' = unacked \cup {d}
        /\ b2c' = Append(b2c, Fr("MSG", 0, d, FALSE, "none"))
    /\ UNCHANGED <<client, conn, c2b, bsub, bdisc, bpend, app, ghost>>

-----------------------------------------------------------------------------

AllMsgs == Msgs \cup SendMsgs

Next ==
    \/ AppSubscribe \/ AppSubscribeDone \/ AppSubscribeTimeout \/ AppUnsubscribe
    \/ \E i \in Incs : AppNext(i) \/ AppNextEnded(i)
    \/ \E d \in autoPend : AutoAck(d) \/ AutoAckCtxDone(d)
    \/ \E d \in held : AppAck(d)
    \/ \E m \in SendMsgs : AppSend(m)
    \/ Close
    \/ Writer \/ ClientRecv \/ ConnDrop \/ SessionEnd \/ Handshake \/ BeginSession \/ Finish
    \/ BrokerRecv \/ BrokerSubReceipt \/ \E m \in AllMsgs : BrokerDeliver(m)

\* Faults, Close and ended contexts are not fair: they may never happen.
Fairness ==
    /\ WF_vars(AppSubscribe) /\ WF_vars(AppSubscribeDone)
    /\ WF_vars(\E i \in Incs : AppNext(i)) /\ WF_vars(\E i \in Incs : AppNextEnded(i))
    /\ WF_vars(\E d \in autoPend : AutoAck(d)) /\ WF_vars(\E d \in held : AppAck(d))
    /\ WF_vars(\E m \in SendMsgs : AppSend(m))
    /\ WF_vars(Writer) /\ WF_vars(ClientRecv) /\ WF_vars(SessionEnd)
    /\ WF_vars(Handshake) /\ WF_vars(BeginSession)
    /\ WF_vars(BrokerRecv) /\ WF_vars(BrokerSubReceipt) /\ WF_vars(\E m \in AllMsgs : BrokerDeliver(m))

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Properties *)

\* SUB-9, SUB-28: no message is acknowledged before ReadMessage stored it.
AckedWereReturned == backed \subseteq returned

\* SUB-9: every delivery the reader took is still queued or was returned by
\* ReadMessage, unless the application permitted discarding: it unsubscribed or
\* gave up that subscription, closed the Client, or set EndOnOverflow
\* (SUB-43, SUB-44, SUB-45).
Queued(d) == d.inc \in Incs /\ \E k \in 1..Len(subq[d.inc]) : subq[d.inc][k] = d
NoUnpermittedDiscard ==
    \/ EndOnOverflow
    \/ \A d \in taken : Queued(d) \/ d.m \in returned \/ d.inc \in permitted

\* SUB-48: a subscription holding messages has been confirmed, by its receipt
\* or by a delivery, so a session end keeps it.
QueuedMeansConfirmed == \A i \in Incs : Len(subq[i]) > 0 => i \in confirmed

\* SUB-21, SUB-27: every ACK names a delivery outstanding on its connection.
AcksNameOutstandingDeliveries == ~badAck

\* CON-25.
NoFrameAfterDisconnect == ~afterDisc

\* SND-5: Send returns nil only after the broker accepted the message.
SendOkMeansAccepted == \A m \in SendMsgs : sendSt[m] = "ok" => m \in accepted

\* MESSAGE frames reach the incarnation the broker delivered them to, and
\* the broker never sees a second SUBSCRIBE for a live id (SUB-4).
NoMisroute == ~misroute

\* RCN-15: the Client reports Connected only while a session runs, except
\* while Close is ending it.
ConnectedMeansSession == Running /\ state = "connected" => hasSess

\* CON-30: no session begins once Close began.
NoSessionAfterClose == ~lateBegin

\* The lifecycle only moves forward, and closed means no session runs.
ClosedHasNoSession == life = "closed" => ~hasSess /\ ~handshook /\ state = "closed"
\* RCN-16: once Close began, the reported state changes only to Closed.
StateFrozenAfterClose ==
    [][life # "running" => state' \in {state, "closed"}]_<<life, state>>

LifeOnlyForward ==
    [][\/ life' = life
       \/ life = "running" /\ life' \in {"closing", "closed"}
       \/ life = "closing" /\ life' = "closed"]_life

Safety == /\ AckedWereReturned /\ NoUnpermittedDiscard /\ QueuedMeansConfirmed /\ AcksNameOutstandingDeliveries /\ NoFrameAfterDisconnect
          /\ SendOkMeansAccepted /\ NoMisroute /\ ConnectedMeansSession
          /\ NoSessionAfterClose /\ ClosedHasNoSession

\* SUB-9 liveness: once faults stop, every message reaches the application,
\* unless the model ran out of subscription incarnations.
Exhausted == nextInc = MaxInc /\ csub = 0 /\ appPhase = "idle"
EveryMessageReturned == <>(AllMsgs \subseteq returned \/ Exhausted)

\* Probes: each must be violated, showing the model reaches the case.
ProbeQueuedAcrossReconnect ==
    ~(\E i \in Incs : \E k \in 1..Len(subq[i]) : subq[i][k].g < gen)
ProbeOverflow == ~overflowed
ProbeConfirmedByMessage == ~(\E i \in handles : subW[i] = "unknown")
ProbeAppendAfterLoopsStopped == ~windowAppend
ProbeResubscribedHoldingOld ==
    ~(\E d \in held \cup autoPend : d.inc < curInc /\ sessSub = "active")
=============================================================================
