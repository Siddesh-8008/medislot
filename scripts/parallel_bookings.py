"""Fire N simultaneous bookings at one slot of the deployed MediSlot API and print the tally.

For the Milestone 5 checkpoint: run it against a capacity-3 Health Camp slot and expect exactly 3 x 201 and
N-3 x 409 SLOT_FULL. Then open the CloudWatch dashboard medislot-<env>-booking to see BookingConflictRetries rise
(contention really happened and was retried, not avoided).

Standard library only. Patients are registered on the fly (unique e-mails / phones) unless you pass --tokens-file
with one patient access token per line.

    python scripts/parallel_bookings.py --api https://<id>.execute-api.ap-south-1.amazonaws.com/dev \\
        --doctor-id 42 --type-id 4 --days-ahead 9 --contenders 25

    # or a specific slot:
    python scripts/parallel_bookings.py --api ... --slot-id 'SLOT#<doctorId>_<YYYYMMDDTHHMMSSZ>_<typeId>' --contenders 25 --cleanup
"""
import argparse
import json
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

PASSWORD = "Parallel-Test-9x!"


def call(api, method, path, token=None, body=None, headers=None, params=None):
    url = api.rstrip("/") + path
    if params:
        url += "?" + "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items())
    h = {"content-type": "application/json", **(headers or {})}
    if token:
        h["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, method=method, headers=h, data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            return r.status, json.loads(r.read() or b"{}"), dict(r.headers)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw or b"{}"), dict(e.headers)
        except ValueError:
            return e.code, {"raw": raw[:200].decode(errors="replace")}, dict(e.headers)


def new_patient(api, run, i):
    email = f"parallel-{run}-{i}@example.com"
    for _ in range(5):
        phone = "9" + "".join(random.choice("0123456789") for _ in range(9))
        st, body, _ = call(api, "POST", "/auth/register/patient", body={
            "firstName": "Parallel", "lastName": "Tester", "email": email, "password": PASSWORD, "phone": phone,
            "dateOfBirth": "1990-01-01", "gender": "Other"})
        if st == 201:
            break
    st, body, _ = call(api, "POST", "/auth/login", body={"email": email, "password": PASSWORD})
    if st != 200:
        raise SystemExit(f"login failed for {email}: {st} {body}")
    return body["accessToken"]


def pick_slot(api, token, doctor_id, type_id, days_ahead):
    first = date.today() + timedelta(days=days_ahead)
    st, body, _ = call(api, "GET", f"/doctors/{doctor_id}/slots", token, params={
        "from": first.isoformat(), "to": (first + timedelta(days=6)).isoformat(), "consultationTypeId": type_id, "availableOnly": "true"})
    if st != 200:
        raise SystemExit(f"could not read slots: {st} {body}")
    free = [s for d in body["days"] for s in d["slots"] if s["freeSeats"] == s["capacity"]]
    if not free:
        raise SystemExit("no completely free slot found in that week")
    s = random.choice(free)
    print(f"slot {s['slotId']}  {s['startAt']}  capacity {s['capacity']}")
    return s["slotId"]


def book(api, token, slot_id, retries):
    key = "par-" + uuid.uuid4().hex
    for attempt in range(retries + 1):
        st, body, headers = call(api, "POST", "/appointments", token, body={"slotId": slot_id}, headers={"Idempotency-Key": key})
        retryable = st in (429, 500, 502, 503, 504) or (st == 409 and body.get("code") in ("SLOT_BUSY", "REQUEST_IN_PROGRESS"))
        if not retryable or attempt == retries:
            return st, body
        time.sleep(0.5 + attempt + random.random())   # same key on purpose: a retry can never double-book


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", required=True)
    ap.add_argument("--slot-id")
    ap.add_argument("--doctor-id", type=int)
    ap.add_argument("--type-id", type=int, default=4)
    ap.add_argument("--days-ahead", type=int, default=9)
    ap.add_argument("--contenders", type=int, default=21)
    ap.add_argument("--tokens-file")
    ap.add_argument("--retries", type=int, default=5, help="re-sends with the SAME key on 429/5xx/SLOT_BUSY")
    ap.add_argument("--cleanup", action="store_true", help="cancel the winning appointments afterwards")
    a = ap.parse_args()

    run = uuid.uuid4().hex[:8]
    if a.tokens_file:
        tokens = [t.strip() for t in open(a.tokens_file) if t.strip()][: a.contenders]
    else:
        print(f"registering {a.contenders} patients ...")
        with ThreadPoolExecutor(8) as pool:
            tokens = list(pool.map(lambda i: new_patient(a.api, run, i), range(a.contenders)))
    slot_id = a.slot_id or pick_slot(a.api, tokens[0], a.doctor_id, a.type_id, a.days_ahead)

    gate = threading.Barrier(len(tokens))

    def one(token):
        gate.wait()
        t0 = time.time()
        st, body = book(a.api, token, slot_id, a.retries)
        return token, st, body, time.time() - t0

    print(f"firing {len(tokens)} simultaneous bookings ...")
    with ThreadPoolExecutor(len(tokens)) as pool:
        results = list(pool.map(one, tokens))
    tally = Counter(f"{st} {body.get('code', 'CONFIRMED' if st == 201 else '')}".strip() for _, st, body, _ in results)
    for outcome, n in sorted(tally.items()):
        print(f"  {n:3d} x {outcome}")
    print(f"  slowest response {max(r[3] for r in results):.2f}s")
    winners = [(tok, body["appointment"]["appointmentId"]) for tok, st, body, _ in results if st == 201]
    print("appointment ids:", [w[1] for w in winners])
    if a.cleanup:
        for tok, appointment_id in winners:
            st, body, _ = call(a.api, "POST", f"/appointments/{appointment_id}/cancel", tok, body={})
            print(f"  cancelled {appointment_id}: {st} refund {body.get('refund', {}).get('refundAmount')}")


if __name__ == "__main__":
    main()
