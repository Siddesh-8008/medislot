import sys, subprocess, uuid, os, json, time

template, stack_name, *parameter_pairs = sys.argv[1:]
region = os.environ.get("AWS_REGION", "ap-south-1")
parameters = []
for pair in parameter_pairs:
    key, value = pair.split("=", 1)
    if value == "":          # optional parameter left empty (e.g. no alarm e-mail): the template default applies
        continue
    parameters.append({"ParameterKey": key, "ParameterValue": value})
def aws(*args, check=True):
    return subprocess.run(["aws", *args, "--region", region], check=check, text=True, capture_output=True)
# CREATE or UPDATE? Only "does not exist" means CREATE; any other error (e.g. AccessDenied) stops the deploy instead
# of guessing. An empty stack left in REVIEW_IN_PROGRESS by an earlier failed CREATE is deleted first.
probe = aws("cloudformation", "describe-stacks", "--stack-name", stack_name, check=False)
if probe.returncode == 0:
    status = json.loads(probe.stdout)["Stacks"][0]["StackStatus"]
    if status in ("REVIEW_IN_PROGRESS", "ROLLBACK_COMPLETE"):
        print(f"{stack_name} is {status} (it was never created); deleting the empty stack first")
        aws("cloudformation", "delete-stack", "--stack-name", stack_name)
        aws("cloudformation", "wait", "stack-delete-complete", "--stack-name", stack_name)
        exists = False
    else:
        exists = True
elif "does not exist" in probe.stderr:
    exists = False
else:
    print(probe.stderr)
    raise SystemExit(f"Could not read stack {stack_name} (see the error above); refusing to guess CREATE vs UPDATE.")
kind = "UPDATE" if exists else "CREATE"
name = f"m2-{uuid.uuid4().hex[:12]}"
cmd = ["cloudformation", "create-change-set", "--stack-name", stack_name,
       "--template-body", f"file://{template}", "--change-set-name", name,
       "--change-set-type", kind, "--capabilities", "CAPABILITY_NAMED_IAM",
       # Milestone 7: cost allocation tags. CloudFormation copies stack tags onto every resource that supports tags.
       "--tags", "Key=Project,Value=MediSlot", f"Key=Environment,Value={os.environ.get('ENVIRONMENT', 'dev')}",
       "--parameters"] + [f"ParameterKey={p['ParameterKey']},ParameterValue={p['ParameterValue']}" for p in parameters]
result = aws(*cmd, check=False)
if result.returncode:
    print(result.stderr)
    raise SystemExit(result.returncode)
for _ in range(90):
    info = aws("cloudformation", "describe-change-set", "--stack-name", stack_name,
               "--change-set-name", name, "--output", "json", check=False)
    if info.returncode == 0:
        data = json.loads(info.stdout)
        if data["Status"] in ("CREATE_COMPLETE", "FAILED"):
            break
    time.sleep(5)
else:
    raise SystemExit("Timed out waiting for Change Set creation")
print(json.dumps({"stack": stack_name, "type": kind, "status": data["Status"],
                  "reason": data.get("StatusReason"), "changes": [
                      {"action": c["ResourceChange"]["Action"],
                       "logicalId": c["ResourceChange"]["LogicalResourceId"],
                       "type": c["ResourceChange"]["ResourceType"]}
                      for c in data.get("Changes", [])]}, indent=2))
if data["Status"] != "CREATE_COMPLETE":
    reason = data.get("StatusReason", "")
    if "didn't contain changes" in reason or "No updates are to be performed" in reason:
        print("No changes to deploy.")
        raise SystemExit(0)
    raise SystemExit("Change Set failed; see reason above.")
aws("cloudformation", "execute-change-set", "--stack-name", stack_name, "--change-set-name", name)
waiter = "stack-create-complete" if kind == "CREATE" else "stack-update-complete"
done = aws("cloudformation", "wait", waiter, "--stack-name", stack_name, check=False)
if done.returncode != 0:
    # Show WHY it failed (the first FAILED events, oldest first) instead of a bare "exit status 255".
    ev = aws("cloudformation", "describe-stack-events", "--stack-name", stack_name, "--output", "json", check=False)
    if ev.returncode == 0:
        failed = [e for e in json.loads(ev.stdout)["StackEvents"] if "FAILED" in e["ResourceStatus"]]
        for e in reversed(failed[:15]):
            print(f'{e["Timestamp"]}  {e["LogicalResourceId"]}  {e["ResourceStatus"]}  {e.get("ResourceStatusReason", "")}')
    raise SystemExit(f"Stack {stack_name} did not reach {waiter}; the failed resources are listed above.")
print(f"Stack {stack_name} deployed successfully.")
