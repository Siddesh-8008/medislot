# Step-by-step GitHub deployment

## A. Put the files on your existing MediSlot feature branch
1. Download and extract `medislot-milestone2.zip` on your computer.
2. Open your existing local MediSlot repository folder (the one containing `.git`). Do **not** delete `.git`.
3. Check out the branch `feature/milestone-2`.
4. Copy the extracted files and folders into the repository root. Allow the new `cloudformation/network-stack.yaml` and `.github/workflows/deploy.yaml` to replace the old files. Keep your existing `.git` folder untouched.
5. Run `git status` and inspect the changed files. Do not add AWS credentials, `.env` files, or generated ZIP packages.
6. Run locally if Python 3.12 is available: `pip install -r requirements-dev.txt`, then `pytest -q`.
7. Commit and push only to the feature branch:
   `git add .`
   `git commit -m "Implement MediSlot milestone 2 foundation"`
   `git push origin feature/milestone-2`

## B. Check repository configuration
1. Open GitHub → your MediSlot repository → **Settings → Secrets and variables → Actions**.
2. Under **Variables**, add `AWS_REGION` = `ap-south-1`, `ENVIRONMENT` = `dev`, and `AWS_DEPLOY_ROLE_ARN` = the existing GitHub OIDC role ARN.
3. If you already stored the role ARN as a repository secret named `AWS_DEPLOY_ROLE_ARN`, leave it there; this workflow accepts either the variable or the secret.
4. Do not create GitHub secrets for the RDS password or JWT signing secret. The workflow generates them and stores them in SSM Parameter Store as SecureString.
5. Confirm the AWS role's trust policy permits this repository and the `feature/milestone-2` branch. Confirm its permissions cover the CloudFormation resources and `iam:PassRole` needed by these templates.

## C. Run CI and deploy
1. In GitHub, open **Actions**.
2. Select **MediSlot Milestone 2 CI and Deployment**.
3. Open the run triggered by your push, or choose **Run workflow** and select `feature/milestone-2`.
4. First inspect the `ci` job. It must pass tests, Python compilation, CloudFormation linting, Gitleaks secret scanning, and Checkov IaC scanning. Checkov findings are surfaced but are currently non-blocking; review them rather than ignoring them.
5. Open the `deploy` job. It first assumes the AWS role through OIDC and prints the assumed identity.
6. The network stack Change Set creates the VPC, public/private subnets, IGW, route tables, security groups and S3/DynamoDB/SSM endpoints.
7. The workflow creates SSM SecureString parameters only if they do not already exist, then deploys the data stack: private RDS, six DynamoDB tables, two private S3 buckets and configuration parameters.
8. The workflow packages and uploads Lambda code to the artifact bucket, deploys the API/auth stack through a Change Set, then invokes the RDS connection-check Lambda.
9. If a step fails, open that step's full log. Then open AWS Console → CloudFormation → the failed stack → **Events** to see the resource-specific reason. Fix the template/permission issue on the feature branch and push again.

## D. Verify authentication
Use the API URL shown in the deployment log. Example with curl:

```bash
API='paste-the-ApiUrl-output-here'

curl -i -X POST "$API/auth/register/patient" \
  -H 'content-type: application/json' \
  -d '{"email":"patient1@example.com","password":"StrongPass!234","fullName":"Test Patient"}'

curl -i -X POST "$API/auth/register/doctor" \
  -H 'content-type: application/json' \
  -d '{"email":"doctor1@example.com","password":"StrongPass!234","fullName":"Test Doctor"}'

curl -i -X POST "$API/auth/login" \
  -H 'content-type: application/json' \
  -d '{"email":"patient1@example.com","password":"StrongPass!234"}'
```

Save the `accessToken` and `refreshToken` from the login response only in your local terminal/session. Do not paste them into GitHub, logs, screenshots or chat. Then test:
- Patient token on `GET $API/patient` → HTTP 200.
- Patient token on `GET $API/doctor` → HTTP 403.
- Change a character in the access token and call `GET $API/patient` → HTTP 401.
- Call `POST $API/auth/logout` with the valid access token and `{"refreshToken":"..."}` body → HTTP 200.
- Reuse the logged-out access token on `GET $API/patient` → HTTP 401.
- Call `POST $API/auth/refresh` with the refresh token before logout → returns a new access token and rotates the refresh token; replaying the old refresh token should fail.

## E. PR and required status check
1. Open **Pull requests → New pull request**.
2. Base: `master`; compare: `feature/milestone-2`.
3. Review the diff and create the PR.
4. Wait for the `ci` required status check, obtain one approving review, then merge.
5. Do not commit directly to `master`. Confirm branch protection still requires PR + approval + `ci` and blocks force-push and branch deletion.
