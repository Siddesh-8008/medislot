import time, secrets, jwt
from common import config, body, response, emit, correlation, auth_table, token_hash
def handler(event, context):
    cid=correlation(event)
    try:
        data=body(event); raw=data.get("refreshToken") if isinstance(data,dict) else None
        if not isinstance(raw,str) or not raw: return response(400,{"message":"refreshToken is required","correlationId":cid})
        c=config(); key="REFRESH#"+token_hash(raw); t=auth_table(); old=t.get_item(Key={"token_key":key,"token_type":"REFRESH"}).get("Item")
        now=int(time.time())
        if not old or old.get("status")!="ACTIVE" or int(old.get("expires_at",0))<=now:
            return response(401,{"message":"Invalid or revoked refresh token","correlationId":cid})
        # Conditional revoke prevents concurrent reuse of the same refresh token.
        t.update_item(Key={"token_key":key,"token_type":"REFRESH"},UpdateExpression="SET #s=:revoked",ConditionExpression="#s=:active",ExpressionAttributeNames={"#s":"status"},ExpressionAttributeValues={":revoked":"REVOKED",":active":"ACTIVE"})
        minutes=int(__import__("common").get_param(f"{c['prefix']}/auth/access-token-minutes"))
        jti=__import__("uuid").uuid4().hex
        access=jwt.encode({"sub":old["user_id"],"userId":old["user_id"],"role":old["role"],"iss":"medislot","aud":"medislot-api","iat":now,"exp":now+minutes*60,"jti":jti},c["jwt"],algorithm="HS256")
        newraw=secrets.token_urlsafe(48); days=int(__import__("common").get_param(f"{c['prefix']}/auth/refresh-token-days"))
        t.put_item(Item={"token_key":"REFRESH#"+token_hash(newraw),"token_type":"REFRESH","user_id":old["user_id"],"role":old["role"],"created_at":now,"expires_at":now+days*86400,"status":"ACTIVE"})
        return response(200,{"accessToken":access,"tokenType":"Bearer","expiresIn":minutes*60,"refreshToken":newraw})
    except Exception:
        emit("error","refresh_failed",cid)
        return response(401,{"message":"Invalid or revoked refresh token","correlationId":cid})
