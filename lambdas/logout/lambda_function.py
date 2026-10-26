import time, jwt
from common import config, body, response, emit, correlation, auth_table, token_hash
def handler(event, context):
    cid=correlation(event)
    try:
        data=body(event); raw=data.get("refreshToken") if isinstance(data,dict) else None
        headers=event.get("headers") or {}; auth=headers.get("authorization") or headers.get("Authorization","")
        if not auth.startswith("Bearer "): return response(401,{"message":"Bearer access token required","correlationId":cid})
        c=config(); claims=jwt.decode(auth[7:],c["jwt"],algorithms=["HS256"],issuer="medislot",audience="medislot-api")
        if isinstance(raw,str) and raw:
            key="REFRESH#"+token_hash(raw)
            try:
                item=auth_table().get_item(Key={"token_key":key,"token_type":"REFRESH"}).get("Item")
                if item and item.get("user_id")==claims["sub"]:
                    auth_table().update_item(Key={"token_key":key,"token_type":"REFRESH"},UpdateExpression="SET #s=:r",ConditionExpression="attribute_exists(token_key)",ExpressionAttributeNames={"#s":"status"},ExpressionAttributeValues={":r":"REVOKED"})
            except Exception: pass
        auth_table().put_item(Item={"token_key":"JTI#"+claims["jti"],"token_type":"DENYLIST","user_id":claims["sub"],"created_at":int(time.time()),"expires_at":int(claims["exp"]),"status":"REVOKED"})
        return response(200,{"message":"Logged out"})
    except Exception:
        return response(401,{"message":"Invalid access token","correlationId":cid})
