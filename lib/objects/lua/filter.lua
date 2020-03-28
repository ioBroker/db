-- func: function(doc) { if (doc.type === "%1") emit(doc._id, doc); }
local rep = {}
-- local keys=redis.call("keys", KEYS[1].."*")
local cursor = KEYS[5];
local result=redis.call("SCAN", cursor, "MATCH", KEYS[1].."*", "COUNT", 500)
cursor = result[1]
local keys = result[2]
local argStart=KEYS[1]..KEYS[2]
local argEnd=KEYS[1]..KEYS[3]
local type=KEYS[4]
local obj
local decoded
-- function(doc) { if (doc.type === "chart") emit(doc._id, doc); }
for _,key in ipairs(keys) do
    if (key >= argStart and key < argEnd) then
        obj = redis.call("get", key)
        if (obj ~= nil and obj ~= "") then
            decoded = cjson.decode(obj)
            if (decoded and decoded.type == type) then
                rep[#rep+1] = obj
            end
        end
    end
end
return {rep, cursor}