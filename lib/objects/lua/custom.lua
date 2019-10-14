-- design: custom
-- search: state
local rep = {}
local keys=redis.call("keys", KEYS[1].."*")
local argStart=KEYS[1]..KEYS[2]
local argEnd=KEYS[1]..KEYS[3]
local obj
local decoded
--  function(doc) {
--      if (doc.type==="state" && (doc.common.custom || doc.common.history))
--          emit(doc._id, doc.common.custom || doc.common.history)
--   }
for i,key in ipairs(keys) do
	if (key >= argStart and key < argEnd) then
	    obj = redis.call("get", key)
	    if (obj ~= nil and obj ~= "") then
            decoded = cjson.decode(obj)
            if (decoded.type == "state" and decoded.common ~= nil and decoded.common.custom ~= nil) then
                rep[#rep+1] = obj
            end
        end
	end
end
return rep