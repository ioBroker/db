-- design: hm-rega
-- search: variables
local rep = {}
local keys=redis.call("keys", KEYS[1].."*")
local argStart=KEYS[1]..KEYS[2]
local argEnd=KEYS[1]..KEYS[3]
local obj
local decoded
--  function(doc) {
--      if (doc._id.match(/^hm-rega\\.[0-9]+\\.[0-9,A-Z,a-z]+/) && (doc.native.TypeName === "ALARMDP" || doc.native.TypeName === "VARDP")) {
--          emit(doc._id, doc);
--      }
--  }
for i,key in ipairs(keys) do
	if (key >= argStart and key < argEnd and key:sub(7, 13) == "hm-rega") then
	    obj = redis.call("get", key)
	    if (obj ~= nil and obj ~= "") then
            decoded = cjson.decode(obj)
            if (decoded.native ~= nil and (decoded.native.TypeName == "ALARMDP" or decoded.native.TypeName == "VARDP")) then
                rep[#rep+1] = obj
            end
        end
	end
end
return rep