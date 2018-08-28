local rep = {} 
local keys=redis.call("keys", KEYS[1].."*")
local argStart=KEYS[1]..KEYS[2]
local argEnd=KEYS[1]..KEYS[3]
local type=KEYS[4]
local obj
-- function(doc) { if (doc.type === "chart") emit(doc.common.name, doc); }
for i,key in ipairs(keys) do
	if (key >= argStart and key < argEnd) then
	    obj = redis.call("get", key)
		if (cjson.decode(obj).type == type) then
            rep[#rep+1] = obj
        end
	end
end
return rep