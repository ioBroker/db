-- design: script
-- search: javascript
local rep = {}
local keys=redis.call("keys", KEYS[1].."*")
local argStart=KEYS[1]..KEYS[2]
local argEnd=KEYS[1]..KEYS[3]
local obj
--  function(doc) {
--      if (doc.type === "script" && doc.common.engineType.match(/^[jJ]ava[sS]cript|^[cC]offee[sS]cript|^[tT]ype[sS]cript|^Blockly/)) emit(doc.common.name, doc); }
for i,key in ipairs(keys) do
	if (key >= argStart and key < argEnd) then
	    obj = redis.call("get", key)
		if (cjson.decode(obj).type == "script") then
            rep[#rep+1] = obj
        end
	end
end
return rep