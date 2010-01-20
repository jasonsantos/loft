require "util"

module("proxy",package.seeall)

--TODO:  memory management
local mt = {
	__index=function(t, k) 
		if not rawget(t,k) then 
			-- TODO: weak table
			t[k] = {} 
		end 
		return t[k] 
	end
}

local pool = setmetatable({}, mt)
local proxies = setmetatable({}, mt)
local refresh = setmetatable({}, mt)

local function update_id(proxy, newid)
	local id = getmetatable(proxy).__id
	local entity= getmetatable(proxy).__entity
	local obj= getmetatable(proxy).__entity
	local all_proxies = proxies[entity][id];
	for _,p in ipairs(all_proxies) do
		getmetatable(p).__id = newid
	end
	proxies[entity][newid] = proxies[entity][id]
	proxies[entity][id] = nil
	pool[entity][newid] = obj;
	pool[entity][id] = nil
end

function get_object(proxy)
	local id = getmetatable(proxy).__id
	local entity= getmetatable(proxy).__entity
	local o = pool[entity][id]
	if not o then error('invalid object from pool') end
	return o
end


local function get(proxy, key)
	local o = get_object(proxy)
	return o[key]
end

local function set(proxy, key, value)
	local o = get_object(proxy)
	o[key]=value
	if key=='id' then
		update_id(proxy, value)
	end
end

function create(entity, existing_id, obj)
	local id = existing_id or (obj and obj.id) or {'new'};
	
	if obj then
		pool[entity][id] = obj;
		refresh[entity][id] = os.time(); -- this object was refreshed now
	elseif not existing_id then
		return nil
	end
	
	proxy = setmetatable({}, {
		__entity=entity,
		__id=id,
		__obj=pool[entity][id] or obj,
		__index = get,
		__newindex = set
	}); 
	
	proxies[entity][id]= proxies[entity][id] or setmetatable({}, {__mode='v'})
	table.insert(proxies[entity][id], proxy)
	
	return proxy
end