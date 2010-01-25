require "util"

module("proxy",package.seeall)

--TODO:  memory management
local mt = {
	__index=function(t, k)
		if k then 
			if not rawget(t,k) then 
				-- TODO: weak table
				t[k] = {} 
			end 
			return t[k]
		end 
	end
}

local pool = setmetatable({}, mt)
local proxies = setmetatable({}, mt)
local refresh = setmetatable({}, mt)

local function update_id(proxy, newid)
	local id = getmetatable(proxy).__id
	local entity= getmetatable(proxy).__entity
	if not id or not entity then error('invalid proxy', 2) end
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

function get_entity(proxy)
	local entity= getmetatable(proxy).__entity
	if not entity then error('invalid proxy', 2) end
	return entity
end

function get_id(proxy)
	local id= getmetatable(proxy).__id
	if not id then error('invalid proxy', 2) end
	return id
end

function get_object(proxy)
	local id = getmetatable(proxy).__id
	local entity= getmetatable(proxy).__entity
	if not id or not entity then error('invalid proxy', 2) end
	local o = pool[entity][id]
	if not o then error('invalid object from pool') end
	return o
end

function invalidate(proxy)
	local id = getmetatable(proxy).__id
	local entity= getmetatable(proxy).__entity
	if not id or not entity then error('invalid proxy', 2) end
	local all_proxies = proxies[entity][id];
	
	for _,p in ipairs(all_proxies) do
		setmetatable(p, {'invalid proxy'})
	end
	
	proxies[entity][id] = {}
	
	pool[entity][id] = nil
end

function touch(proxy)
	local id = getmetatable(proxy).__id
	local entity= getmetatable(proxy).__entity
	if not id or not entity then error('invalid proxy', 2) end
	getmetatable(proxy).__dirty = true
end

function reset(proxy)
	local id = getmetatable(proxy).__id
	local entity= getmetatable(proxy).__entity
	if not id or not entity then error('invalid proxy', 2) end
	getmetatable(proxy).__dirty = false
end

function is_dirty(proxy)
	local id = getmetatable(proxy).__id
	local entity= getmetatable(proxy).__entity
	if not id or not entity then error('invalid proxy', 2) end
	return getmetatable(proxy).__dirty
end

--Generic Getters and Setters
local function get(proxy, key)
	local o = get_object(proxy)
	--TODO: add hooks
	return o[key]
end

local function set(proxy, key, value)
	local o = get_object(proxy)
	--TODO: add hooks
	if not o[key] or o[key]~=value then
		touch(proxy)
	end 
	
	if key=='id' then
		update_id(proxy, value)
	else
		o[key]=value
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
	
	local age = refresh[entity][id] and os.time() - refresh[entity][id]
	local max_age = entity.options and entity.options.max_age or 10
	
	if age and age>max_age then
		return nil
	end
	
	proxy = setmetatable({}, {
		__entity=entity,
		__id=id,
		__obj=pool[entity][id] or obj,
		__index = get,
		__newindex = set,
		__dirty=true
	}); 
	
	proxies[entity][id]= proxies[entity][id] or setmetatable({}, {__mode='v'})
	table.insert(proxies[entity][id], proxy)
	
	return proxy
end