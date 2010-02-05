require'util'

-----------------------------------------------------
-- Event Handlers and Notifiers for the Loft Module
-----------------------------------------------------
-- this module temporarily includes the functionality of the mold module, from LuaFaces

module('events', package.seeall)

description = [[Generic Event Handlers and Notifiers for the Loft Module ]]

local index = {}

local all_contexts = {}

local function find_value(index, path)
	local value = index
	local k,step = next(path)
	while step and value do
		value = value[step] or value['*']
		k,step = next(path,k) 
	end
	return value
end

local function create_value(index, path)
	local value = index
	local k,step = next(path)
	while step and value do
		value[step] = value[step] or {}
		value = value[step] 
		k,step = next(path,k) 
	end
	return value
end

local function get_index(name)
	return {string.split(name, '.')}
end

--- adds a handler for an event in a given context
-- Event and context can be a dot-separated pattern of names and wildcards '*'
-- this means this event handler will be triggered when hit by notifiers that meet this pattern
function watch(event, context, handler)
	local handlers_by_context = find_value(all_contexts, get_index(context)) or create_value(all_contexts, get_index(context))
	local event_handlers = find_value(handlers_by_context, get_index(event)) or create_value(handlers_by_context, get_index(event))
	local ok
	if type(handler)=='function' then
		table.insert(event_handlers, handler)
		ok = true
	elseif type(handler)=='table' then
		for _,h in ipairs(handler) do
			table.insert(event_handlers, h)
		end
		ok = true
	end
	return ok
end

--- calls all handlers for an event in a given context, passing additional data
function notify(event, context, data)
	local handlers_by_context = find_value(all_contexts, get_index(context)) or {} 
	local event_handlers = find_value(handlers_by_context, get_index(event)) or {}
	local n
	for _,handler in ipairs(event_handlers) do
		if type(handler)=='function' then
			n = (n or 0) + (handler(data) and 1 or 0)
		end
	end
	return n
end

--- returns the number of handlers observing a given context. 
function watchers(event, context)
	local handlers_by_context = find_value(all_contexts, get_index(context)) or {} 
	local event_handlers = find_value(handlers_by_context, get_index(event)) or {}
	return #event_handlers
end

--- returns the number of handlers observing a given context.
function remove(event, context)
	local handlers_by_context = find_value(all_contexts, get_index(context)) or {} 
	local event_handlers = find_value(handlers_by_context, get_index(event)) or {}
	local n = #event_handlers
	local m = n
	while n>0 do
		table.remove(event_handlers, n)
		n = n - 1
	end
	return m
end
