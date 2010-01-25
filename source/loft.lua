require "util"
require "list"
require "proxy"

module("loft",package.seeall)

local object_pool = {}

-- Configuration API
-- ----------------- --

local defaults = {}

-- add default values to missing options
local function prepare(options)
	return table.merge(defaults, options)
end

--- register default values for loft.engine
function configure(options)
	local defs = table.add(defaults, options)
	return table.copy(defs)
end

local engine_api

--- creates a new loft engine 
-- the engine has its own options table
-- and its own plugins table. It is possible to add plugins
function engine(opts)
	
	local options = prepare(opts)
	local engine = {options=options}
	
	-- load provider
	local provider_name = options.provider or 'base'
	local provider = require('loft.providers.'..provider_name)
	
	-- provider initialization. 
	-- A provider will likely want to store connection data on the engine table. 
	-- This gives it the oportunity for preparing the engine.
	engine.provider = provider.setup(engine) or provider

	-- create publicly avaliable plugin proxies
	-- each with a brand new configuration table, just for this provider
	engine.plugins = {}
	for name, plugin in pairs(loft.plugins) do
		local plugin_config = {}
		
		engine.plugins[name] = setmetatable(plugin_config, {
			__index={
				run=function()
					return plugin.run(plugin_config, engine)
				end
			},
			__call=function(t, options)
				table.add(plugin_config, options or {})
				return plugin_config
			end
		})
		
		plugin.configure(plugin_config, engine)
	end
	
	return engine_api(engine)
end

-- Plugin API
-- ----------------- --

local plugin_api = {}

function plugin_api.add(plugin)
	if not (plugin.name and plugin.configure and plugin.run) then
		error("Invalid Plugin structure: plugins must have a name and a configure and run functions", 2)
	end
	if plugins[plugin.name] then
		error("Plugin "..plugin.name.." already registered", 2) 
	end
	plugins[plugin.name] = plugin
	return plugins
end


--- loft.plugins 
-- a way to find all registered plugins
-- plugins are registered in the table itself
-- but the table can also be used to access the plugin API
plugins = setmetatable({}, {__index=plugin_api})

-- Engine API
-- ----------------- --
engine_api=function(engine) 
	
	-- Loft Engine Public API Table
	local api = engine or {}
	
	
	--- Creates a new instance of an object
	-- alternatively, can turn a simple table into an object
	-- can also be used to recreate objects from their data tables
	-- @param entity	schema entity object or its name
	--					if no entity is designated, a simple table object will be created
	-- @param data 		(optional) table with data to be loaded into object
	-- @param id 		(optional) ID of the object to be restored
	-- @return new object of the designated type or a simple object
	function api.new(entity, data, id)
		local id = id or (data and data.id) or false
		
		-- TODO: usar estrutura do schema novo para inicializar objeto
		local obj = data or {}
	
		if type(entity)~='table' then
			error("Object must belong to a valid entity",2)
		end
		
		return proxy.create(entity, id, obj)
	end
	
	--- Recovers an object by its ID.
	-- if object is already in memory cache and its time_to_live is still valid, it is obtained directly from there
	-- if not, it will be loaded from persistence and restored to memory cache
	-- @param entity 	schema entity of the object to be retrieved
	-- @param id 		ID of the object to be retrieved
	-- @return 			object recovered
	function api.get(entity, id)
		local obj = proxy.create(entity, id)
		
		if obj then
			return obj
		end
		
		local provider = entity.options and entity.options.provider or engine.provider or {}
		if not provider.retrieve then
			error('Invalid persistence provider')
		end
		local data = provider.retrieve and provider.retrieve(entity, id)
		
		return data and api.new(entity, data, id)
	end
	
	--- Finds a list of objects matching a given set of filters.
	-- foreach given object matching the criteria, if it is already 
	-- in memory cache, it is obtained directly from there
	-- if not, it will be loaded from persistence and restored to memory cache
	--
	-- @param entity 	schema entity of the objects to be retrieved
	-- @param options	table containing the criteria for the retrieval of objects
	-- 
	--  entity			alternate place to put the entity param. It can alson be put in 
	--				    position [1] of the options table.
	-- 
	--  order			array containing a list of fields to be used in the sorting clauses 
	-- 
	--  filter		 	table containing a set of filter conditions
	--					filters are tables with keys representing fieldnames
	--					and their correspontant values can be either strings 
	--					(when you want to filter by equalty to a specific value)
	-- 					arrays (when you want to indicate multiple possible values)
	--					or tables (when you want to indicate a distinct comparison operation).
	--					Ex.: { nome = "fulano", state = {1, 4, 6}, {like= '%manager%'} } 
	--					
	--	visitor			function to be executed every time an item is found in persistence
	--
	-- @return 			list with all objects recovered
	
	function api.find(entity, options)
		local options = options or entity
		local entity = options.entity or entity
		local order = options.order or {}
		local filters = options.filters or {}
		
		local results = {}
		
		local visitor = options.visitor or function(n, data)
			local id = data.id
			local obj = api.new(entity, data, id)
			table.insert(results, obj)
		end
		
		local provider = entity.options and entity.options.provider or engine.provider or {}
		if not provider.search then
			error('Invalid persistence provider')
		end
		provider.search(entity, {entity=entity, filters=filters, order=order, visitor=visitor})
		
		if list and not options.noLists then
			results = list.create(results)
		end
		
		return results
	end
	
	
	--- Saves the object to the persistence.
	-- if object has a complex type, saves to the appropriate repository
	-- if object has a simple type, saves according to the object ID
	-- @param obj object to be saved
	-- @param force boolean indicating whethe the object is to be saved even if it's not changed
	-- @return boolean indicating whether the object needed to be saved or not (i.e. if it was changed since its last)
	function api.save(obj, force)
		if not proxy.is_dirty(obj) and not force then
			return false
		end
		
		local entity = proxy.get_entity(obj)
		local data = proxy.get_object(obj)
		if not entity or not data then
			error('invalid object', 2)
		end
		
		local provider = entity.options and entity.options.provider or engine.provider or {}
		if not provider.persist then
			error('Invalid persistence provider')
		end
		
		local id = proxy.get_id(obj)
		
		provider.persist(entity, id, data)
		
		if data.id and data.id ~= id then
			obj.id = data.id
		end
		
		proxy.reset(obj)
		
		return true
	end
	
	
	--- Destroys an object.
	-- remove it from memory and persistence. 
	--
	-- @param obj object to be destroyed
	-- @param force boolean indicating whethe the object is to be saved even if it has been changed
	-- @return true if object was successfully erased from persistence
	function api.destroy(obj, force)
		if proxy.is_dirty(obj) and not force then
			--TODO: add an option for always forcing deletions
			return false, "Cannot delete a changed object (unless 'force' is selected)"
		end
		
		local entity = proxy.get_entity(obj)
		local id = proxy.get_id(obj)
		if not entity or not id then
			error('invalid object', 2)
		end

		local provider = entity.options and entity.options.provider or engine.provider or {}
		if not provider.erase then
			error('Invalid persistence provider')
		end
		
		if provider.erase(entity, id) then
			return proxy.invalidate(obj)
		end
		
		return false
	end
	
	--- Initialize a schema to be used with Loft
	-- entities  and objects will receive Loft methods
	function api.decorate(schema, options)
		error'not implemented'
	end

	return api
end