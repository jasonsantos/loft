local util 		= require "util"
local list 		= require "list"
local proxy 	= require "proxy"
local extract 	= require "loft.extract"

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
-- @param options table of options to be used in this instance. 
--				  All values are optional, since they can be entered 
--				  in the default options table in the configure method.<br/>
--
-- 			database (string) name of the database for connecting
--			username (string) username for the database connection
--			password (string) password for the database connection
--			hostname (string) hostname for the database connection
--			port (number) port for the database connection
-- 
-- 			forced_deletions (boolean) always force deletions for dirty objects
--			page_size (number) number of items on a page to use with engine.find
--
-- @see configure
function engine(opts)
	
	local options = prepare(opts)
	local engine = {options=options}
	
	-- load provider
	local provider_name = options.provider or 'base'
	local provider = require('loft.providers.'..provider_name)
	
	-- provider initialization. 
	-- A provider will likely want to store connection data on the engine table. 
	-- This gives it the oportunity for preparing the engine.
	engine.provider = provider
	engine.provider_setup = provider.setup(engine)
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

	local provider_function = function(entity, function_name)
		local provider = entity.options and entity.options.provider or engine.provider
		if not provider then
			error("Persistence provider not loaded")
		end
		if not provider[function_name] then
			error("Invalid persistence provider: function '" .. tostring(function_name) .. "' not found")
		end
		return function(...) return provider[function_name](engine, ...) end
	end
	
	-- Loft Engine Public API Table
	local api = {}
	
	local api_function = function(entity, function_name, object_self)
		return function(o,...) 
			if object_self ~= o then
				error('Function must be called as method (ex.: '..tostring(entity.name)..':'.. tostring(function_name).. '())', 2) 
			end
			return api[function_name](entity, ...) 
		end
	end
	
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
		
		return proxy.create(entity, id, obj, {
			save = function(...)
				return api.save(entity, ...)
			end,
			destroy = function(...)
				return api.destroy(entity, ...)
			end
		})
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
		
		local retrieve = provider_function(entity, 'retrieve')
		
		local data = assert(retrieve(entity, id))
		
		return data and api.new(entity, data, id)
	end
	
	
	--- Creates an entity
	-- if the provider supports creating entities, this will create a 
	-- database table with the appropriate name and the corresponding
	-- fields
	-- @param entity 	schema entity of the object to be retrieved
	-- @return 			true if the table was created
	function api.create(entity)
		local create = provider_function(entity, 'create')
		return create(entity, id)
	end
	
	--- Finds a list of objects matching a given set of filters.
	-- for each given object matching the criteria, if it is already 
	-- in memory cache, it is obtained directly from there
	-- if not, it will be loaded from persistence and restored to memory cache
	--
	-- @param entity 	schema entity of the objects to be retrieved
	-- @param options	table containing the criteria for the retrieval of objects
	-- 
	--  entity			alternate place to put the entity param. It can alson be put in 
	--				    position [1] of the options table.
	-- 
	--  sorting			array containing a list of fields to be used in the sorting clauses 
	--
	--  pagination		table containing pagination options
	--					valid options are
	--					  'limit' or 'top' or 'page_size' = amount of items to be fetched
	--					  'offset' = position of the first item to be fetched
	--					  'page' = page to be fetched. page_size can be configured in the global options table
	-- 
	--  filter		 	table containing a set of filter conditions
	--					filters are tables with keys representing fieldnames
	--					and their correspontant values can be either strings 
	--					(when you want to filter by equalty to a specific value)
	-- 					arrays (when you want to indicate multiple possible values)
	--					or tables (when you want to indicate a distinct comparison operation).
	--					Ex.: { nome = "fulano", state = {1, 4, 6}, position={like= '%manager%'}, salary={lt=30000} } 
	--					
	--	visitor			function to be executed every time an item is found in persistence
	--
	-- @return 			list with all objects recovered
	-- @see api.count
	function api.find(entity, options)
		local options = options or entity
		local entity = options.entity or entity
		local sorting = options.sorting or {}
		local filters = options.filters or {}
		local pagination = options.pagination or {}
		
		local results = {}
		
		local visitor = options.visitor or function(data)
			local id = data.id
			local obj = api.new(entity, data, id)
			table.insert(results, obj)
		end
		
		local search = provider_function(entity, 'search')
		
		assert(search{entity=entity, filters=filters, sorting=sorting, pagination=pagination, visitor=visitor})
		
		if list and not options.noLists then
			results = list.create(results)
		end
		
		return results
	end

	--- Counts the amount of items of a given entity using filters	
	-- @param entity 	schema entity of the objects to be retrieved
	-- @param options	table containing the criteria for the counting of objects. Similar to the filters on 'find'
	-- @see api.find
	function api.count(entity, options)
		local options = options or entity
		local entity = options.entity or entity
		local filters = options.filters or {}
		
		local count = provider_function(entity, 'count')
		
		return assert(count{entity=entity, filters=filters})
	end	
	
	--- Saves the object to the persistence.
	-- if object has a complex type, saves to the appropriate repository
	-- if object has a simple type, saves according to the object ID
	-- @param obj object to be saved
	-- @param force boolean indicating whethe the object is to be saved even if it's not changed
	-- @return boolean indicating whether the object needed to be saved or not (i.e. if it was changed since its last)
	function api.save(entity, obj, force)
		if not proxy.is_dirty(obj) and not force then
			return false
		end
		
		local entity = entity or proxy.get_entity(obj)
		local data = proxy.get_object(obj)
		if not entity or not data then
			error('invalid object', 2)
		end
		
		local persist = provider_function(entity, 'persist')
		
		local id = proxy.get_id(obj)
				
		if not (id and tonumber(id)) then
			id = nil
		end
		
		assert(persist(entity, id, data))
		
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
	function api.destroy(entity ,obj, force)
		if proxy.is_dirty(obj) and (not engine.options.forced_deletions and not force) then
			--TODO: add an option for always forcing deletions
			return false, "Cannot delete a changed object (unless 'force' is selected)"
		end
		
		local entity = entity or proxy.get_entity(obj)
		local id = proxy.get_id(obj)
		if not entity or not id then
			error('invalid object', 2)
		end

		local delete = provider_function(entity, 'delete')
		
		if assert(delete(entity, id)) then
			return proxy.invalidate(obj)
		end
		
		return true
	end
	
	--- Initialize a schema to be used with Loft
	-- entities  and objects will receive Loft methods
	function api.decorate(schema, options)
		local model = {}
		
		for entity_name,inner_entity in pairs(schema.entities) do
			
			local new_entity = {}
			
			for function_name, _ in pairs(api) do
				new_entity[function_name] = api_function(inner_entity, function_name, new_entity)
			end
			
			model[entity_name] = new_entity
		end
		
		engine.schema = schema
		
		return model
	end
	
	function api.extract(options)
		return extract.introspect(engine, options)
	end

	function api.schema(options)
		local format = options.format or 'table'
		if format=='table' then 
			return engine.schema
		else
			return extract.render(engine, options)
		end
	end

	table.add(engine, api)

	return api
end